use std::collections::{BTreeMap, BTreeSet};

use faer::Mat;
use linfa::DatasetBase;
use linfa::traits::{Fit, Predict};
use linfa_clustering::KMeans;
use ndarray::Array2;
use rand::SeedableRng;
use rand_xoshiro::Xoshiro256PlusPlus;

use crate::algorithm::weights;
use crate::error::MrtreeError;
use crate::model::{EffectiveTable, LabelMatrix, Path, RealLabel};

#[derive(Debug, Clone)]
pub(crate) struct ConsensusState {
    labels: LabelMatrix,
    group_mapping: Vec<usize>,
}

impl ConsensusState {
    pub(crate) fn labels(&self) -> &LabelMatrix {
        &self.labels
    }

    pub(crate) fn group_mapping(&self) -> &[usize] {
        &self.group_mapping
    }

    pub(crate) fn expand_paths(&self, reduced_paths: &[Path]) -> crate::Result<Vec<Path>> {
        let mut expanded_paths = Vec::with_capacity(reduced_paths.len());
        for (row, path) in reduced_paths.iter().enumerate() {
            let mut expanded = Vec::with_capacity(self.group_mapping.len());
            for &group_index in &self.group_mapping {
                let Some(label) = path.get(group_index).copied() else {
                    return Err(MrtreeError::InternalAlgorithmInvariantViolation(format!(
                        "reduced consensus path row {row} is missing group index {group_index}"
                    )));
                };
                expanded.push(label);
            }
            expanded_paths.push(expanded);
        }

        Ok(expanded_paths)
    }
}

pub(crate) fn reduce_same_k_groups(
    effective: &EffectiveTable,
    sample_weighted: bool,
    seed: u64,
) -> crate::Result<ConsensusState> {
    let mut reduced_columns = Vec::new();
    let mut group_mapping = vec![0; effective.ks().len()];
    let mut group_start = 0;

    while group_start < effective.ks().len() {
        let reduced_index = reduced_columns.len();
        let k = effective.ks()[group_start];
        let mut group_end = group_start + 1;
        while group_end < effective.ks().len() && effective.ks()[group_end] == k {
            group_end += 1;
        }

        for group in group_mapping.iter_mut().take(group_end).skip(group_start) {
            *group = reduced_index;
        }

        if group_end - group_start == 1 {
            reduced_columns.push(
                effective
                    .labels()
                    .column_iter(group_start)
                    .collect::<Vec<_>>(),
            );
            group_start = group_end;
            continue;
        }

        let group_columns = (group_start..group_end).collect::<Vec<_>>();
        let group_labels = effective.labels().reordered_columns(&group_columns);
        reduced_columns.push(consensus_column(
            &group_labels,
            k,
            sample_weighted,
            seed.wrapping_add(reduced_index as u64),
        )?);
        group_start = group_end;
    }

    if reduced_columns.len() < 2 {
        return Err(MrtreeError::ConsensusRequiresAtLeastTwoLayers);
    }

    Ok(ConsensusState {
        labels: columns_to_matrix(&reduced_columns),
        group_mapping,
    })
}

fn consensus_column(
    labels: &LabelMatrix,
    k: usize,
    sample_weighted: bool,
    seed: u64,
) -> crate::Result<Vec<RealLabel>> {
    if k == 1 {
        return Ok(vec![RealLabel::new(1); labels.n_rows()]);
    }

    let mut encoded = build_membership_matrix(labels, k);
    if sample_weighted {
        let sample_weights = weights::compute_sample_weights(labels);
        for (row_idx, weight) in sample_weights.into_iter().enumerate() {
            encoded
                .row_mut(row_idx)
                .mapv_inplace(|value| value * weight);
        }
    }

    let embedding = svd_embedding(&encoded, k)?;
    let dataset = DatasetBase::from(embedding.clone());
    let rng = Xoshiro256PlusPlus::seed_from_u64(seed);
    let model = KMeans::params_with_rng(k, rng)
        .max_n_iterations(30)
        .n_runs(5)
        .tolerance(1e-5)
        .fit(&dataset)
        .map_err(|error| MrtreeError::ConsensusKMeans(error.to_string()))?;
    let predictions = model.predict(&embedding);

    Ok(canonicalize_cluster_ids(
        predictions.iter().copied().collect::<Vec<_>>(),
    ))
}

fn build_membership_matrix(labels: &LabelMatrix, k: usize) -> Array2<f64> {
    let mut matrix = Array2::<f64>::zeros((labels.n_rows(), labels.n_cols() * k));

    for column in 0..labels.n_cols() {
        let distinct = labels.column_iter(column).collect::<BTreeSet<_>>();
        let index_by_label = distinct
            .into_iter()
            .enumerate()
            .map(|(offset, label)| (label, offset))
            .collect::<BTreeMap<_, _>>();

        for row in 0..labels.n_rows() {
            let label = labels.row(row)[column];
            let offset = index_by_label[&label];
            matrix[(row, column * k + offset)] = 1.0;
        }
    }

    matrix
}

fn svd_embedding(encoded: &Array2<f64>, rank: usize) -> crate::Result<Array2<f64>> {
    let faer_matrix = Mat::<f64>::from_fn(encoded.nrows(), encoded.ncols(), |row, column| {
        encoded[(row, column)]
    });
    let svd = faer_matrix
        .as_ref()
        .thin_svd()
        .map_err(|error| MrtreeError::ConsensusSvd(format!("{error:?}")))?;
    let singular_values = svd.S().column_vector();
    let u = svd.U();

    let mut embedding = Array2::<f64>::zeros((encoded.nrows(), rank));
    for (column, (u_col, singular_value)) in u
        .col_iter()
        .zip(singular_values.iter())
        .take(rank)
        .enumerate()
    {
        for (row, value) in u_col.iter().enumerate() {
            embedding[(row, column)] = *value * *singular_value;
        }
    }
    Ok(embedding)
}

fn canonicalize_cluster_ids(raw_assignments: Vec<usize>) -> Vec<RealLabel> {
    let mut seen = BTreeMap::new();
    let mut next_label = 1_u64;

    raw_assignments
        .into_iter()
        .map(|cluster| {
            *seen.entry(cluster).or_insert_with(|| {
                let label = RealLabel::new(next_label);
                next_label += 1;
                label
            })
        })
        .collect()
}

fn columns_to_matrix(columns: &[Vec<RealLabel>]) -> LabelMatrix {
    let n_rows = columns.first().map_or(0, Vec::len);
    let n_cols = columns.len();
    let mut data = Vec::with_capacity(n_rows * n_cols);

    for row in 0..n_rows {
        for column in columns {
            data.push(column[row]);
        }
    }

    LabelMatrix::new(n_rows, n_cols, data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{EffectiveTable, LabelMatrix, PathLabel};

    fn label(value: u64) -> RealLabel {
        RealLabel::new(value)
    }

    #[test]
    fn reduces_and_expands_same_k_groups() {
        let effective = EffectiveTable::new(
            None,
            vec!["a".into(), "b".into(), "c".into(), "d".into()],
            None,
            LabelMatrix::new(
                4,
                4,
                vec![
                    label(1),
                    label(1),
                    label(1),
                    label(1), //
                    label(1),
                    label(1),
                    label(1),
                    label(1), //
                    label(2),
                    label(2),
                    label(2),
                    label(2), //
                    label(2),
                    label(2),
                    label(2),
                    label(2), //
                ],
            ),
            vec![0, 1, 2, 3],
            vec![2, 2, 2, 2],
        )
        .expect("valid effective table");

        let error = reduce_same_k_groups(&effective, false, 7)
            .expect_err("single reduced layer should be rejected");
        assert!(matches!(
            error,
            MrtreeError::ConsensusRequiresAtLeastTwoLayers
        ));
    }

    #[test]
    fn consensus_is_seed_deterministic() {
        let effective = EffectiveTable::new(
            None,
            vec!["a".into(), "b".into(), "c".into(), "d".into(), "e".into()],
            None,
            LabelMatrix::new(
                5,
                3,
                vec![
                    label(1),
                    label(1),
                    label(1), //
                    label(1),
                    label(1),
                    label(1), //
                    label(2),
                    label(2),
                    label(2), //
                    label(2),
                    label(2),
                    label(2), //
                    label(3),
                    label(3),
                    label(3), //
                ],
            ),
            vec![0, 1, 2],
            vec![3, 3, 4],
        )
        .expect("valid effective table");

        let first = reduce_same_k_groups(&effective, false, 11).unwrap();
        let second = reduce_same_k_groups(&effective, false, 11).unwrap();
        assert_eq!(first.labels(), second.labels());
    }

    #[test]
    fn consensus_reduction_and_expansion_restore_effective_width() {
        let effective = EffectiveTable::new(
            Some("sample_id".into()),
            vec!["a".into(), "b".into(), "c".into(), "d".into()],
            Some(vec![
                "k2_a".into(),
                "k2_b".into(),
                "k3_a".into(),
                "k3_b".into(),
            ]),
            LabelMatrix::new(
                4,
                4,
                vec![
                    label(1),
                    label(1),
                    label(1),
                    label(1), //
                    label(1),
                    label(1),
                    label(1),
                    label(2), //
                    label(2),
                    label(2),
                    label(2),
                    label(2), //
                    label(2),
                    label(2),
                    label(3),
                    label(3), //
                ],
            ),
            vec![0, 1, 2, 3],
            vec![2, 2, 3, 3],
        )
        .expect("valid effective table");

        let state =
            reduce_same_k_groups(&effective, false, 5).expect("consensus reduction should succeed");
        assert_eq!(state.labels().n_cols(), 2);

        let reduced_paths = (0..state.labels().n_rows())
            .map(|row| {
                state
                    .labels()
                    .row(row)
                    .iter()
                    .copied()
                    .map(PathLabel::Real)
                    .collect::<Path>()
            })
            .collect::<Vec<_>>();
        let expanded = state
            .expand_paths(&reduced_paths)
            .expect("expanded output should be materialized");

        assert_eq!(expanded.len(), effective.labels().n_rows());
        assert_eq!(expanded[0].len(), effective.labels().n_cols());
    }

    #[test]
    fn expand_paths_preserves_augmented_labels() {
        let state = ConsensusState {
            labels: LabelMatrix::new(1, 2, vec![label(1), label(1)]),
            group_mapping: vec![0, 0, 1],
        };

        let expanded = state
            .expand_paths(&[vec![PathLabel::Real(label(3)), PathLabel::Augmented]])
            .expect("expanded output should preserve augmented labels");

        assert_eq!(
            expanded,
            vec![vec![
                PathLabel::Real(label(3)),
                PathLabel::Real(label(3)),
                PathLabel::Augmented,
            ]]
        );
    }
}
