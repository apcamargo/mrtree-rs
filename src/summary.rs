use std::collections::HashSet;

use crate::model::{EffectiveTable, LabelMatrix, Path, PathLabel};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OutputSummary {
    pub(crate) clusters_per_level: Vec<usize>,
    pub(crate) reassignments_per_level: Vec<usize>,
    pub(crate) rows_reassigned: usize,
}

#[must_use]
pub(crate) fn clusters_per_level(labels: &LabelMatrix) -> Vec<usize> {
    (0..labels.n_cols())
        .map(|column| {
            let mut distinct = HashSet::with_capacity(labels.n_rows());
            distinct.extend(labels.column_iter(column));
            distinct.len()
        })
        .collect()
}

pub(crate) fn summarize_output(
    effective: &EffectiveTable,
    output_paths: &[Path],
) -> crate::Result<OutputSummary> {
    effective.validate_output_row_count(output_paths.len())?;

    let n_levels = effective.labels().n_cols();
    let mut clusters_by_level = (0..n_levels)
        .map(|_| HashSet::with_capacity(effective.labels().n_rows()))
        .collect::<Vec<_>>();
    let mut reassignments_per_level = vec![0; n_levels];
    let mut rows_reassigned = 0;

    for (row_idx, path) in output_paths.iter().enumerate() {
        effective.validate_output_path(row_idx, path)?;
        let mut reassigned = false;
        let original_row = effective.labels().row(row_idx);
        for (level, &path_label) in path.iter().enumerate() {
            clusters_by_level[level].insert(path_label);
            if path_label != PathLabel::Real(original_row[level]) {
                reassignments_per_level[level] += 1;
                reassigned = true;
            }
        }

        if reassigned {
            rows_reassigned += 1;
        }
    }

    Ok(OutputSummary {
        clusters_per_level: clusters_by_level
            .into_iter()
            .map(|labels| labels.len())
            .collect(),
        reassignments_per_level,
        rows_reassigned,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::MrtreeError;
    use crate::model::RealLabel;

    fn label(value: u64) -> RealLabel {
        RealLabel::new(value)
    }

    fn sample_effective_table() -> EffectiveTable {
        EffectiveTable::new(
            None,
            vec!["a".into(), "b".into(), "c".into()],
            None,
            LabelMatrix::new(
                3,
                2,
                vec![
                    label(1),
                    label(10), //
                    label(1),
                    label(20), //
                    label(2),
                    label(20),
                ],
            ),
            vec![0, 1],
            vec![2, 2],
        )
        .expect("effective table should be valid")
    }

    #[test]
    fn summarize_output_counts_clusters_and_reassignments() {
        let effective = sample_effective_table();

        let summary = summarize_output(
            &effective,
            &[
                vec![PathLabel::Real(label(1)), PathLabel::Real(label(10))],
                vec![PathLabel::Augmented, PathLabel::Real(label(20))],
                vec![PathLabel::Real(label(2)), PathLabel::Real(label(30))],
            ],
        )
        .expect("summary should succeed");

        assert_eq!(summary.clusters_per_level, vec![3, 3]);
        assert_eq!(summary.reassignments_per_level, vec![1, 1]);
        assert_eq!(summary.rows_reassigned, 2);
    }

    #[test]
    fn summarize_output_rejects_row_with_wrong_path_length() {
        let effective = sample_effective_table();
        let error = summarize_output(
            &effective,
            &[
                vec![PathLabel::Real(label(1))],
                vec![PathLabel::Real(label(1)), PathLabel::Real(label(20))],
                vec![PathLabel::Real(label(2)), PathLabel::Real(label(20))],
            ],
        )
        .expect_err("wrong path length should fail");

        assert!(matches!(
            error,
            MrtreeError::InternalAlgorithmInvariantViolation(message)
                if message == "output row 0 has 1 labels, expected 2"
        ));
    }
}
