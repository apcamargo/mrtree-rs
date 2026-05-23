use crate::error::MrtreeError;
use crate::model::{LabelMatrix, Path};
use crate::weights::{SampleWeights, validate_level_weights};
use float_cmp::approx_eq;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReconcileOptions {
    pub augment_path: bool,
    pub threads: usize,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum LevelWeightMode<'a> {
    Unweighted,
    Weighted(&'a [f64]),
}

/// # Errors
///
/// Returns an error if sample weights do not match the input row count, if
/// level weights do not match the current input column count, if any level
/// weight is non-finite or not greater than 0, or if reconciliation fails
/// internally.
pub fn reconcile_labels(
    labels: &LabelMatrix,
    weights: Option<&SampleWeights>,
    level_weights: Option<&[f64]>,
    options: &ReconcileOptions,
) -> crate::Result<Vec<Path>> {
    let uniform_weights;
    let weight_slice = if let Some(weights) = weights {
        if weights.len() != labels.n_rows() {
            return Err(MrtreeError::SampleWeightsLengthMismatch {
                expected: labels.n_rows(),
                actual: weights.len(),
            });
        }
        weights.as_slice()
    } else {
        uniform_weights = vec![1.0; labels.n_rows()];
        &uniform_weights
    };

    let level_weight_mode = if let Some(level_weights) = level_weights {
        validate_level_weights(labels.n_cols(), level_weights)?;
        if has_effective_level_weighting(level_weights) {
            LevelWeightMode::Weighted(level_weights)
        } else {
            LevelWeightMode::Unweighted
        }
    } else {
        LevelWeightMode::Unweighted
    };

    crate::algorithm::reconcile::run(labels, weight_slice, level_weight_mode, *options)
}

fn has_effective_level_weighting(level_weights: &[f64]) -> bool {
    let Some((&first, rest)) = level_weights.split_first() else {
        return false;
    };

    rest.iter().any(|&weight| !approx_eq!(f64, weight, first))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{LabelMatrix, PathLabel, RealLabel};

    fn label(value: u64) -> RealLabel {
        RealLabel::new(value)
    }

    fn labels(rows: &[&[u64]]) -> LabelMatrix {
        let n_rows = rows.len();
        let n_cols = rows.first().map_or(0, |row| row.len());
        let mut data = Vec::with_capacity(n_rows * n_cols);
        for row in rows {
            data.extend(row.iter().copied().map(label));
        }
        LabelMatrix::new(n_rows, n_cols, data)
    }

    fn options() -> ReconcileOptions {
        ReconcileOptions {
            augment_path: false,
            threads: 1,
        }
    }

    #[test]
    fn reconcile_rejects_mismatched_level_weight_lengths() {
        let labels = labels(&[&[1, 1], &[2, 2]]);
        let error = reconcile_labels(&labels, None, Some(&[1.0]), &options())
            .expect_err("mismatched level weights should fail");

        assert!(matches!(
            error,
            MrtreeError::LevelWeightsLengthMismatch {
                expected: 2,
                actual: 1
            }
        ));
    }

    #[test]
    fn reconcile_treats_uniform_non_one_level_weights_like_unweighted() {
        let labels = labels(&[&[1, 1, 1], &[1, 1, 2], &[2, 1, 3], &[2, 2, 4]]);

        let unweighted = reconcile_labels(&labels, None, None, &options())
            .expect("unweighted reconciliation should succeed");
        let weighted = reconcile_labels(&labels, None, Some(&[2.0, 2.0, 2.0]), &options())
            .expect("uniform level-weighted reconciliation should succeed");

        assert_eq!(unweighted, weighted);
        assert!(
            weighted
                .iter()
                .all(|path| { path.iter().all(|label| matches!(label, PathLabel::Real(_))) })
        );
    }

    #[test]
    fn reconcile_rejects_non_positive_and_non_finite_level_weights() {
        let labels = labels(&[&[1, 1, 1], &[1, 1, 2], &[2, 2, 3]]);

        for invalid in [0.0, -1.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let weights = [1.0, invalid, 1.0];
            let error = reconcile_labels(&labels, None, Some(&weights), &options())
                .expect_err("invalid level weights should fail");

            assert!(matches!(
                error,
                MrtreeError::InvalidLevelWeight { index: 1, .. }
            ));
        }
    }
}
