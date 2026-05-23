use crate::model::{EffectiveTable, LabelMatrix, Path};
use crate::weights::validate_level_weights;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ConsensusOptions {
    pub sample_weighting: bool,
    pub seed: u64,
}

#[derive(Debug, Clone)]
pub struct ConsensusReduction {
    inner: crate::algorithm::consensus::ConsensusState,
}

impl ConsensusReduction {
    #[must_use]
    pub fn reduced_labels(&self) -> &LabelMatrix {
        self.inner.labels()
    }

    #[must_use]
    pub fn group_mapping(&self) -> &[usize] {
        self.inner.group_mapping()
    }

    /// # Errors
    ///
    /// Returns an error if any reduced path cannot be expanded back to the
    /// effective clustering width.
    pub fn expand_paths(&self, reduced_paths: &[Path]) -> crate::Result<Vec<Path>> {
        self.inner.expand_paths(reduced_paths)
    }
}

/// # Errors
///
/// Returns an error if same-K reduction would leave fewer than two effective
/// levels, if level weights do not match the current effective column count,
/// if any level weight is non-finite or not greater than 0, or if consensus
/// clustering fails internally.
pub fn reduce_same_k_groups(
    effective: &EffectiveTable,
    level_weights: Option<&[f64]>,
    options: &ConsensusOptions,
) -> crate::Result<ConsensusReduction> {
    if let Some(level_weights) = level_weights {
        validate_level_weights(effective.labels().n_cols(), level_weights)?;
    }

    Ok(ConsensusReduction {
        inner: crate::algorithm::consensus::reduce_same_k_groups(
            effective,
            options.sample_weighting,
            level_weights,
            options.seed,
        )?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::MrtreeError;
    use crate::model::{EffectiveTable, RealLabel};

    fn label(value: u64) -> RealLabel {
        RealLabel::new(value)
    }

    fn effective() -> EffectiveTable {
        EffectiveTable::new(
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
                    label(1),
                    label(1),
                    label(1),
                    label(1),
                    label(2),
                    label(2),
                    label(2),
                    label(2),
                    label(2),
                    label(2),
                    label(2),
                    label(3),
                    label(3),
                ],
            ),
            vec![0, 1, 2, 3],
            vec![2, 2, 3, 3],
        )
        .expect("valid effective table")
    }

    fn options() -> ConsensusOptions {
        ConsensusOptions {
            sample_weighting: false,
            seed: 0,
        }
    }

    #[test]
    fn consensus_rejects_mismatched_level_weight_lengths() {
        let effective = effective();

        for weights in [&[1.0, 1.0, 1.0][..], &[1.0, 1.0, 1.0, 1.0, 1.0][..]] {
            let error = reduce_same_k_groups(&effective, Some(weights), &options())
                .expect_err("mismatched level weights should fail");

            assert!(matches!(
                error,
                MrtreeError::LevelWeightsLengthMismatch {
                    expected: 4,
                    actual: 3 | 5
                }
            ));
        }
    }

    #[test]
    fn consensus_rejects_non_positive_and_non_finite_level_weights() {
        let effective = effective();

        for invalid in [0.0, -1.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let weights = [1.0, invalid, 1.0, 1.0];
            let error = reduce_same_k_groups(&effective, Some(&weights), &options())
                .expect_err("invalid level weights should fail");

            assert!(matches!(
                error,
                MrtreeError::InvalidLevelWeight { index: 1, .. }
            ));
        }
    }
}
