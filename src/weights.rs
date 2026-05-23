use crate::error::MrtreeError;
use crate::model::LabelMatrix;

#[derive(Debug, Clone, PartialEq)]
pub struct SampleWeights {
    values: Vec<f64>,
}

impl SampleWeights {
    /// # Errors
    ///
    /// Returns an error if `values.len()` does not match `expected_rows`.
    pub fn new(expected_rows: usize, values: Vec<f64>) -> crate::Result<Self> {
        if values.len() != expected_rows {
            return Err(MrtreeError::SampleWeightsLengthMismatch {
                expected: expected_rows,
                actual: values.len(),
            });
        }

        Ok(Self { values })
    }

    #[must_use]
    pub fn as_slice(&self) -> &[f64] {
        &self.values
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

pub(crate) fn validate_level_weights(expected_levels: usize, values: &[f64]) -> crate::Result<()> {
    if values.len() != expected_levels {
        return Err(MrtreeError::LevelWeightsLengthMismatch {
            expected: expected_levels,
            actual: values.len(),
        });
    }

    for (index, &weight) in values.iter().enumerate() {
        if !weight.is_finite() || weight <= 0.0 {
            return Err(MrtreeError::InvalidLevelWeight { index, weight });
        }
    }

    Ok(())
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LevelWeights {
    values: Vec<f64>,
}

impl LevelWeights {
    pub(crate) fn new(expected_levels: usize, values: Vec<f64>) -> crate::Result<Self> {
        validate_level_weights(expected_levels, &values)?;
        Ok(Self { values })
    }

    #[must_use]
    pub(crate) fn as_slice(&self) -> &[f64] {
        &self.values
    }

    #[must_use]
    pub(crate) fn map_to_effective(&self, original_column_indices: &[usize]) -> Self {
        Self {
            values: original_column_indices
                .iter()
                .map(|&index| self.values[index])
                .collect(),
        }
    }

    #[must_use]
    pub(crate) fn normalize_by_max(&self) -> Self {
        let max_weight = self
            .values
            .iter()
            .copied()
            .fold(f64::NEG_INFINITY, f64::max);
        Self {
            values: self
                .values
                .iter()
                .map(|weight| weight / max_weight)
                .collect(),
        }
    }

    #[must_use]
    pub(crate) fn reduce_by_group_mapping(&self, group_mapping: &[usize]) -> Self {
        let reduced_len = group_mapping
            .iter()
            .copied()
            .max()
            .map_or(0, |max_group| max_group + 1);
        let mut reduced = vec![0.0; reduced_len];

        for (&weight, &group_index) in self.values.iter().zip(group_mapping.iter()) {
            reduced[group_index] += weight;
        }

        Self { values: reduced }
    }
}

#[must_use]
pub fn compute_sample_weights(labels: &LabelMatrix) -> SampleWeights {
    SampleWeights {
        values: crate::algorithm::weights::compute_sample_weights(labels),
    }
}

#[cfg(test)]
mod tests {
    use super::LevelWeights;
    use crate::error::MrtreeError;

    #[test]
    fn level_weights_reject_length_mismatches() {
        let error = LevelWeights::new(3, vec![1.0, 2.0]).expect_err("length mismatch should fail");

        assert!(matches!(
            error,
            MrtreeError::LevelWeightsLengthMismatch {
                expected: 3,
                actual: 2
            }
        ));
    }

    #[test]
    fn level_weights_reject_non_positive_and_non_finite_values() {
        for invalid in [0.0, -1.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let error =
                LevelWeights::new(1, vec![invalid]).expect_err("invalid weight should fail");
            assert!(matches!(
                error,
                MrtreeError::InvalidLevelWeight { index: 0, .. }
            ));
        }
    }

    #[test]
    fn level_weights_normalize_by_max_preserves_ratios() {
        let normalized = LevelWeights::new(3, vec![2.0, 4.0, 8.0])
            .expect("weights should be valid")
            .normalize_by_max();

        assert_eq!(normalized.as_slice(), &[0.25, 0.5, 1.0]);
    }

    #[test]
    fn level_weights_reduce_by_group_mapping_sums_source_weights() {
        let reduced = LevelWeights::new(4, vec![1.0, 2.0, 3.0, 4.0])
            .expect("weights should be valid")
            .reduce_by_group_mapping(&[0, 0, 1, 1]);

        assert_eq!(reduced.as_slice(), &[3.0, 7.0]);
    }

    #[test]
    fn level_weights_map_to_effective_reorders_columns() {
        let effective = LevelWeights::new(4, vec![1.0, 2.0, 3.0, 4.0])
            .expect("weights should be valid")
            .map_to_effective(&[2, 0, 3]);

        assert_eq!(effective.as_slice(), &[3.0, 1.0, 4.0]);
    }
}
