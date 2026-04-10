use crate::error::MrtreeError;
use crate::model::LabelMatrix;

#[derive(Debug, Clone, PartialEq)]
pub struct SampleWeights {
    values: Vec<f64>,
}

impl SampleWeights {
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

#[must_use]
pub fn compute_sample_weights(labels: &LabelMatrix) -> SampleWeights {
    SampleWeights {
        values: crate::algorithm::weights::compute_sample_weights(labels),
    }
}
