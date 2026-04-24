use crate::error::MrtreeError;
use crate::model::{LabelMatrix, Path};
use crate::weights::SampleWeights;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReconcileOptions {
    pub augment_path: bool,
    pub threads: usize,
}

pub fn reconcile_labels(
    labels: &LabelMatrix,
    weights: Option<&SampleWeights>,
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

    crate::algorithm::reconcile::run(labels, weight_slice, *options)
}
