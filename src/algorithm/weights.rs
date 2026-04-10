use std::collections::HashMap;

use crate::model::LabelMatrix;

pub(crate) fn compute_sample_weights(labels: &LabelMatrix) -> Vec<f64> {
    let mut layer_sizes = Vec::with_capacity(labels.n_cols());
    for column in 0..labels.n_cols() {
        let mut sizes = HashMap::with_capacity(labels.n_rows());
        for value in labels.column_iter(column) {
            *sizes.entry(value).or_insert(0_usize) += 1;
        }
        layer_sizes.push(sizes);
    }

    (0..labels.n_rows())
        .map(|row| {
            let total = labels
                .row(row)
                .iter()
                .enumerate()
                .map(|(column, value)| layer_sizes[column][value])
                .sum::<usize>();
            1.0 / (total as f64).sqrt()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::RealLabel;

    #[test]
    fn computes_corrected_inverse_cluster_size_weights() {
        let labels = LabelMatrix::new(
            4,
            2,
            vec![
                RealLabel::new(1),
                RealLabel::new(10), //
                RealLabel::new(1),
                RealLabel::new(10), //
                RealLabel::new(1),
                RealLabel::new(11), //
                RealLabel::new(2),
                RealLabel::new(20), //
            ],
        );

        let weights = compute_sample_weights(&labels);

        assert!((weights[0] - 1.0 / 5.0_f64.sqrt()).abs() < 1e-12);
        assert!((weights[2] - 0.5).abs() < 1e-12);
        assert!((weights[3] - 1.0 / 2.0_f64.sqrt()).abs() < 1e-12);
        assert!(
            weights[3] > weights[0],
            "rare-cluster rows should receive more weight"
        );
    }
}
