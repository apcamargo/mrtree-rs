use std::collections::HashMap;

use tracing::{Level, enabled, trace};

use crate::model::LabelMatrix;

#[derive(Debug, Clone, PartialEq)]
struct WeightTraceSummary {
    distinct_per_level: Vec<usize>,
    cluster_size_ranges: Vec<(usize, usize)>,
    weight_min: f64,
    weight_max: f64,
    weight_mean: f64,
}

pub(crate) fn compute_sample_weights(labels: &LabelMatrix) -> Vec<f64> {
    let mut layer_sizes = Vec::with_capacity(labels.n_cols());
    for column in 0..labels.n_cols() {
        let mut sizes = HashMap::with_capacity(labels.n_rows());
        for value in labels.column_iter(column) {
            *sizes.entry(value).or_insert(0_usize) += 1;
        }
        layer_sizes.push(sizes);
    }

    let weights = (0..labels.n_rows())
        .map(|row| {
            let total = labels
                .row(row)
                .iter()
                .enumerate()
                .map(|(column, value)| layer_sizes[column][value])
                .sum::<usize>();
            1.0 / (total as f64).sqrt()
        })
        .collect::<Vec<_>>();
    if enabled!(Level::TRACE) {
        let summary = summarize_weight_trace(&layer_sizes, &weights);
        trace!(
            rows = labels.n_rows(),
            levels = labels.n_cols(),
            distinct_per_level = ?summary.distinct_per_level,
            cluster_size_ranges = ?summary.cluster_size_ranges,
            weight_min = summary.weight_min,
            weight_max = summary.weight_max,
            weight_mean = summary.weight_mean,
            "Sample weighting summary"
        );
    }
    weights
}

fn summarize_weight_trace(
    layer_sizes: &[HashMap<crate::model::RealLabel, usize>],
    weights: &[f64],
) -> WeightTraceSummary {
    let distinct_per_level = layer_sizes
        .iter()
        .map(std::collections::HashMap::len)
        .collect::<Vec<_>>();
    let cluster_size_ranges = layer_sizes
        .iter()
        .map(|sizes| {
            let min = sizes.values().copied().min().unwrap_or(0);
            let max = sizes.values().copied().max().unwrap_or(0);
            (min, max)
        })
        .collect::<Vec<_>>();
    let weight_min = weights.iter().copied().reduce(f64::min).unwrap_or(0.0);
    let weight_max = weights.iter().copied().reduce(f64::max).unwrap_or(0.0);
    let weight_mean = if weights.is_empty() {
        0.0
    } else {
        weights.iter().sum::<f64>() / weights.len() as f64
    };

    WeightTraceSummary {
        distinct_per_level,
        cluster_size_ranges,
        weight_min,
        weight_max,
        weight_mean,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::RealLabel;
    use float_cmp::assert_approx_eq;

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

        assert_approx_eq!(f64, weights[0], 1.0 / 5.0_f64.sqrt());
        assert_approx_eq!(f64, weights[2], 0.5);
        assert_approx_eq!(f64, weights[3], 1.0 / 2.0_f64.sqrt());
        assert!(
            weights[3] > weights[0],
            "rare-cluster rows should receive more weight"
        );
    }

    #[test]
    fn summarize_weight_trace_reports_deterministic_ranges_and_moments() {
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
        let mut layer_sizes = Vec::new();
        for column in 0..labels.n_cols() {
            let mut sizes = HashMap::new();
            for value in labels.column_iter(column) {
                *sizes.entry(value).or_insert(0_usize) += 1;
            }
            layer_sizes.push(sizes);
        }

        let summary = summarize_weight_trace(&layer_sizes, &compute_sample_weights(&labels));

        assert_eq!(summary.distinct_per_level, vec![2, 3]);
        assert_eq!(summary.cluster_size_ranges, vec![(1, 3), (1, 2)]);
        assert_approx_eq!(f64, summary.weight_min, 1.0 / 5.0_f64.sqrt());
        assert_approx_eq!(f64, summary.weight_max, 1.0 / 2.0_f64.sqrt());
        assert_approx_eq!(
            f64,
            summary.weight_mean,
            ((2.0 / 5.0_f64.sqrt()) + 0.5 + (1.0 / 2.0_f64.sqrt())) / 4.0
        );
    }
}
