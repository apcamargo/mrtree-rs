use std::collections::HashSet;
use std::fmt;

use crate::error::MrtreeError;
use crate::model::{EffectiveTable, InputTable, LabelMatrix};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PrepareOptions {
    pub max_k: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReorderWarning {
    original_order: Vec<String>,
    canonical_order: Vec<String>,
}

impl ReorderWarning {
    #[must_use]
    pub fn original_order(&self) -> &[String] {
        &self.original_order
    }

    #[must_use]
    pub fn canonical_order(&self) -> &[String] {
        &self.canonical_order
    }
}

impl fmt::Display for ReorderWarning {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Effective clustering column order changed, original effective order [{}], canonical order [{}]",
            self.original_order.join(", "),
            self.canonical_order.join(", ")
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedInput {
    effective: EffectiveTable,
    reorder_warning: Option<ReorderWarning>,
}

impl PreparedInput {
    #[must_use]
    pub fn effective(&self) -> &EffectiveTable {
        &self.effective
    }

    #[must_use]
    pub fn reorder_warning(&self) -> Option<&ReorderWarning> {
        self.reorder_warning.as_ref()
    }

    pub(crate) fn into_effective_and_warning(self) -> (EffectiveTable, Option<ReorderWarning>) {
        (self.effective, self.reorder_warning)
    }
}

pub fn prepare(input: InputTable, options: &PrepareOptions) -> crate::Result<PreparedInput> {
    let (sample_header, sample_ids, cluster_headers, labels) = input.into_parts();
    let ks_by_column = (0..labels.n_cols())
        .map(|column| count_distinct(&labels, column))
        .collect::<Vec<_>>();
    let surviving_input_order = (0..labels.n_cols())
        .filter(|&column| {
            options
                .max_k
                .is_none_or(|limit| ks_by_column[column] <= limit)
        })
        .collect::<Vec<_>>();

    if surviving_input_order.len() < 2 {
        return Err(MrtreeError::TooFewLayersAfterFiltering);
    }

    let mut canonical_indices = surviving_input_order.clone();
    canonical_indices.sort_by_key(|&column| ks_by_column[column]);
    let effective_labels = labels.reordered_columns(&canonical_indices);
    let effective_headers = cluster_headers.as_ref().map(|headers| {
        canonical_indices
            .iter()
            .map(|&index| headers[index].clone())
            .collect::<Vec<_>>()
    });
    let ks = canonical_indices
        .iter()
        .map(|&column| ks_by_column[column])
        .collect::<Vec<_>>();

    let reorder_warning = (surviving_input_order != canonical_indices).then(|| ReorderWarning {
        original_order: render_order_labels(cluster_headers.as_deref(), &surviving_input_order),
        canonical_order: render_order_labels(cluster_headers.as_deref(), &canonical_indices),
    });

    Ok(PreparedInput {
        effective: EffectiveTable::new(
            sample_header,
            sample_ids,
            effective_headers,
            effective_labels,
            canonical_indices,
            ks,
        )?,
        reorder_warning,
    })
}

fn count_distinct(labels: &LabelMatrix, column: usize) -> usize {
    let mut distinct = HashSet::with_capacity(labels.n_rows());
    distinct.extend(labels.column_iter(column));
    distinct.len()
}

fn render_order_labels(headers: Option<&[String]>, columns: &[usize]) -> Vec<String> {
    if let Some(headers) = headers {
        columns
            .iter()
            .map(|&column| headers[column].clone())
            .collect()
    } else {
        columns
            .iter()
            .map(|&column| fallback_level_name(column))
            .collect()
    }
}

fn fallback_level_name(column: usize) -> String {
    format!("level_{}", column + 1)
}
