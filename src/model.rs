use std::cmp::Ordering;
use std::fmt;

use crate::error::MrtreeError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RealLabel(u64);

impl RealLabel {
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }
}

impl fmt::Display for RealLabel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl From<u64> for RealLabel {
    fn from(value: u64) -> Self {
        Self::new(value)
    }
}

impl From<RealLabel> for u64 {
    fn from(value: RealLabel) -> Self {
        value.value()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabelMatrix {
    n_rows: usize,
    n_cols: usize,
    data: Vec<RealLabel>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputTable {
    sample_header: Option<String>,
    sample_ids: Vec<String>,
    cluster_headers: Option<Vec<String>>,
    labels: LabelMatrix,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectiveTable {
    sample_header: Option<String>,
    sample_ids: Vec<String>,
    cluster_headers: Option<Vec<String>>,
    labels: LabelMatrix,
    original_column_indices: Vec<usize>,
    ks: Vec<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PathLabel {
    Real(RealLabel),
    Augmented,
}

impl PathLabel {
    #[must_use]
    pub const fn is_augmented(self) -> bool {
        matches!(self, Self::Augmented)
    }

    #[must_use]
    pub const fn as_real(self) -> Option<RealLabel> {
        match self {
            Self::Real(value) => Some(value),
            Self::Augmented => None,
        }
    }
}

impl fmt::Display for PathLabel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Real(value) => value.fmt(formatter),
            Self::Augmented => (-1_i64).fmt(formatter),
        }
    }
}

impl Ord for PathLabel {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Real(left), Self::Real(right)) => left.cmp(right),
            (Self::Real(_), Self::Augmented) => Ordering::Less,
            (Self::Augmented, Self::Real(_)) => Ordering::Greater,
            (Self::Augmented, Self::Augmented) => Ordering::Equal,
        }
    }
}

impl PartialOrd for PathLabel {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct NodeId {
    pub layer: usize,
    pub label: PathLabel,
}

pub type Path = Vec<PathLabel>;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Edge {
    pub start: NodeId,
    pub end: NodeId,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Candidate {
    pub edge: Edge,
    pub cost: f64,
    pub order: usize,
}

impl LabelMatrix {
    #[must_use]
    pub fn new(n_rows: usize, n_cols: usize, data: Vec<RealLabel>) -> Self {
        assert_eq!(
            data.len(),
            n_rows * n_cols,
            "label matrix data length does not match declared dimensions"
        );
        Self {
            n_rows,
            n_cols,
            data,
        }
    }

    #[must_use]
    pub const fn n_rows(&self) -> usize {
        self.n_rows
    }

    #[must_use]
    pub const fn n_cols(&self) -> usize {
        self.n_cols
    }

    #[must_use]
    pub fn row(&self, row: usize) -> &[RealLabel] {
        let start = row * self.n_cols;
        &self.data[start..start + self.n_cols]
    }

    pub fn column_iter(&self, column: usize) -> impl Iterator<Item = RealLabel> + '_ {
        (0..self.n_rows).map(move |row| self.data[row * self.n_cols + column])
    }

    #[must_use]
    pub fn reordered_columns(&self, indices: &[usize]) -> Self {
        let mut data = Vec::with_capacity(self.n_rows * indices.len());
        for row in 0..self.n_rows {
            let slice = self.row(row);
            for &column in indices {
                data.push(slice[column]);
            }
        }
        Self::new(self.n_rows, indices.len(), data)
    }
}

impl InputTable {
    pub fn new(
        sample_header: Option<String>,
        sample_ids: Vec<String>,
        cluster_headers: Option<Vec<String>>,
        labels: LabelMatrix,
    ) -> crate::Result<Self> {
        if sample_ids.len() != labels.n_rows() {
            return Err(MrtreeError::InputRowCountMismatch {
                label_rows: labels.n_rows(),
                sample_ids: sample_ids.len(),
            });
        }

        if let Some(headers) = cluster_headers.as_ref()
            && headers.len() != labels.n_cols()
        {
            return Err(MrtreeError::ClusterHeaderCountMismatch {
                expected: labels.n_cols(),
                actual: headers.len(),
            });
        }

        Ok(Self {
            sample_header,
            sample_ids,
            cluster_headers,
            labels,
        })
    }

    #[must_use]
    pub fn sample_header(&self) -> Option<&str> {
        self.sample_header.as_deref()
    }

    #[must_use]
    pub fn sample_ids(&self) -> &[String] {
        &self.sample_ids
    }

    #[must_use]
    pub fn cluster_headers(&self) -> Option<&[String]> {
        self.cluster_headers.as_deref()
    }

    #[must_use]
    pub fn labels(&self) -> &LabelMatrix {
        &self.labels
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        Option<String>,
        Vec<String>,
        Option<Vec<String>>,
        LabelMatrix,
    ) {
        (
            self.sample_header,
            self.sample_ids,
            self.cluster_headers,
            self.labels,
        )
    }
}

impl EffectiveTable {
    pub fn new(
        sample_header: Option<String>,
        sample_ids: Vec<String>,
        cluster_headers: Option<Vec<String>>,
        labels: LabelMatrix,
        original_column_indices: Vec<usize>,
        ks: Vec<usize>,
    ) -> crate::Result<Self> {
        if sample_ids.len() != labels.n_rows() {
            return Err(MrtreeError::InputRowCountMismatch {
                label_rows: labels.n_rows(),
                sample_ids: sample_ids.len(),
            });
        }

        if let Some(headers) = cluster_headers.as_ref()
            && headers.len() != labels.n_cols()
        {
            return Err(MrtreeError::ClusterHeaderCountMismatch {
                expected: labels.n_cols(),
                actual: headers.len(),
            });
        }

        if original_column_indices.len() != labels.n_cols() || ks.len() != labels.n_cols() {
            return Err(MrtreeError::EffectiveMetadataLengthMismatch {
                expected: labels.n_cols(),
                original_column_indices: original_column_indices.len(),
                ks: ks.len(),
            });
        }

        Ok(Self {
            sample_header,
            sample_ids,
            cluster_headers,
            labels,
            original_column_indices,
            ks,
        })
    }

    #[must_use]
    pub fn sample_header(&self) -> Option<&str> {
        self.sample_header.as_deref()
    }

    #[must_use]
    pub fn sample_ids(&self) -> &[String] {
        &self.sample_ids
    }

    #[must_use]
    pub fn cluster_headers(&self) -> Option<&[String]> {
        self.cluster_headers.as_deref()
    }

    #[must_use]
    pub fn labels(&self) -> &LabelMatrix {
        &self.labels
    }

    #[must_use]
    pub fn original_column_indices(&self) -> &[usize] {
        &self.original_column_indices
    }

    #[must_use]
    pub fn ks(&self) -> &[usize] {
        &self.ks
    }

    pub(crate) fn validate_output_row_count(&self, actual_rows: usize) -> crate::Result<()> {
        let expected_rows = self.labels.n_rows();
        if actual_rows != expected_rows {
            return Err(MrtreeError::InternalAlgorithmInvariantViolation(format!(
                "output contains {actual_rows} rows, expected {expected_rows}"
            )));
        }

        Ok(())
    }

    pub(crate) fn validate_output_path(
        &self,
        row_idx: usize,
        path: &[PathLabel],
    ) -> crate::Result<()> {
        let expected_levels = self.labels.n_cols();
        if path.len() != expected_levels {
            return Err(MrtreeError::InternalAlgorithmInvariantViolation(format!(
                "output row {row_idx} has {} labels, expected {expected_levels}",
                path.len()
            )));
        }

        Ok(())
    }
}
