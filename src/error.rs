use thiserror::Error;

#[derive(Debug, Error)]
pub enum MrtreeError {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Csv(#[from] csv::Error),

    #[error("input contains no data rows")]
    EmptyInput,

    #[error("input contains only a header row and no samples")]
    HeaderOnlyInput,

    #[error(
        "input must contain at least three columns: one sample ID column and at least two clustering columns"
    )]
    InputHasTooFewColumns,

    #[error("line {line} has {actual} fields, expected {expected}")]
    RaggedRow {
        line: usize,
        expected: usize,
        actual: usize,
    },

    #[error("missing clustering label at line {line}, column {column}")]
    MissingClusterLabel { line: usize, column: usize },

    #[error(
        "negative cluster label at line {line}, column {column}: {value}; real cluster labels must be non-negative integers and -1 is reserved for synthetic augmented output"
    )]
    NegativeClusterLabel {
        line: usize,
        column: usize,
        value: String,
    },

    #[error("invalid integer label at line {line}, column {column}: {value}{hint}")]
    InvalidClusterLabel {
        line: usize,
        column: usize,
        value: String,
        hint: String,
    },

    #[error("fewer than two clustering columns remain after --max-k filtering")]
    TooFewLayersAfterFiltering,

    #[error("fewer than two effective layers remain after same-K consensus reduction")]
    ConsensusRequiresAtLeastTwoLayers,

    #[error(
        "input table row count mismatch: label matrix has {label_rows} rows but sample IDs contain {sample_ids}"
    )]
    InputRowCountMismatch {
        label_rows: usize,
        sample_ids: usize,
    },

    #[error("cluster header count mismatch: expected {expected}, got {actual}")]
    ClusterHeaderCountMismatch { expected: usize, actual: usize },

    #[error(
        "effective table metadata count mismatch: expected {expected} columns, got {original_column_indices} original indices and {ks} K values"
    )]
    EffectiveMetadataLengthMismatch {
        expected: usize,
        original_column_indices: usize,
        ks: usize,
    },

    #[error("sample weight length mismatch: expected {expected}, got {actual}")]
    SampleWeightsLengthMismatch { expected: usize, actual: usize },

    #[error("consensus k-means failed: {0}")]
    ConsensusKMeans(String),

    #[error("consensus SVD failed: {0}")]
    ConsensusSvd(String),

    #[error("failed to build rayon thread pool: {0}")]
    ThreadPoolBuild(String),

    #[error("internal algorithm invariant violation: {0}")]
    InternalAlgorithmInvariantViolation(String),
}
