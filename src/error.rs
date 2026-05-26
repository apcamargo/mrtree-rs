use thiserror::Error;

#[derive(Debug, Error)]
pub enum MrtreeError {
    #[error("Failed to read TSV input: {0}")]
    TsvRead(String),

    #[error("Failed to write TSV output: {0}")]
    TsvWrite(String),

    #[error("Input contains no data rows")]
    EmptyInput,

    #[error("Input contains only a header row and no samples")]
    HeaderOnlyInput,

    #[error(
        "Input must contain at least three columns: one sample ID column and at least two clustering columns"
    )]
    InputHasTooFewColumns,

    #[error("Line {line} has {actual} fields, expected {expected}")]
    RaggedRow {
        line: usize,
        expected: usize,
        actual: usize,
    },

    #[error("Missing clustering label at line {line}, column {column}")]
    MissingClusterLabel { line: usize, column: usize },

    #[error(
        "Negative cluster label at line {line}, column {column}: {value}; real cluster labels must be non-negative integers and -1 is reserved for synthetic augmented output"
    )]
    NegativeClusterLabel {
        line: usize,
        column: usize,
        value: String,
    },

    #[error("Invalid integer label at line {line}, column {column}: {value}{hint}")]
    InvalidClusterLabel {
        line: usize,
        column: usize,
        value: String,
        hint: String,
    },

    #[error("Fewer than two clustering columns remain after --max-k filtering")]
    TooFewLayersAfterFiltering,

    #[error("Fewer than two effective layers remain after same-K consensus reduction")]
    ConsensusRequiresAtLeastTwoLayers,

    #[error(
        "Input table row count mismatch: label matrix has {label_rows} rows but sample IDs contain {sample_ids}"
    )]
    InputRowCountMismatch {
        label_rows: usize,
        sample_ids: usize,
    },

    #[error("Cluster header count mismatch: expected {expected}, got {actual}")]
    ClusterHeaderCountMismatch { expected: usize, actual: usize },

    #[error(
        "Effective table metadata count mismatch: expected {expected} columns, got {original_column_indices} original indices and {ks} K values"
    )]
    EffectiveMetadataLengthMismatch {
        expected: usize,
        original_column_indices: usize,
        ks: usize,
    },

    #[error("Sample weight length mismatch: expected {expected}, got {actual}")]
    SampleWeightsLengthMismatch { expected: usize, actual: usize },

    #[error("Level weight length mismatch: expected {expected} clustering levels, got {actual}")]
    LevelWeightsLengthMismatch { expected: usize, actual: usize },

    #[error(
        "Invalid level weight at clustering level {}: {weight}; weights must be finite and greater than 0",
        index + 1
    )]
    InvalidLevelWeight { index: usize, weight: f64 },

    #[error("Consensus k-means failed: {0}")]
    ConsensusKMeans(String),

    #[error("Consensus SVD failed: {0}")]
    ConsensusSvd(String),

    #[error("Failed to build rayon thread pool: {0}")]
    ThreadPoolBuild(String),

    #[error("Input contains duplicate sample IDs")]
    DuplicateSampleIds,

    #[error("Frozen sample ID not found: {sample_id}")]
    FrozenSampleIdNotFound { sample_id: String },

    #[error(
        "Frozen samples are not supported together with --consensus; \
         consensus rewrites cluster labels before reconciliation, \
         which would violate the guarantee that frozen rows keep their \
         full path unchanged"
    )]
    FrozenSamplesWithConsensus,

    #[error("Internal algorithm invariant violation: {0}")]
    InternalAlgorithmInvariantViolation(String),
}
