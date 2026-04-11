use clap::{
    Args, Parser,
    builder::styling::{AnsiColor, Style, Styles},
};

const STYLES: Styles = Styles::styled()
    .header(AnsiColor::Cyan.on_default().bold())
    .usage(AnsiColor::Yellow.on_default().bold())
    .literal(AnsiColor::Yellow.on_default().bold())
    .placeholder(Style::new().dimmed());

#[derive(Debug, Clone, Parser)]
#[command(
    name = "mrtree-rs",
    version,
    about = "Reconcile multiresolution cluster label matrices into a consistent hierarchy.",
    max_term_width = 79,
    styles = STYLES
)]
pub(crate) struct Cli {
    /// Input TSV path. Use '-' for stdin
    #[arg(default_value = "-")]
    pub(crate) input: String,

    /// Output TSV path. Use '-' for stdout
    #[arg(default_value = "-")]
    pub(crate) output: String,

    /// Treat the first row as a header and emit a header row on output
    #[arg(long, help_heading = "Input and output")]
    pub(crate) header: bool,

    #[command(flatten)]
    pub(crate) preprocess: PreprocessArgs,

    #[command(flatten)]
    pub(crate) scoring: ScoringArgs,

    #[command(flatten)]
    pub(crate) runtime: RuntimeArgs,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct PreprocessArgs {
    /// Keep only clustering columns where the number of clusters (K) is < N
    #[arg(long = "max-k", value_name = "N", help_heading = "Preprocessing")]
    pub(crate) max_k: Option<usize>,

    /// Merge levels with the same number of clusters by taking their consensus
    #[arg(long, help_heading = "Preprocessing")]
    pub(crate) consensus: bool,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct ScoringArgs {
    /// Use inverse-cluster-size sample weights during scoring and consensus
    #[arg(long = "sample-weighting", help_heading = "Reconciliation")]
    pub(crate) sample_weighting: bool,

    /// Enable synthetic path augmentation
    #[arg(long = "augment-path", help_heading = "Reconciliation")]
    pub(crate) augment_path: bool,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct RuntimeArgs {
    /// Seed used for deterministic consensus clustering
    #[arg(
        long,
        value_name = "N",
        default_value_t = 0,
        help_heading = "Runtime and logging"
    )]
    pub(crate) seed: u64,

    /// Number of worker threads used for candidate evaluation.
    ///
    /// Use 0 to use all available threads.
    /// Values above the available thread count are capped internally.
    #[arg(
        long,
        value_name = "N",
        default_value_t = 1,
        help_heading = "Runtime and logging"
    )]
    pub(crate) threads: usize,

    /// Emit preprocessing and progress details to stderr
    #[arg(short, long, help_heading = "Runtime and logging")]
    pub(crate) verbose: bool,
}
