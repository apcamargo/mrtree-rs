mod algorithm;
pub mod consensus;
pub mod error;
pub mod io;
pub mod model;
pub mod preprocess;
pub mod reconcile;
mod summary;
pub mod weights;

use std::{
    fmt,
    io::{IsTerminal, stderr},
};

use time::{OffsetDateTime, format_description::well_known::Rfc3339};

pub use crate::error::MrtreeError as Error;

use crate::model::{InputTable, Path};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LogLevel {
    Info,
    Warn,
}

impl LogLevel {
    fn as_str(self) -> &'static str {
        match self {
            Self::Info => "INFO",
            Self::Warn => "WARN",
        }
    }
}

fn log_timestamp() -> String {
    let now = OffsetDateTime::now_local().unwrap_or_else(|_| OffsetDateTime::now_utc());
    let now = now.replace_nanosecond(0).unwrap_or(now);

    now.format(&Rfc3339)
        .unwrap_or_else(|_| "<timestamp-unavailable>".to_owned())
}

fn log_message(level: LogLevel, args: fmt::Arguments<'_>) {
    let prefix = format!("[{} | {}]", log_timestamp(), level.as_str());

    if stderr().is_terminal() && level == LogLevel::Info {
        eprintln!("\x1b[90m{prefix}\x1b[0m {args}");
    } else {
        eprintln!("{prefix} {args}");
    }
}

pub(crate) fn log_info(verbose: bool, args: fmt::Arguments<'_>) {
    if verbose {
        log_message(LogLevel::Info, args);
    }
}

pub(crate) fn log_warn(args: fmt::Arguments<'_>) {
    log_message(LogLevel::Warn, args);
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunPreprocessOptions {
    pub max_k: Option<usize>,
    pub consensus: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunScoringOptions {
    pub sample_weighted: bool,
    pub augment_path: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunRuntimeOptions {
    pub seed: u64,
    pub threads: usize,
    pub verbose: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunOptions {
    pub preprocess: RunPreprocessOptions,
    pub scoring: RunScoringOptions,
    pub runtime: RunRuntimeOptions,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunResult {
    pub effective: crate::model::EffectiveTable,
    pub paths: Vec<Path>,
    pub reorder_warning: Option<preprocess::ReorderWarning>,
}

pub fn reconcile_input(input: InputTable, options: &RunOptions) -> Result<RunResult> {
    let prepared = preprocess::prepare(
        input,
        &preprocess::PrepareOptions {
            max_k: options.preprocess.max_k,
        },
    )?;
    if options.runtime.verbose {
        let input_clusters = summary::clusters_per_level(prepared.effective().labels());
        log_info(
            true,
            format_args!(
                "Prepared input: Rows={}, Levels={}, Clusters per level={:?}",
                prepared.effective().labels().n_rows(),
                prepared.effective().labels().n_cols(),
                input_clusters
            ),
        );
    }
    if let Some(reorder_warning) = prepared.reorder_warning() {
        log_warn(format_args!("{reorder_warning}"));
    }

    if options.scoring.sample_weighted {
        log_info(
            options.runtime.verbose,
            format_args!("Enabled sample weighting"),
        );
    }

    let consensus_state = if options.preprocess.consensus {
        let state = consensus::reduce_same_k_groups(
            prepared.effective(),
            &consensus::ConsensusOptions {
                sample_weighted: options.scoring.sample_weighted,
                seed: options.runtime.seed,
            },
        )?;
        if options.runtime.verbose {
            let reduced_clusters = summary::clusters_per_level(state.reduced_labels());
            log_info(
                true,
                format_args!(
                    "Prepared consensus input: Levels={}, Clusters per level={:?}, Seed={}",
                    state.reduced_labels().n_cols(),
                    reduced_clusters,
                    options.runtime.seed
                ),
            );
        }
        Some(state)
    } else {
        None
    };
    let reconcile_input = consensus_state.as_ref().map_or(
        prepared.effective().labels(),
        consensus::ConsensusReduction::reduced_labels,
    );
    let sample_weights = options
        .scoring
        .sample_weighted
        .then(|| weights::compute_sample_weights(reconcile_input));

    let reconciled = reconcile::reconcile_labels(
        reconcile_input,
        sample_weights.as_ref(),
        &reconcile::ReconcileOptions {
            augment_path: options.scoring.augment_path,
            threads: options.runtime.threads,
            verbose: options.runtime.verbose,
        },
    )?;

    let output_paths = if let Some(state) = consensus_state.as_ref() {
        state.expand_paths(&reconciled)?
    } else {
        reconciled
    };
    if options.runtime.verbose {
        let output_summary = summary::summarize_output(prepared.effective(), &output_paths)?;
        log_info(
            true,
            format_args!(
                "Finished reconciliation: Levels={}, Clusters per level={:?}, Reassignments per level={:?}, Rows reassigned={}",
                output_summary.clusters_per_level.len(),
                output_summary.clusters_per_level,
                output_summary.reassignments_per_level,
                output_summary.rows_reassigned
            ),
        );
    }
    let (effective, reorder_warning) = prepared.into_effective_and_warning();

    Ok(RunResult {
        effective,
        paths: output_paths,
        reorder_warning,
    })
}
