mod algorithm;
pub mod consensus;
pub mod error;
pub mod io;
pub mod model;
pub mod preprocess;
pub mod reconcile;
mod summary;
pub mod weights;

use tracing::{Level, debug, enabled, info, warn};

pub use crate::error::MrtreeError as Error;

use crate::model::{InputTable, Path};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunPreprocessOptions {
    pub max_k: Option<usize>,
    pub consensus: bool,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct RunScoringOptions {
    pub sample_weighting: bool,
    pub level_weights: Option<Vec<f64>>,
    pub augment_path: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunRuntimeOptions {
    pub seed: u64,
    pub threads: usize,
}

#[derive(Debug, Clone, Default, PartialEq)]
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

/// # Errors
///
/// Returns an error if preprocessing, optional consensus reduction, weight
/// validation/remapping, reconciliation, or output validation fails.
#[allow(clippy::too_many_lines)]
pub fn reconcile_input(input: InputTable, options: &RunOptions) -> Result<RunResult> {
    let original_level_count = input.labels().n_cols();
    let prepared = preprocess::prepare(
        input,
        &preprocess::PrepareOptions {
            max_k: options.preprocess.max_k,
        },
    )?;

    let effective_level_weights = options
        .scoring
        .level_weights
        .as_ref()
        .map(|weights| weights::LevelWeights::new(original_level_count, weights.clone()))
        .transpose()?
        .map(|weights| {
            weights
                .map_to_effective(prepared.effective().original_column_indices())
                .normalize_by_max()
        });

    if enabled!(Level::INFO) {
        let input_clusters = summary::clusters_per_level(prepared.effective().labels());
        if let Some(level_weights) = effective_level_weights.as_ref() {
            info!(
                rows = prepared.effective().labels().n_rows(),
                levels = prepared.effective().labels().n_cols(),
                clusters_per_level = ?input_clusters,
                level_weights_per_level = ?level_weights.as_slice(),
                "Prepared input"
            );
        } else {
            info!(
                rows = prepared.effective().labels().n_rows(),
                levels = prepared.effective().labels().n_cols(),
                clusters_per_level = ?input_clusters,
                "Prepared input"
            );
        }
    }
    if let Some(reorder_warning) = prepared.reorder_warning() {
        warn!("{reorder_warning}");
    }

    let consensus_state = if options.preprocess.consensus {
        info!(
            seed = options.runtime.seed,
            "Enabled same-K consensus reduction"
        );
        let state = consensus::reduce_same_k_groups(
            prepared.effective(),
            effective_level_weights
                .as_ref()
                .map(weights::LevelWeights::as_slice),
            &consensus::ConsensusOptions {
                sample_weighting: options.scoring.sample_weighting,
                seed: options.runtime.seed,
            },
        )?;
        let reduced_level_weights = effective_level_weights
            .as_ref()
            .map(|weights| weights.reduce_by_group_mapping(state.group_mapping()));
        if enabled!(Level::DEBUG) {
            let reduced_clusters = summary::clusters_per_level(state.reduced_labels());
            if let Some(level_weights) = reduced_level_weights.as_ref() {
                debug!(
                    levels = state.reduced_labels().n_cols(),
                    clusters_per_level = ?reduced_clusters,
                    level_weights_per_level = ?level_weights.as_slice(),
                    seed = options.runtime.seed,
                    "Prepared consensus input"
                );
            } else {
                debug!(
                    levels = state.reduced_labels().n_cols(),
                    clusters_per_level = ?reduced_clusters,
                    seed = options.runtime.seed,
                    "Prepared consensus input"
                );
            }
        }
        Some((state, reduced_level_weights))
    } else {
        None
    };
    let reconcile_input = consensus_state
        .as_ref()
        .map_or(prepared.effective().labels(), |(state, _)| {
            state.reduced_labels()
        });
    let sample_weights = options
        .scoring
        .sample_weighting
        .then(|| weights::compute_sample_weights(reconcile_input));
    let reconcile_level_weights = consensus_state.as_ref().map_or_else(
        || {
            effective_level_weights
                .as_ref()
                .map(weights::LevelWeights::as_slice)
        },
        |(_, reduced_level_weights)| {
            reduced_level_weights
                .as_ref()
                .map(weights::LevelWeights::as_slice)
        },
    );

    let reconciled = reconcile::reconcile_labels(
        reconcile_input,
        sample_weights.as_ref(),
        reconcile_level_weights,
        &reconcile::ReconcileOptions {
            augment_path: options.scoring.augment_path,
            threads: options.runtime.threads,
        },
    )?;

    let output_paths = if let Some((state, _)) = consensus_state.as_ref() {
        state.expand_paths(&reconciled)?
    } else {
        reconciled
    };
    if enabled!(Level::INFO) {
        let output_summary = summary::summarize_output(prepared.effective(), &output_paths)?;
        info!(
            levels = output_summary.clusters_per_level.len(),
            clusters_per_level = ?output_summary.clusters_per_level,
            reassignments_per_level = ?output_summary.reassignments_per_level,
            rows_reassigned = output_summary.rows_reassigned,
            "Finished reconciliation"
        );
    }
    let (effective, reorder_warning) = prepared.into_effective_and_warning();

    Ok(RunResult {
        effective,
        paths: output_paths,
        reorder_warning,
    })
}
