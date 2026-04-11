use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};

use float_cmp::approx_eq;
#[cfg(test)]
use float_cmp::assert_approx_eq;
use rayon::prelude::*;
use rayon::ThreadPool;

use super::paths;
use super::tree;
use crate::error::MrtreeError;
use crate::model::{Candidate, Edge, LabelMatrix, NodeId, Path, PathLabel, RealLabel};

const MIN_LABEL_COMPARISONS_PER_TASK: usize = 16_384;
const MAX_SCORING_DISTANCE_CACHE_BYTES: usize = 128 * 1024 * 1024;

fn available_threads() -> usize {
    std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get)
}

fn resolve_thread_count(requested_threads: usize) -> usize {
    let available = available_threads();
    if requested_threads == 0 {
        available
    } else {
        requested_threads.min(available)
    }
}

#[derive(Debug, Clone)]
struct AssignedPathState {
    assigned_path_ids: Vec<paths::PathId>,
    rows_by_path: HashMap<paths::PathId, BTreeSet<usize>>,
}

impl AssignedPathState {
    fn new(assigned_path_ids: Vec<paths::PathId>) -> Self {
        let mut rows_by_path = HashMap::<paths::PathId, BTreeSet<usize>>::new();
        for (row, &path_id) in assigned_path_ids.iter().enumerate() {
            rows_by_path.entry(path_id).or_default().insert(row);
        }

        Self {
            assigned_path_ids,
            rows_by_path,
        }
    }

    fn len(&self) -> usize {
        self.assigned_path_ids.len()
    }

    fn path_id(&self, row: usize) -> paths::PathId {
        self.assigned_path_ids[row]
    }

    fn assigned_path_ids(&self) -> &[paths::PathId] {
        &self.assigned_path_ids
    }

    fn realized_path_ids_in_row_order(&self) -> Vec<paths::PathId> {
        let mut first_rows = self
            .rows_by_path
            .iter()
            .map(|(&path_id, rows)| {
                let &first_row = rows
                    .first()
                    .expect("every active path should have at least one row");
                (first_row, path_id)
            })
            .collect::<Vec<_>>();
        first_rows.sort_unstable_by_key(|(first_row, _)| *first_row);
        first_rows.into_iter().map(|(_, path_id)| path_id).collect()
    }

    fn assign_row(&mut self, row: usize, new_path_id: paths::PathId) {
        let old_path_id = self.assigned_path_ids[row];
        if old_path_id == new_path_id {
            return;
        }

        let remove_old_path = {
            let old_rows = self
                .rows_by_path
                .get_mut(&old_path_id)
                .expect("assigned row should belong to an active path");
            let removed = old_rows.remove(&row);
            debug_assert!(removed, "row should exist in its old path row set");

            old_rows.is_empty()
        };
        if remove_old_path {
            self.rows_by_path.remove(&old_path_id);
        }

        self.rows_by_path
            .entry(new_path_id)
            .or_default()
            .insert(row);

        self.assigned_path_ids[row] = new_path_id;
    }
}

#[derive(Debug, Clone, Default)]
struct ChildGroup {
    rows: Vec<usize>,
    distance_table: Option<Vec<u32>>,
}

#[derive(Debug, Clone)]
struct CandidateJob<'a> {
    edge: Edge,
    child_group_index: usize,
    pruned_paths: Vec<paths::ScoringPathView<'a>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CandidateChunkTask {
    job_index: usize,
    row_start: usize,
    row_end: usize,
}

#[derive(Debug, Clone)]
struct PreparedRound<'a> {
    feasible_path_count: usize,
    child_groups: Vec<ChildGroup>,
    jobs: Vec<CandidateJob<'a>>,
}

#[derive(Debug)]
struct ReconciliationState {
    path_store: paths::PathStore,
    assigned_state: AssignedPathState,
    feasible_path_ids: Vec<paths::PathId>,
    lowest_done: BTreeSet<usize>,
}

#[derive(Debug)]
struct RoundCandidates {
    candidates: Vec<Edge>,
    lowest_layers: Vec<usize>,
}

struct ScoringInputs<'a> {
    assigned_state: &'a AssignedPathState,
    path_store: &'a paths::PathStore,
    labels: &'a LabelMatrix,
    sample_weights: &'a [f64],
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ThreadedScoringPlan {
    Serial,
    ParallelChunked { tasks: Vec<CandidateChunkTask> },
}

impl<'a> PreparedRound<'a> {
    fn new(
        candidates: &[Edge],
        feasible_path_ids: &[paths::PathId],
        assigned_state: &AssignedPathState,
        path_store: &'a paths::PathStore,
        labels: &LabelMatrix,
        thread_pool: Option<&ThreadPool>,
    ) -> Self {
        let feasible_paths = feasible_path_ids
            .iter()
            .copied()
            .map(|path_id| path_store.get(path_id).as_slice())
            .collect::<Vec<_>>();
        let feasible_path_count = feasible_paths.len();
        let mut child_group_index_by_node = HashMap::with_capacity(candidates.len());
        let mut child_groups = Vec::with_capacity(candidates.len());
        let mut child_layers = Vec::with_capacity(candidates.len());
        for edge in candidates {
            child_group_index_by_node
                .entry(edge.end.clone())
                .or_insert_with(|| {
                    child_layers.push(edge.end.layer);
                    child_groups.push(ChildGroup::default());
                    child_groups.len() - 1
                });
        }
        child_layers.sort_unstable();
        child_layers.dedup();

        for row in 0..assigned_state.len() {
            let current_path = path_store.get(assigned_state.path_id(row));
            for &child_layer in &child_layers {
                let child = NodeId {
                    layer: child_layer,
                    label: current_path[child_layer],
                };
                if let Some(&group_index) = child_group_index_by_node.get(&child) {
                    child_groups[group_index].rows.push(row);
                }
            }
        }

        let jobs = build_candidate_jobs(
            candidates,
            feasible_path_ids,
            path_store,
            &child_group_index_by_node,
            thread_pool,
        );

        let mut prepared = Self {
            feasible_path_count,
            child_groups,
            jobs,
        };
        prepared.maybe_build_distance_tables(&feasible_paths, labels, thread_pool);
        prepared
    }

    fn maybe_build_distance_tables(
        &mut self,
        feasible_paths: &[&[PathLabel]],
        labels: &LabelMatrix,
        thread_pool: Option<&ThreadPool>,
    ) {
        let cache_bytes = self
            .child_groups
            .iter()
            .map(|group| group.rows.len())
            .sum::<usize>()
            .saturating_mul(self.feasible_path_count)
            .saturating_mul(std::mem::size_of::<u32>());
        if cache_bytes == 0 || cache_bytes > MAX_SCORING_DISTANCE_CACHE_BYTES {
            return;
        }

        let build_distance_table = |group: &mut ChildGroup| {
            let mut distances =
                Vec::with_capacity(group.rows.len().saturating_mul(feasible_paths.len()));
            for &row in &group.rows {
                let row_labels = labels.row(row);
                for feasible_path in feasible_paths {
                    distances.push(
                        u32::try_from(paths::path_distance(row_labels, feasible_path))
                            .expect("path distance should fit in u32"),
                    );
                }
            }
            group.distance_table = Some(distances);
        };

        if self.child_groups.len() > 1 {
            if let Some(pool) = thread_pool {
                pool.install(|| {
                    self.child_groups
                        .par_iter_mut()
                        .for_each(build_distance_table);
                });
            } else {
                self.child_groups.iter_mut().for_each(build_distance_table);
            }
        } else {
            self.child_groups.iter_mut().for_each(build_distance_table);
        }
    }

    fn rows_for_job(&self, job_index: usize) -> &[usize] {
        &self.child_groups[self.jobs[job_index].child_group_index].rows
    }
}

fn build_candidate_job<'a>(
    edge: &Edge,
    feasible_path_ids: &[paths::PathId],
    path_store: &'a paths::PathStore,
    child_group_index_by_node: &HashMap<NodeId, usize>,
) -> CandidateJob<'a> {
    CandidateJob {
        edge: edge.clone(),
        child_group_index: *child_group_index_by_node
            .get(&edge.end)
            .expect("candidate children should have a prepared child group"),
        pruned_paths: paths::prune_scoring_paths(feasible_path_ids, edge, path_store),
    }
}

fn build_candidate_jobs<'a>(
    candidates: &[Edge],
    feasible_path_ids: &[paths::PathId],
    path_store: &'a paths::PathStore,
    child_group_index_by_node: &HashMap<NodeId, usize>,
    thread_pool: Option<&ThreadPool>,
) -> Vec<CandidateJob<'a>> {
    if let Some(pool) = thread_pool.filter(|_| candidates.len() > 1) {
        pool.install(|| {
            candidates
                .par_iter()
                .map(|edge| {
                    build_candidate_job(
                        edge,
                        feasible_path_ids,
                        path_store,
                        child_group_index_by_node,
                    )
                })
                .collect()
        })
    } else {
        candidates
            .iter()
            .map(|edge| {
                build_candidate_job(
                    edge,
                    feasible_path_ids,
                    path_store,
                    child_group_index_by_node,
                )
            })
            .collect()
    }
}

impl ReconciliationState {
    fn new(labels: &LabelMatrix) -> Self {
        let mut path_store = paths::PathStore::new();
        let assigned_path_ids = (0..labels.n_rows())
            .map(|row| {
                path_store.intern(
                    labels
                        .row(row)
                        .iter()
                        .copied()
                        .map(PathLabel::Real)
                        .collect::<Path>(),
                )
            })
            .collect::<Vec<_>>();
        let assigned_state = AssignedPathState::new(assigned_path_ids);
        let feasible_path_ids = assigned_state.realized_path_ids_in_row_order();

        Self {
            path_store,
            assigned_state,
            feasible_path_ids,
            lowest_done: BTreeSet::new(),
        }
    }

    fn materialize_output(&self) -> Vec<Path> {
        paths::materialize_paths(self.assigned_state.assigned_path_ids(), &self.path_store)
    }

    fn collect_round_candidates(&self) -> Option<RoundCandidates> {
        let current_tree = tree::construct_tree_from_ids(
            &self.path_store,
            self.assigned_state.assigned_path_ids(),
        );
        let bad_nodes = tree::get_bad_nodes(&current_tree);
        if bad_nodes.is_empty() {
            return None;
        }

        let mut candidates = current_tree
            .iter()
            .filter(|edge| bad_nodes.contains(&edge.end))
            .cloned()
            .collect::<Vec<_>>();
        let lowest_layers = two_shallowest_layers(&candidates);
        candidates.retain(|edge| lowest_layers.contains(&edge.start.layer));

        Some(RoundCandidates {
            candidates,
            lowest_layers,
        })
    }

    fn maybe_augment_feasible_paths(&mut self, lowest_layers: &[usize], augment_path: bool) {
        if !augment_path {
            return;
        }

        let new_layers = lowest_layers
            .iter()
            .copied()
            .filter(|layer| !self.lowest_done.contains(layer))
            .collect::<Vec<_>>();
        if !new_layers.is_empty() {
            self.feasible_path_ids =
                paths::augment_path_ids(&self.feasible_path_ids, &new_layers, &mut self.path_store);
        }
        self.lowest_done.extend(lowest_layers.iter().copied());
    }

    fn apply_selected_candidate(
        &mut self,
        edge: &Edge,
        affected_rows: &[usize],
        labels: &LabelMatrix,
        augment_path: bool,
        lowest_layers: &[usize],
    ) {
        self.feasible_path_ids =
            paths::prune_path_ids(&self.feasible_path_ids, edge, &mut self.path_store);
        reassign_affected_samples(
            edge,
            affected_rows,
            &self.feasible_path_ids,
            &self.path_store,
            labels,
            &mut self.assigned_state,
        );

        if augment_path {
            let retained_augmented = self
                .feasible_path_ids
                .iter()
                .copied()
                .filter(|&path_id| {
                    lowest_layers
                        .iter()
                        .any(|&layer| self.path_store.get(path_id)[layer] == PathLabel::Augmented)
                })
                .collect::<Vec<_>>();
            let realized = self.assigned_state.realized_path_ids_in_row_order();
            self.feasible_path_ids =
                paths::unique_path_ids(realized.into_iter().chain(retained_augmented));
        } else {
            self.feasible_path_ids = self.assigned_state.realized_path_ids_in_row_order();
        }
    }
}

pub(crate) fn run(
    labels: &LabelMatrix,
    sample_weights: &[f64],
    options: crate::reconcile::ReconcileOptions,
) -> crate::Result<Vec<Path>> {
    if labels.n_cols() < 2 {
        return Err(MrtreeError::InternalAlgorithmInvariantViolation(
            "reconciliation requires at least two layers".to_owned(),
        ));
    }

    if sample_weights.len() != labels.n_rows() {
        return Err(MrtreeError::SampleWeightsLengthMismatch {
            expected: labels.n_rows(),
            actual: sample_weights.len(),
        });
    }

    let thread_count = resolve_thread_count(options.threads);
    crate::log_info(
        options.verbose,
        format_args!(
            "Running reconciliation: Rows={}, Levels={}, Weighted={}, Augment path={}",
            labels.n_rows(),
            labels.n_cols(),
            has_effective_weighting(sample_weights),
            options.augment_path
        ),
    );

    let mut state = ReconciliationState::new(labels);
    let thread_pool = if thread_count > 1 {
        Some(
            rayon::ThreadPoolBuilder::new()
                .num_threads(thread_count)
                .build()
                .map_err(|error| MrtreeError::ThreadPoolBuild(error.to_string()))?,
        )
    } else {
        None
    };

    loop {
        let Some(round) = state.collect_round_candidates() else {
            return Ok(state.materialize_output());
        };

        state.maybe_augment_feasible_paths(&round.lowest_layers, options.augment_path);
        let scoring_inputs = ScoringInputs {
            assigned_state: &state.assigned_state,
            path_store: &state.path_store,
            labels,
            sample_weights,
        };
        let prepared_round = PreparedRound::new(
            &round.candidates,
            &state.feasible_path_ids,
            &state.assigned_state,
            &state.path_store,
            labels,
            thread_pool.as_ref(),
        );
        let scoring_plan = thread_pool
            .as_ref()
            .map_or(ThreadedScoringPlan::Serial, |pool| {
                plan_threaded_scoring(&prepared_round, labels, pool.current_num_threads())
            });
        let selected = select_best_candidate(
            &prepared_round,
            &scoring_plan,
            &scoring_inputs,
            thread_pool.as_ref(),
        )
        .ok_or_else(|| {
            MrtreeError::InternalAlgorithmInvariantViolation(
                "no candidate edges available while bad nodes remain".to_owned(),
            )
        })?;

        let affected_rows = prepared_round.rows_for_job(selected.order).to_vec();
        drop(prepared_round);
        state.apply_selected_candidate(
            &selected.edge,
            &affected_rows,
            labels,
            options.augment_path,
            &round.lowest_layers,
        );
    }
}

fn two_shallowest_layers(candidates: &[Edge]) -> Vec<usize> {
    let mut layers = candidates
        .iter()
        .map(|edge| edge.start.layer)
        .collect::<Vec<_>>();
    layers.sort_unstable();
    layers.dedup();
    layers.into_iter().take(2).collect()
}

fn compare_candidates(left: &Candidate, right: &Candidate) -> Ordering {
    left.cost
        .total_cmp(&right.cost)
        .then(left.order.cmp(&right.order))
}

fn row_matches_candidate_child(current_path: &[PathLabel], edge: &Edge) -> bool {
    current_path[edge.end.layer] == edge.end.label
}

fn row_requires_reassignment(current_path: &[PathLabel], edge: &Edge) -> bool {
    row_matches_candidate_child(current_path, edge)
        && current_path[edge.start.layer] != edge.start.label
}

fn best_scoring_path_index<'a, F>(
    pruned_paths: &[paths::ScoringPathView<'a>],
    mut distance_for_path: F,
) -> usize
where
    F: FnMut(usize, &paths::ScoringPathView<'a>) -> usize,
{
    let mut best_index = 0;
    let mut best_distance = usize::MAX;

    for (index, path) in pruned_paths.iter().enumerate() {
        let distance = distance_for_path(index, path);
        if distance < best_distance {
            best_distance = distance;
            best_index = index;
        }
    }

    best_index
}

fn plan_threaded_scoring(
    prepared_round: &PreparedRound<'_>,
    labels: &LabelMatrix,
    n_threads: usize,
) -> ThreadedScoringPlan {
    if n_threads <= 1 {
        return ThreadedScoringPlan::Serial;
    }

    let estimated_work = prepared_round
        .jobs
        .iter()
        .map(|job| {
            prepared_round.child_groups[job.child_group_index]
                .rows
                .len()
                .saturating_mul(job.pruned_paths.len())
                .saturating_mul(labels.n_cols())
        })
        .sum::<usize>();
    if estimated_work < n_threads.saturating_mul(MIN_LABEL_COMPARISONS_PER_TASK) {
        return ThreadedScoringPlan::Serial;
    }

    let tasks = build_chunk_tasks(prepared_round, labels.n_cols());
    if tasks.len() <= 1 {
        ThreadedScoringPlan::Serial
    } else {
        ThreadedScoringPlan::ParallelChunked { tasks }
    }
}

fn build_chunk_tasks(prepared_round: &PreparedRound<'_>, n_cols: usize) -> Vec<CandidateChunkTask> {
    let mut tasks = Vec::new();

    for (job_index, job) in prepared_round.jobs.iter().enumerate() {
        let group = &prepared_round.child_groups[job.child_group_index];
        if group.rows.is_empty() {
            continue;
        }

        let comparisons_per_row = job.pruned_paths.len().max(1).saturating_mul(n_cols);
        let chunk_rows = (MIN_LABEL_COMPARISONS_PER_TASK / comparisons_per_row).max(1);
        for row_start in (0..group.rows.len()).step_by(chunk_rows) {
            let row_end = (row_start + chunk_rows).min(group.rows.len());
            tasks.push(CandidateChunkTask {
                job_index,
                row_start,
                row_end,
            });
        }
    }

    tasks
}

fn select_best_candidate(
    prepared_round: &PreparedRound<'_>,
    scoring_plan: &ThreadedScoringPlan,
    inputs: &ScoringInputs<'_>,
    thread_pool: Option<&ThreadPool>,
) -> Option<Candidate> {
    match (scoring_plan, thread_pool) {
        (ThreadedScoringPlan::ParallelChunked { tasks }, Some(pool)) => {
            let partial_costs = pool.install(|| {
                tasks
                    .par_iter()
                    .map(|task| {
                        (
                            task.job_index,
                            candidate_chunk_cost(task, prepared_round, inputs),
                        )
                    })
                    .collect::<Vec<_>>()
            });
            let mut candidate_costs = vec![0.0; prepared_round.jobs.len()];
            for (job_index, partial_cost) in partial_costs {
                candidate_costs[job_index] += partial_cost;
            }

            prepared_round
                .jobs
                .iter()
                .zip(candidate_costs)
                .enumerate()
                .map(|(order, (job, cost))| Candidate {
                    edge: job.edge.clone(),
                    cost,
                    order,
                })
                .min_by(compare_candidates)
        }
        _ => prepared_round
            .jobs
            .iter()
            .enumerate()
            .map(|(order, job)| Candidate {
                edge: job.edge.clone(),
                cost: candidate_cost(job, prepared_round, inputs),
                order,
            })
            .min_by(compare_candidates),
    }
}

fn count_path_changes(current_path: &[PathLabel], new_path: &[PathLabel]) -> usize {
    current_path
        .iter()
        .zip(new_path.iter())
        .filter(|(lhs, rhs)| lhs != rhs)
        .count()
}

fn accumulate_candidate_cost<'a, I, F>(
    pruned_paths: &[paths::ScoringPathView<'a>],
    rows: I,
    assigned_state: &AssignedPathState,
    path_store: &paths::PathStore,
    labels: &LabelMatrix,
    sample_weights: &[f64],
    mut distance_for_row: F,
) -> f64
where
    I: IntoIterator<Item = (usize, usize)>,
    F: FnMut(usize, &[RealLabel], &paths::ScoringPathView<'a>) -> usize,
{
    let mut total_cost = 0.0;

    for (row_offset, row) in rows {
        let current_path = path_store.get(assigned_state.path_id(row));
        let row_labels = labels.row(row);
        let best_index = best_scoring_path_index(pruned_paths, |_, path| {
            distance_for_row(row_offset, row_labels, path)
        });
        let new_path = pruned_paths[best_index].as_slice();
        total_cost += sample_weights[row] * count_path_changes(current_path, new_path) as f64;
    }

    total_cost
}

fn candidate_cost(
    job: &CandidateJob<'_>,
    prepared_round: &PreparedRound<'_>,
    inputs: &ScoringInputs<'_>,
) -> f64 {
    let group = &prepared_round.child_groups[job.child_group_index];
    accumulate_job_cost(
        job,
        group,
        0,
        group.rows.len(),
        prepared_round.feasible_path_count,
        inputs,
    )
}

fn candidate_chunk_cost(
    task: &CandidateChunkTask,
    prepared_round: &PreparedRound<'_>,
    inputs: &ScoringInputs<'_>,
) -> f64 {
    let job = &prepared_round.jobs[task.job_index];
    let group = &prepared_round.child_groups[job.child_group_index];
    accumulate_job_cost(
        job,
        group,
        task.row_start,
        task.row_end,
        prepared_round.feasible_path_count,
        inputs,
    )
}

fn accumulate_job_cost(
    job: &CandidateJob<'_>,
    group: &ChildGroup,
    row_start: usize,
    row_end: usize,
    feasible_path_count: usize,
    inputs: &ScoringInputs<'_>,
) -> f64 {
    let rows = group.rows[row_start..row_end]
        .iter()
        .copied()
        .enumerate()
        .map(|(index, row)| (row_start + index, row));

    if let Some(distance_table) = group.distance_table.as_ref() {
        accumulate_candidate_cost(
            &job.pruned_paths,
            rows,
            inputs.assigned_state,
            inputs.path_store,
            inputs.labels,
            inputs.sample_weights,
            |row_offset, row_labels, path| {
                let cached_distances = &distance_table
                    [row_offset * feasible_path_count..(row_offset + 1) * feasible_path_count];
                match path {
                    paths::ScoringPathView::Feasible { feasible_index, .. } => {
                        usize::try_from(cached_distances[*feasible_index])
                            .expect("cached path distance should fit in usize")
                    }
                    paths::ScoringPathView::Owned(path) => {
                        paths::path_distance(row_labels, path.as_slice())
                    }
                }
            },
        )
    } else {
        accumulate_candidate_cost(
            &job.pruned_paths,
            rows,
            inputs.assigned_state,
            inputs.path_store,
            inputs.labels,
            inputs.sample_weights,
            |_, row_labels, path| paths::path_distance(row_labels, path.as_slice()),
        )
    }
}

fn reassign_rows<I>(
    rows: I,
    edge: &Edge,
    feasible_path_ids: &[paths::PathId],
    path_store: &paths::PathStore,
    labels: &LabelMatrix,
    assigned_state: &mut AssignedPathState,
) where
    I: IntoIterator<Item = usize>,
{
    for row in rows {
        let current_path = path_store.get(assigned_state.path_id(row));
        if !row_requires_reassignment(current_path, edge) {
            continue;
        }

        let path_index =
            paths::assign_row_to_path_id(labels.row(row), feasible_path_ids, path_store);
        assigned_state.assign_row(row, feasible_path_ids[path_index]);
    }
}

fn reassign_affected_samples(
    edge: &Edge,
    affected_rows: &[usize],
    feasible_path_ids: &[paths::PathId],
    path_store: &paths::PathStore,
    labels: &LabelMatrix,
    assigned_state: &mut AssignedPathState,
) {
    reassign_rows(
        affected_rows.iter().copied(),
        edge,
        feasible_path_ids,
        path_store,
        labels,
        assigned_state,
    );
}

fn has_effective_weighting(sample_weights: &[f64]) -> bool {
    let Some((&first, rest)) = sample_weights.split_first() else {
        return false;
    };

    rest.iter().any(|&weight| !approx_eq!(f64, weight, first))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reconcile::ReconcileOptions;

    fn label(value: u64) -> RealLabel {
        RealLabel::new(value)
    }

    fn real(value: u64) -> PathLabel {
        PathLabel::Real(label(value))
    }

    fn path(values: &[u64]) -> Path {
        values.iter().copied().map(real).collect()
    }

    fn labels(rows: &[&[u64]]) -> LabelMatrix {
        let n_rows = rows.len();
        let n_cols = rows.first().map_or(0, |row| row.len());
        let mut data = Vec::with_capacity(n_rows * n_cols);
        for row in rows {
            data.extend(row.iter().copied().map(label));
        }
        LabelMatrix::new(n_rows, n_cols, data)
    }

    fn labels_from_paths(paths: &[Path]) -> LabelMatrix {
        let n_rows = paths.len();
        let n_cols = paths.first().map_or(0, Vec::len);
        let mut data = Vec::with_capacity(n_rows * n_cols);
        for path in paths {
            for label in path {
                match label {
                    PathLabel::Real(value) => data.push(*value),
                    PathLabel::Augmented => {
                        panic!("test fixtures should only contain real labels");
                    }
                }
            }
        }
        LabelMatrix::new(n_rows, n_cols, data)
    }

    fn options(augment_path: bool, threads: usize) -> ReconcileOptions {
        ReconcileOptions {
            augment_path,
            threads,
            verbose: false,
        }
    }

    fn assert_reconciled_output(paths: &[Path], expected_rows: usize, expected_cols: usize) {
        assert_eq!(paths.len(), expected_rows);
        assert!(paths.iter().all(|path| path.len() == expected_cols));

        let (store, state) = assigned_state(paths);
        let edges = tree::construct_tree_from_ids(&store, state.assigned_path_ids());
        assert!(
            tree::get_bad_nodes(&edges).is_empty(),
            "reconciled output should not contain multi-parent nodes"
        );
    }

    fn edge(parent_layer: usize, parent: u64, child_layer: usize, child: u64) -> Edge {
        Edge {
            start: NodeId {
                layer: parent_layer,
                label: real(parent),
            },
            end: NodeId {
                layer: child_layer,
                label: real(child),
            },
        }
    }

    fn assigned_state(paths: &[Path]) -> (paths::PathStore, AssignedPathState) {
        let mut store = paths::PathStore::new();
        let assigned_path_ids = paths
            .iter()
            .cloned()
            .map(|path| store.intern(path))
            .collect::<Vec<_>>();
        let state = AssignedPathState::new(assigned_path_ids);
        (store, state)
    }

    fn prepared_round<'a>(
        candidates: &[Edge],
        feasible_ids: &[paths::PathId],
        state: &AssignedPathState,
        store: &'a paths::PathStore,
        labels: &LabelMatrix,
        thread_pool: Option<&ThreadPool>,
    ) -> PreparedRound<'a> {
        PreparedRound::new(candidates, feasible_ids, state, store, labels, thread_pool)
    }

    fn score_candidates(
        prepared_round: &PreparedRound<'_>,
        inputs: &ScoringInputs<'_>,
    ) -> Vec<Candidate> {
        prepared_round
            .jobs
            .iter()
            .enumerate()
            .map(|(order, job)| Candidate {
                edge: job.edge.clone(),
                cost: candidate_cost(job, prepared_round, inputs),
                order,
            })
            .collect()
    }

    fn brute_force_candidate_cost(
        edge: &Edge,
        feasible_path_ids: &[paths::PathId],
        assigned_state: &AssignedPathState,
        path_store: &paths::PathStore,
        labels: &LabelMatrix,
        sample_weights: &[f64],
    ) -> f64 {
        let pruned_paths = paths::prune_scoring_paths(feasible_path_ids, edge, path_store);
        let mut total_cost = 0.0;

        for (row, sample_weight) in sample_weights
            .iter()
            .copied()
            .enumerate()
            .take(assigned_state.len())
        {
            let current_path = path_store.get(assigned_state.path_id(row));
            if current_path[edge.end.layer] != edge.end.label {
                continue;
            }

            let row_labels = labels.row(row);
            let mut best_index = 0;
            let mut best_distance = usize::MAX;
            for (index, path) in pruned_paths.iter().enumerate() {
                let distance = paths::path_distance(row_labels, path.as_slice());
                if distance < best_distance {
                    best_distance = distance;
                    best_index = index;
                }
            }

            let new_path = pruned_paths[best_index].as_slice();
            total_cost += sample_weight * count_path_changes(current_path, new_path) as f64;
        }

        total_cost
    }

    fn reassign_affected_samples_by_full_scan(
        edge: &Edge,
        feasible_path_ids: &[paths::PathId],
        path_store: &paths::PathStore,
        labels: &LabelMatrix,
        assigned_state: &mut AssignedPathState,
    ) {
        reassign_rows(
            0..assigned_state.len(),
            edge,
            feasible_path_ids,
            path_store,
            labels,
            assigned_state,
        );
    }

    fn repeated_candidate_paths(rows_per_variant: usize, n_cols: usize) -> Vec<Path> {
        let variants = [(0_u64, 0_u64), (1, 1), (0, 2), (1, 3)];
        let mut paths = Vec::with_capacity(rows_per_variant * variants.len());
        for (parent, value) in variants {
            let mut path = vec![real(parent), real(1)];
            path.extend(std::iter::repeat_n(real(value), n_cols.saturating_sub(2)));
            for _ in 0..rows_per_variant {
                paths.push(path.clone());
            }
        }
        paths
    }

    #[test]
    fn run_rejects_mismatched_sample_weights() {
        let labels = labels(&[&[1, 1], &[2, 2]]);
        let error = run(&labels, &[1.0], options(false, 1))
            .expect_err("mismatched sample weights should fail");

        assert!(matches!(
            error,
            MrtreeError::SampleWeightsLengthMismatch {
                expected: 2,
                actual: 1
            }
        ));
    }

    #[test]
    fn collect_round_candidates_uses_bad_nodes_from_two_shallowest_layers() {
        let labels = labels_from_paths(&[
            path(&[1, 1, 1, 1]),
            path(&[2, 1, 2, 2]),
            path(&[3, 3, 2, 3]),
            path(&[4, 4, 4, 3]),
        ]);
        let state = ReconciliationState::new(&labels);

        let round = state
            .collect_round_candidates()
            .expect("conflicting input should produce a reconciliation round");

        assert_eq!(round.lowest_layers, vec![0, 1]);
        assert_eq!(
            round.candidates,
            vec![
                edge(0, 1, 1, 1),
                edge(0, 2, 1, 1),
                edge(1, 1, 2, 2),
                edge(1, 3, 2, 2),
            ]
        );
    }

    #[test]
    fn reassign_affected_samples_only_updates_conflicting_rows() {
        let labels = labels(&[&[1, 1], &[2, 1], &[2, 2]]);
        let assigned = vec![path(&[1, 1]), path(&[2, 1]), path(&[2, 2])];
        let (mut store, mut state) = assigned_state(&assigned);
        let pruned = [path(&[1, 1]), path(&[2, 2])];
        let pruned_ids = pruned
            .iter()
            .cloned()
            .map(|path| store.intern(path))
            .collect::<Vec<_>>();
        let candidate = edge(0, 1, 1, 1);
        let prepared = prepared_round(
            std::slice::from_ref(&candidate),
            state.assigned_path_ids(),
            &state,
            &store,
            &labels,
            None,
        );

        reassign_affected_samples(
            &candidate,
            prepared.rows_for_job(0),
            &pruned_ids,
            &store,
            &labels,
            &mut state,
        );

        assert_eq!(
            paths::materialize_paths(state.assigned_path_ids(), &store),
            vec![path(&[1, 1]), path(&[1, 1]), path(&[2, 2])]
        );
    }

    #[test]
    fn run_reconciles_conflicting_labels_without_changing_shape() {
        let labels = labels(&[&[1, 1, 1], &[1, 1, 2], &[2, 1, 3], &[2, 2, 4]]);
        let weights = vec![1.0; labels.n_rows()];

        let reconciled = run(&labels, &weights, options(false, 1))
            .expect("reconciliation should succeed on conflicting labels");

        assert_reconciled_output(&reconciled, labels.n_rows(), labels.n_cols());
    }

    #[test]
    fn run_matches_between_single_and_multi_threaded_scoring() {
        let input_paths = repeated_candidate_paths(16, 64);
        let labels = labels_from_paths(&input_paths);
        let weights = vec![1.0; labels.n_rows()];

        let single_threaded =
            run(&labels, &weights, options(false, 1)).expect("single-threaded run should work");
        let multi_threaded =
            run(&labels, &weights, options(false, 2)).expect("multi-threaded run should work");

        assert_eq!(multi_threaded, single_threaded);
        assert_reconciled_output(&multi_threaded, labels.n_rows(), labels.n_cols());
    }

    #[test]
    fn run_with_augment_path_serializes_surviving_augmented_labels_as_negative_one() {
        let labels = labels(&[&[1, 1, 1], &[1, 2, 1], &[2, 1, 2]]);
        let weights = vec![1.0; labels.n_rows()];

        let reconciled = run(&labels, &weights, options(true, 1))
            .expect("augment-path reconciliation should succeed");

        assert_eq!(
            reconciled,
            vec![
                path(&[1, 1, 1]),
                path(&[1, 1, 1]),
                vec![real(2), PathLabel::Augmented, real(2)],
            ]
        );

        let rendered = reconciled
            .iter()
            .map(|path| path.iter().map(ToString::to_string).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        assert_eq!(
            rendered,
            vec![
                vec!["1".to_owned(), "1".to_owned(), "1".to_owned()],
                vec!["1".to_owned(), "1".to_owned(), "1".to_owned()],
                vec!["2".to_owned(), "-1".to_owned(), "2".to_owned()],
            ]
        );
    }

    #[test]
    fn select_best_candidate_matches_between_serial_and_parallel_paths() {
        let assigned = repeated_candidate_paths(16, 64);
        let labels = labels_from_paths(&assigned);
        let (store, state) = assigned_state(&assigned);
        let feasible_ids = state.realized_path_ids_in_row_order();
        let candidates = vec![edge(0, 0, 1, 1); 10];
        let weights = vec![1.0; assigned.len()];
        let inputs = ScoringInputs {
            assigned_state: &state,
            path_store: &store,
            labels: &labels,
            sample_weights: &weights,
        };
        let serial_prepared =
            prepared_round(&candidates, &feasible_ids, &state, &store, &labels, None);
        let serial = select_best_candidate(
            &serial_prepared,
            &ThreadedScoringPlan::Serial,
            &inputs,
            None,
        );

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(2)
            .build()
            .expect("rayon thread pool should build");
        let parallel_prepared = prepared_round(
            &candidates,
            &feasible_ids,
            &state,
            &store,
            &labels,
            Some(&pool),
        );
        let parallel_plan =
            plan_threaded_scoring(&parallel_prepared, &labels, pool.current_num_threads());
        assert!(matches!(
            parallel_plan,
            ThreadedScoringPlan::ParallelChunked { .. }
        ));
        let parallel =
            select_best_candidate(&parallel_prepared, &parallel_plan, &inputs, Some(&pool));

        assert_eq!(serial, parallel);
    }

    #[test]
    fn prepared_round_scoring_matches_between_serial_and_parallel_construction() {
        let assigned = repeated_candidate_paths(32, 24);
        let labels = labels_from_paths(&assigned);
        let (store, state) = assigned_state(&assigned);
        let feasible_ids = state.realized_path_ids_in_row_order();
        let candidates = vec![
            edge(0, 0, 1, 1),
            edge(0, 1, 1, 1),
            edge(1, 1, 2, 0),
            edge(1, 1, 2, 2),
        ];

        let serial_prepared =
            prepared_round(&candidates, &feasible_ids, &state, &store, &labels, None);
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(4)
            .build()
            .expect("rayon thread pool should build");
        let parallel_prepared = prepared_round(
            &candidates,
            &feasible_ids,
            &state,
            &store,
            &labels,
            Some(&pool),
        );
        let weights = vec![1.0; assigned.len()];
        let inputs = ScoringInputs {
            assigned_state: &state,
            path_store: &store,
            labels: &labels,
            sample_weights: &weights,
        };
        let serial_scores = score_candidates(&serial_prepared, &inputs);
        let parallel_scores = score_candidates(&parallel_prepared, &inputs);

        assert_eq!(serial_scores, parallel_scores);
        assert_eq!(
            select_best_candidate(
                &serial_prepared,
                &ThreadedScoringPlan::Serial,
                &inputs,
                None
            ),
            select_best_candidate(
                &parallel_prepared,
                &ThreadedScoringPlan::Serial,
                &inputs,
                None
            )
        );
    }

    #[test]
    fn prepared_round_groups_only_rows_for_active_children() {
        let labels = labels(&[&[1, 1, 1], &[2, 1, 2], &[2, 2, 2], &[1, 2, 1]]);
        let assigned = vec![
            path(&[1, 1, 1]),
            path(&[2, 1, 2]),
            path(&[2, 2, 2]),
            path(&[1, 2, 1]),
        ];
        let (store, state) = assigned_state(&assigned);
        let feasible_ids = state.realized_path_ids_in_row_order();
        let candidates = vec![edge(0, 1, 1, 1), edge(1, 2, 2, 2)];
        let prepared = prepared_round(&candidates, &feasible_ids, &state, &store, &labels, None);

        assert_eq!(prepared.rows_for_job(0), &[0, 1]);
        assert_eq!(prepared.rows_for_job(1), &[1, 2]);
    }

    #[test]
    fn grouped_candidate_cost_matches_bruteforce_for_recombined_paths() {
        let labels = labels(&[&[1, 1, 1], &[1, 1, 2], &[2, 1, 3], &[2, 2, 4]]);
        let assigned = vec![
            path(&[1, 1, 1]),
            path(&[1, 1, 2]),
            path(&[2, 1, 3]),
            path(&[2, 2, 4]),
        ];
        let (store, state) = assigned_state(&assigned);
        let feasible_ids = state.realized_path_ids_in_row_order();
        let candidate = edge(0, 1, 1, 1);
        let weights = [1.0, 1.5, 2.0, 0.5];
        let cached_prepared = prepared_round(
            std::slice::from_ref(&candidate),
            &feasible_ids,
            &state,
            &store,
            &labels,
            None,
        );
        let mut uncached_prepared = prepared_round(
            std::slice::from_ref(&candidate),
            &feasible_ids,
            &state,
            &store,
            &labels,
            None,
        );
        for group in &mut uncached_prepared.child_groups {
            group.distance_table = None;
        }
        let inputs = ScoringInputs {
            assigned_state: &state,
            path_store: &store,
            labels: &labels,
            sample_weights: &weights,
        };
        let expected = brute_force_candidate_cost(
            &candidate,
            &feasible_ids,
            &state,
            &store,
            &labels,
            &weights,
        );

        assert_approx_eq!(
            f64,
            candidate_cost(&cached_prepared.jobs[0], &cached_prepared, &inputs),
            expected
        );
        assert_approx_eq!(
            f64,
            candidate_cost(&uncached_prepared.jobs[0], &uncached_prepared, &inputs),
            expected
        );
    }

    #[test]
    fn reassign_affected_samples_with_cached_rows_matches_full_scan() {
        let labels = labels(&[&[1, 1, 1], &[1, 1, 2], &[2, 1, 3], &[2, 2, 4]]);
        let assigned = vec![
            path(&[1, 1, 1]),
            path(&[1, 1, 2]),
            path(&[2, 1, 3]),
            path(&[2, 2, 4]),
        ];
        let (mut store, mut grouped_state) = assigned_state(&assigned);
        let (_, mut full_scan_state) = assigned_state(&assigned);
        let feasible_ids = grouped_state.realized_path_ids_in_row_order();
        let candidate = edge(0, 1, 1, 1);
        let pruned_ids = paths::prune_path_ids(&feasible_ids, &candidate, &mut store);
        let prepared = prepared_round(
            std::slice::from_ref(&candidate),
            &feasible_ids,
            &grouped_state,
            &store,
            &labels,
            None,
        );

        reassign_affected_samples(
            &candidate,
            prepared.rows_for_job(0),
            &pruned_ids,
            &store,
            &labels,
            &mut grouped_state,
        );
        reassign_affected_samples_by_full_scan(
            &candidate,
            &pruned_ids,
            &store,
            &labels,
            &mut full_scan_state,
        );

        assert_eq!(
            paths::materialize_paths(grouped_state.assigned_path_ids(), &store),
            paths::materialize_paths(full_scan_state.assigned_path_ids(), &store)
        );
    }

    #[test]
    fn plan_threaded_scoring_uses_grouped_rows_for_work_estimation() {
        let assigned = std::iter::repeat_n(path(&[0, 0, 0, 0]), 4094)
            .chain([path(&[1, 1, 1, 1]), path(&[2, 2, 2, 2])])
            .collect::<Vec<_>>();
        let labels = labels_from_paths(&assigned);
        let (store, state) = assigned_state(&assigned);
        let feasible_ids = state.realized_path_ids_in_row_order();
        let candidates = vec![edge(0, 1, 1, 1), edge(0, 2, 1, 2)];
        let prepared = prepared_round(&candidates, &feasible_ids, &state, &store, &labels, None);

        assert_eq!(prepared.rows_for_job(0).len(), 1);
        assert_eq!(prepared.rows_for_job(1).len(), 1);
        assert_eq!(
            plan_threaded_scoring(&prepared, &labels, 8),
            ThreadedScoringPlan::Serial
        );
    }

    #[test]
    fn chunked_parallel_scoring_matches_serial_for_single_candidate_round() {
        let assigned = repeated_candidate_paths(128, 64);
        let labels = labels_from_paths(&assigned);
        let (store, state) = assigned_state(&assigned);
        let feasible_ids = state.realized_path_ids_in_row_order();
        let candidates = vec![edge(0, 0, 1, 1)];
        let weights = vec![1.0; assigned.len()];
        let inputs = ScoringInputs {
            assigned_state: &state,
            path_store: &store,
            labels: &labels,
            sample_weights: &weights,
        };
        let serial_prepared =
            prepared_round(&candidates, &feasible_ids, &state, &store, &labels, None);
        let serial = select_best_candidate(
            &serial_prepared,
            &ThreadedScoringPlan::Serial,
            &inputs,
            None,
        );

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(4)
            .build()
            .expect("rayon thread pool should build");
        let parallel_prepared = prepared_round(
            &candidates,
            &feasible_ids,
            &state,
            &store,
            &labels,
            Some(&pool),
        );
        let parallel_plan =
            plan_threaded_scoring(&parallel_prepared, &labels, pool.current_num_threads());
        assert!(matches!(
            parallel_plan,
            ThreadedScoringPlan::ParallelChunked { .. }
        ));
        let parallel =
            select_best_candidate(&parallel_prepared, &parallel_plan, &inputs, Some(&pool));

        assert_eq!(serial, parallel);
    }

    #[test]
    fn assigned_path_state_assign_row_matches_recomputed_order_across_sequence() {
        let input = vec![path(&[1, 1]), path(&[2, 2]), path(&[1, 1]), path(&[3, 3])];
        let (mut store, mut state) = assigned_state(&input);
        let path_c = store.intern(path(&[3, 3]));
        let path_b = store.intern(path(&[2, 2]));
        let path_a = store.intern(path(&[1, 1]));

        state.assign_row(0, path_c);
        assert_eq!(
            state.realized_path_ids_in_row_order(),
            paths::unique_path_ids(state.assigned_path_ids().iter().copied())
        );

        state.assign_row(1, path_a);
        assert_eq!(
            state.realized_path_ids_in_row_order(),
            paths::unique_path_ids(state.assigned_path_ids().iter().copied())
        );

        state.assign_row(3, path_b);
        assert_eq!(
            state.realized_path_ids_in_row_order(),
            paths::unique_path_ids(state.assigned_path_ids().iter().copied())
        );
    }
}
