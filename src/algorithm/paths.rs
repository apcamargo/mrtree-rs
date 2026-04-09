use std::hash::{Hash, Hasher};

use indexmap::IndexSet;

use crate::model::{Edge, Path, PathLabel, RealLabel};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct PathId(usize);

#[derive(Debug, Default)]
pub(super) struct PathStore {
    paths: IndexSet<Path>,
}

impl PathStore {
    pub(super) fn new() -> Self {
        Self::default()
    }

    pub(super) fn intern(&mut self, path: Path) -> PathId {
        let (index, _) = self.paths.insert_full(path);
        PathId(index)
    }

    pub(super) fn get(&self, path_id: PathId) -> &Path {
        self.paths
            .get_index(path_id.0)
            .expect("path ids should always refer to interned paths")
    }
}

fn unique_items<T>(items: impl IntoIterator<Item = T>) -> Vec<T>
where
    T: Eq + std::hash::Hash,
{
    let mut set = IndexSet::new();
    set.extend(items);
    set.into_iter().collect()
}

fn unique_paths(paths: impl IntoIterator<Item = Path>) -> Vec<Path> {
    unique_items(paths)
}

pub(super) fn unique_path_ids(path_ids: impl IntoIterator<Item = PathId>) -> Vec<PathId> {
    unique_items(path_ids)
}

#[derive(Debug, Clone)]
pub(super) enum ScoringPathView<'a> {
    Feasible {
        feasible_index: usize,
        path: &'a [PathLabel],
    },
    Owned(Path),
}

impl ScoringPathView<'_> {
    pub(super) fn as_slice(&self) -> &[PathLabel] {
        match self {
            Self::Feasible { path, .. } => path,
            Self::Owned(path) => path.as_slice(),
        }
    }
}

impl PartialEq for ScoringPathView<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Eq for ScoringPathView<'_> {}

impl Hash for ScoringPathView<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_slice().hash(state);
    }
}

fn unique_scoring_path_views<'a>(
    paths: impl IntoIterator<Item = ScoringPathView<'a>>,
) -> Vec<ScoringPathView<'a>> {
    unique_items(paths)
}

pub(super) fn materialize_paths(path_ids: &[PathId], path_store: &PathStore) -> Vec<Path> {
    path_ids
        .iter()
        .copied()
        .map(|path_id| path_store.get(path_id).clone())
        .collect()
}

fn path_dot_product(paths1: &[Path], paths2: &[Path]) -> Vec<Path> {
    let unique1 = unique_paths(paths1.iter().cloned());
    let unique2 = unique_paths(paths2.iter().cloned());
    if unique1.is_empty() || unique2.is_empty() {
        return Vec::new();
    }

    let mut product = Vec::with_capacity(unique1.len() * unique2.len());
    for suffix in &unique2 {
        for prefix in &unique1 {
            let mut combined = Vec::with_capacity(prefix.len() + suffix.len());
            combined.extend_from_slice(prefix);
            combined.extend_from_slice(suffix);
            product.push(combined);
        }
    }
    product
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EdgePathRelation {
    Selected,
    Conflicting,
    Unaffected,
}

fn classify_path_for_edge(path: &[PathLabel], edge: &Edge) -> EdgePathRelation {
    let parent_layer = edge.start.layer;
    let child_layer = edge.end.layer;
    let parent_label = edge.start.label;
    let child_label = edge.end.label;

    if path[parent_layer] == parent_label && path[child_layer] == child_label {
        EdgePathRelation::Selected
    } else if path[parent_layer] != parent_label && path[child_layer] == child_label {
        EdgePathRelation::Conflicting
    } else {
        EdgePathRelation::Unaffected
    }
}

fn recombined_paths<T, U>(
    selected: &[T],
    conflicting: &[U],
    child_layer: usize,
    path_len: usize,
) -> Vec<Path>
where
    T: AsRef<[PathLabel]>,
    U: AsRef<[PathLabel]>,
{
    if conflicting.is_empty() || child_layer + 1 >= path_len {
        return Vec::new();
    }

    let prefixes = selected
        .iter()
        .map(|item| item.as_ref()[..=child_layer].to_vec())
        .collect::<Vec<_>>();
    let suffixes = conflicting
        .iter()
        .map(|item| item.as_ref()[(child_layer + 1)..].to_vec())
        .collect::<Vec<_>>();
    path_dot_product(&prefixes, &suffixes)
}

fn build_augmented_paths<T>(paths: &[T], layer: usize, n_layers: usize) -> Vec<Path>
where
    T: AsRef<[PathLabel]>,
{
    if layer + 1 == n_layers
        || paths
            .iter()
            .any(|item| item.as_ref()[layer] == PathLabel::Augmented)
    {
        return Vec::new();
    }

    if layer == 0 {
        let suffixes = paths
            .iter()
            .filter(|item| !item.as_ref()[1..].contains(&PathLabel::Augmented))
            .map(|item| item.as_ref()[1..].to_vec())
            .collect::<Vec<_>>();
        let prefixes = vec![vec![PathLabel::Augmented]];
        path_dot_product(&prefixes, &suffixes)
    } else {
        let prefixes = paths
            .iter()
            .filter(|item| !item.as_ref()[..layer].contains(&PathLabel::Augmented))
            .map(|item| {
                let mut prefix = item.as_ref()[..layer].to_vec();
                prefix.push(PathLabel::Augmented);
                prefix
            })
            .collect::<Vec<_>>();
        let suffixes = paths
            .iter()
            .filter(|item| !item.as_ref()[(layer + 1)..].contains(&PathLabel::Augmented))
            .map(|item| item.as_ref()[(layer + 1)..].to_vec())
            .collect::<Vec<_>>();
        path_dot_product(&prefixes, &suffixes)
    }
}

#[derive(Debug)]
struct PartitionedPaths {
    path_len: usize,
    kept_path_ids: Vec<PathId>,
    kept_feasible_paths: Vec<(usize, PathId)>,
    selected_path_ids: Vec<PathId>,
    conflicting_path_ids: Vec<PathId>,
}

fn partition_paths(path_ids: &[PathId], edge: &Edge, path_store: &PathStore) -> PartitionedPaths {
    let path_len = path_store.get(path_ids[0]).len();
    let mut kept_path_ids = Vec::new();
    let mut kept_feasible_paths = Vec::new();
    let mut selected_path_ids = Vec::new();
    let mut conflicting_path_ids = Vec::new();
    let mut keep_path = |feasible_index: usize, path_id| {
        kept_path_ids.push(path_id);
        kept_feasible_paths.push((feasible_index, path_id));
    };

    for (feasible_index, &path_id) in path_ids.iter().enumerate() {
        let path = path_store.get(path_id);
        match classify_path_for_edge(path, edge) {
            EdgePathRelation::Selected => {
                keep_path(feasible_index, path_id);
                selected_path_ids.push(path_id);
            }
            EdgePathRelation::Conflicting => conflicting_path_ids.push(path_id),
            EdgePathRelation::Unaffected => keep_path(feasible_index, path_id),
        }
    }

    PartitionedPaths {
        path_len,
        kept_path_ids,
        kept_feasible_paths,
        selected_path_ids,
        conflicting_path_ids,
    }
}

fn recombined_partition_paths(
    partition: &PartitionedPaths,
    edge: &Edge,
    path_store: &PathStore,
) -> Vec<Path> {
    let selected_paths = partition
        .selected_path_ids
        .iter()
        .map(|&path_id| path_store.get(path_id).as_slice())
        .collect::<Vec<_>>();
    let conflicting_paths = partition
        .conflicting_path_ids
        .iter()
        .map(|&path_id| path_store.get(path_id).as_slice())
        .collect::<Vec<_>>();

    recombined_paths(
        &selected_paths,
        &conflicting_paths,
        edge.end.layer,
        partition.path_len,
    )
}

pub(super) fn prune_path_ids(
    path_ids: &[PathId],
    edge: &Edge,
    path_store: &mut PathStore,
) -> Vec<PathId> {
    if path_ids.is_empty() {
        return Vec::new();
    }

    let partition = partition_paths(path_ids, edge, path_store);
    let recombined = recombined_partition_paths(&partition, edge, path_store);
    let mut kept_path_ids = partition.kept_path_ids;
    kept_path_ids.extend(recombined.into_iter().map(|path| path_store.intern(path)));

    unique_path_ids(kept_path_ids)
}

pub(super) fn prune_scoring_paths<'a>(
    path_ids: &[PathId],
    edge: &Edge,
    path_store: &'a PathStore,
) -> Vec<ScoringPathView<'a>> {
    if path_ids.is_empty() {
        return Vec::new();
    }

    let partition = partition_paths(path_ids, edge, path_store);
    let mut kept = partition
        .kept_feasible_paths
        .iter()
        .map(|&(feasible_index, path_id)| ScoringPathView::Feasible {
            feasible_index,
            path: path_store.get(path_id).as_slice(),
        })
        .collect::<Vec<_>>();
    kept.extend(
        recombined_partition_paths(&partition, edge, path_store)
            .into_iter()
            .map(ScoringPathView::Owned),
    );

    unique_scoring_path_views(kept)
}

pub(super) fn augment_path_ids(
    path_ids: &[PathId],
    layers: &[usize],
    path_store: &mut PathStore,
) -> Vec<PathId> {
    if path_ids.is_empty() {
        return Vec::new();
    }

    let mut augmented = path_ids.to_vec();
    let n_layers = path_store.get(path_ids[0]).len();

    for &layer in layers {
        let new_paths = {
            let augmented_paths = augmented
                .iter()
                .map(|&path_id| path_store.get(path_id))
                .collect::<Vec<_>>();
            build_augmented_paths(&augmented_paths, layer, n_layers)
        };
        augmented.extend(new_paths.into_iter().map(|path| path_store.intern(path)));
    }

    unique_path_ids(augmented)
}

pub(super) fn path_distance(row: &[RealLabel], path: &[PathLabel]) -> usize {
    row.iter()
        .zip(path.iter())
        .filter(
            |(label, path_label)| !matches!(path_label, PathLabel::Real(value) if value == *label),
        )
        .count()
}

fn assign_row_to_path_impl<'a>(
    row: &[RealLabel],
    paths: impl IntoIterator<Item = &'a [PathLabel]>,
) -> usize {
    let mut best_index = 0;
    let mut best_distance = usize::MAX;

    for (index, path) in paths.into_iter().enumerate() {
        let distance = path_distance(row, path);
        if distance < best_distance {
            best_distance = distance;
            best_index = index;
        }
    }

    best_index
}

pub(super) fn assign_row_to_path_id(
    row: &[RealLabel],
    path_ids: &[PathId],
    path_store: &PathStore,
) -> usize {
    assign_row_to_path_impl(
        row,
        path_ids
            .iter()
            .copied()
            .map(|path_id| path_store.get(path_id).as_slice()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::NodeId;

    fn real(value: u64) -> PathLabel {
        PathLabel::Real(RealLabel::new(value))
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

    fn store_from_paths(paths: &[Path]) -> (PathStore, Vec<PathId>) {
        let mut store = PathStore::new();
        let path_ids = paths
            .iter()
            .cloned()
            .map(|path| store.intern(path))
            .collect::<Vec<_>>();
        (store, path_ids)
    }

    #[test]
    fn unique_path_ids_preserves_first_occurrence_order() {
        let paths = vec![
            vec![real(1), real(1)],
            vec![real(2), real(2)],
            vec![real(1), real(1)],
            vec![real(3), real(3)],
        ];
        let (store, path_ids) = store_from_paths(&paths);

        assert_eq!(
            materialize_paths(&unique_path_ids(path_ids), &store),
            vec![
                vec![real(1), real(1)],
                vec![real(2), real(2)],
                vec![real(3), real(3)],
            ]
        );
    }

    #[test]
    fn path_dot_product_deduplicates_and_orders_by_suffix_then_prefix() {
        let product = path_dot_product(
            &[vec![real(1)], vec![real(2)], vec![real(1)]],
            &[vec![real(9)], vec![real(8)], vec![real(9)]],
        );

        assert_eq!(
            product,
            vec![
                vec![real(1), real(9)],
                vec![real(2), real(9)],
                vec![real(1), real(8)],
                vec![real(2), real(8)],
            ]
        );
    }

    #[test]
    fn prune_path_ids_removes_conflicts_without_recombining_on_last_layer() {
        let input = vec![
            vec![real(1), real(1)],
            vec![real(2), real(1)],
            vec![real(2), real(2)],
        ];
        let candidate = edge(0, 1, 1, 1);
        let (mut store, path_ids) = store_from_paths(&input);
        let pruned = materialize_paths(&prune_path_ids(&path_ids, &candidate, &mut store), &store);

        assert_eq!(pruned, vec![vec![real(1), real(1)], vec![real(2), real(2)]]);
    }

    #[test]
    fn prune_path_ids_recombines_selected_prefixes_with_conflicting_suffixes() {
        let input = vec![
            vec![real(1), real(1), real(1)],
            vec![real(1), real(1), real(2)],
            vec![real(2), real(1), real(3)],
            vec![real(2), real(2), real(4)],
        ];
        let candidate = edge(0, 1, 1, 1);
        let (mut store, path_ids) = store_from_paths(&input);
        let pruned = materialize_paths(&prune_path_ids(&path_ids, &candidate, &mut store), &store);

        assert_eq!(
            pruned,
            vec![
                vec![real(1), real(1), real(1)],
                vec![real(1), real(1), real(2)],
                vec![real(2), real(2), real(4)],
                vec![real(1), real(1), real(3)],
            ]
        );
    }

    #[test]
    fn prune_scoring_paths_preserves_feasible_order_and_appends_owned_recombinations() {
        let input = vec![
            vec![real(1), real(1), real(1)],
            vec![real(1), real(1), real(2)],
            vec![real(2), real(1), real(3)],
            vec![real(2), real(2), real(4)],
        ];
        let candidate = edge(0, 1, 1, 1);
        let (store, path_ids) = store_from_paths(&input);

        let pruned = prune_scoring_paths(&path_ids, &candidate, &store);

        assert_eq!(pruned.len(), 4);
        assert!(matches!(
            pruned[0],
            ScoringPathView::Feasible {
                feasible_index: 0,
                ..
            }
        ));
        assert!(matches!(
            pruned[1],
            ScoringPathView::Feasible {
                feasible_index: 1,
                ..
            }
        ));
        assert!(matches!(
            pruned[2],
            ScoringPathView::Feasible {
                feasible_index: 3,
                ..
            }
        ));
        assert!(matches!(pruned[3], ScoringPathView::Owned(_)));
        assert_eq!(
            pruned
                .iter()
                .map(ScoringPathView::as_slice)
                .collect::<Vec<_>>(),
            vec![
                &[real(1), real(1), real(1)][..],
                &[real(1), real(1), real(2)][..],
                &[real(2), real(2), real(4)][..],
                &[real(1), real(1), real(3)][..],
            ]
        );
    }

    #[test]
    fn augment_path_ids_handles_first_middle_and_last_layers() {
        let base = vec![
            vec![real(1), real(1), real(1)],
            vec![real(2), real(2), real(2)],
        ];
        let (mut first_store, first_ids) = store_from_paths(&base);
        let first_augmented = materialize_paths(
            &augment_path_ids(&first_ids, &[0], &mut first_store),
            &first_store,
        );
        let (mut middle_store, middle_ids) = store_from_paths(&base);
        let middle_augmented = materialize_paths(
            &augment_path_ids(&middle_ids, &[1], &mut middle_store),
            &middle_store,
        );
        let (mut last_store, last_ids) = store_from_paths(&base);
        let last_unchanged = materialize_paths(
            &augment_path_ids(&last_ids, &[2], &mut last_store),
            &last_store,
        );

        assert!(first_augmented.contains(&vec![PathLabel::Augmented, real(1), real(1)]));
        assert!(first_augmented.contains(&vec![PathLabel::Augmented, real(2), real(2)]));

        assert!(middle_augmented.contains(&vec![real(1), PathLabel::Augmented, real(1)]));
        assert!(middle_augmented.contains(&vec![real(2), PathLabel::Augmented, real(2)]));

        assert_eq!(last_unchanged, base);
    }

    #[test]
    fn assign_row_to_path_id_uses_first_minimum_for_ties() {
        let paths = vec![vec![real(1), real(3)], vec![real(3), real(2)]];
        let (store, path_ids) = store_from_paths(&paths);

        assert_eq!(
            assign_row_to_path_id(&[RealLabel::new(1), RealLabel::new(2)], &path_ids, &store),
            0
        );
    }
}
