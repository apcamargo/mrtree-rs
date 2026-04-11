use rayon::ThreadPool;
use rayon::prelude::*;

use super::paths::{PathId, PathStore};
use crate::model::{Edge, NodeId, PathLabel};

const MIN_EDGE_PAIRS_PER_TASK: usize = 16_384;

pub(super) fn collect_bad_edges_from_ids(
    path_store: &PathStore,
    path_ids: &[PathId],
    thread_pool: Option<&ThreadPool>,
) -> Vec<Edge> {
    if path_ids.is_empty() {
        return Vec::new();
    }

    let n_layers = path_store.get(path_ids[0]).len();
    let layer_pairs = thread_pool.map_or_else(
        || collect_layer_pairs_serial(path_store, path_ids, n_layers),
        |pool| collect_layer_pairs_parallel(path_store, path_ids, n_layers, pool),
    );

    collect_bad_edges_from_layer_pairs(layer_pairs)
}

type LayerPairs = Vec<Vec<(PathLabel, PathLabel)>>;

fn append_path_pairs(layer_pairs: &mut LayerPairs, path: &[PathLabel]) {
    for layer in 0..layer_pairs.len() {
        layer_pairs[layer].push((path[layer + 1], path[layer]));
    }
}

fn collect_layer_pairs_serial(
    path_store: &PathStore,
    path_ids: &[PathId],
    n_layers: usize,
) -> LayerPairs {
    let mut layer_pairs = (0..n_layers.saturating_sub(1))
        .map(|_| Vec::with_capacity(path_ids.len()))
        .collect::<LayerPairs>();

    for &path_id in path_ids {
        append_path_pairs(&mut layer_pairs, path_store.get(path_id).as_slice());
    }

    layer_pairs
}

fn collect_layer_pairs_parallel(
    path_store: &PathStore,
    path_ids: &[PathId],
    n_layers: usize,
    thread_pool: &ThreadPool,
) -> LayerPairs {
    let n_threads = thread_pool.current_num_threads();
    let estimated_edge_pairs = path_ids.len().saturating_mul(n_layers.saturating_sub(1));
    if n_threads <= 1 || estimated_edge_pairs < n_threads.saturating_mul(MIN_EDGE_PAIRS_PER_TASK) {
        return collect_layer_pairs_serial(path_store, path_ids, n_layers);
    }

    let min_paths_per_split = (MIN_EDGE_PAIRS_PER_TASK / n_layers.saturating_sub(1).max(1)).max(1);
    let make_layer_pairs = || {
        (0..n_layers.saturating_sub(1))
            .map(|_| Vec::new())
            .collect::<LayerPairs>()
    };
    thread_pool.install(|| {
        path_ids
            .par_iter()
            .copied()
            .with_min_len(min_paths_per_split)
            .fold(make_layer_pairs, |mut layer_pairs, path_id| {
                append_path_pairs(&mut layer_pairs, path_store.get(path_id).as_slice());
                layer_pairs
            })
            .reduce(make_layer_pairs, |mut left, right| {
                for (left_layer, right_layer) in left.iter_mut().zip(right) {
                    left_layer.extend(right_layer);
                }
                left
            })
    })
}

fn collect_bad_edges_from_layer_pairs(layer_pairs: LayerPairs) -> Vec<Edge> {
    let mut edges = Vec::new();

    for (layer, mut pairs) in layer_pairs.into_iter().enumerate() {
        if pairs.is_empty() {
            continue;
        }

        pairs.sort_unstable();
        pairs.dedup();

        let mut start = 0;
        while start < pairs.len() {
            let child = pairs[start].0;
            let mut end = start + 1;
            while end < pairs.len() && pairs[end].0 == child {
                end += 1;
            }

            if end - start > 1 {
                for &(child, parent) in &pairs[start..end] {
                    edges.push(Edge {
                        start: NodeId {
                            layer,
                            label: parent,
                        },
                        end: NodeId {
                            layer: layer + 1,
                            label: child,
                        },
                    });
                }
            }

            start = end;
        }
    }

    edges
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Path, RealLabel};

    fn real(value: u64) -> PathLabel {
        PathLabel::Real(RealLabel::new(value))
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

    #[test]
    fn collect_bad_edges_from_ids_returns_edges_for_children_with_multiple_parents() {
        let input = vec![
            vec![real(1), real(1)],
            vec![real(2), real(1)],
            vec![real(2), real(2)],
        ];
        let (store, path_ids) = store_from_paths(&input);

        assert_eq!(
            collect_bad_edges_from_ids(&store, &path_ids, None),
            vec![edge(0, 1, 1, 1), edge(0, 2, 1, 1)]
        );
    }

    #[test]
    fn collect_bad_edges_from_ids_orders_edges_by_layer_then_child_then_parent() {
        let input = vec![
            vec![real(2), real(2), real(1)],
            vec![real(1), real(2), real(2)],
            vec![real(1), real(1), real(2)],
            vec![real(2), real(1), real(1)],
        ];
        let (store, path_ids) = store_from_paths(&input);

        assert_eq!(
            collect_bad_edges_from_ids(&store, &path_ids, None),
            vec![
                edge(0, 1, 1, 1),
                edge(0, 2, 1, 1),
                edge(0, 1, 1, 2),
                edge(0, 2, 1, 2),
                edge(1, 1, 2, 1),
                edge(1, 2, 2, 1),
                edge(1, 1, 2, 2),
                edge(1, 2, 2, 2),
            ]
        );
    }

    #[test]
    fn collect_bad_edges_from_ids_ignores_duplicate_edges_from_same_parent() {
        let input = vec![vec![real(1), real(2)], vec![real(1), real(2)]];
        let (store, path_ids) = store_from_paths(&input);

        assert!(collect_bad_edges_from_ids(&store, &path_ids, None).is_empty());
    }
}
