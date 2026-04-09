use std::collections::{BTreeSet, HashMap, HashSet};

use super::paths::{PathId, PathStore};
use crate::model::{Edge, NodeId, PathLabel};

pub(super) fn construct_tree_from_ids(path_store: &PathStore, path_ids: &[PathId]) -> Vec<Edge> {
    if path_ids.is_empty() {
        return Vec::new();
    }

    let n_layers = path_store.get(path_ids[0]).len();
    let mut edges = Vec::with_capacity(path_ids.len().saturating_mul(n_layers.saturating_sub(1)));

    for layer in 0..(n_layers - 1) {
        extend_layer_edges(
            &mut edges,
            path_ids
                .iter()
                .map(|&path_id| path_store.get(path_id).as_slice()),
            layer,
            path_ids.len(),
        );
    }

    edges
}

pub(super) fn get_bad_nodes(edges: &[Edge]) -> BTreeSet<NodeId> {
    let mut first_parent_by_child = HashMap::<NodeId, NodeId>::with_capacity(edges.len());
    let mut bad_children = HashSet::<NodeId>::with_capacity(edges.len());

    for edge in edges {
        match first_parent_by_child.get(&edge.end) {
            Some(parent) if parent != &edge.start => {
                bad_children.insert(edge.end.clone());
            }
            Some(_) => {}
            None => {
                first_parent_by_child.insert(edge.end.clone(), edge.start.clone());
            }
        }
    }

    bad_children.into_iter().collect()
}

fn extend_layer_edges<'a, I>(edges: &mut Vec<Edge>, paths: I, layer: usize, path_count: usize)
where
    I: IntoIterator<Item = &'a [PathLabel]>,
{
    let mut unique_edges = HashSet::<(PathLabel, PathLabel)>::with_capacity(path_count);

    for path in paths {
        let parent = path[layer];
        let child = path[layer + 1];
        unique_edges.insert((child, parent));
    }

    let mut layer_edges = unique_edges.into_iter().collect::<Vec<_>>();
    layer_edges.sort_unstable_by_key(|(child, parent)| (*child, *parent));

    edges.reserve(layer_edges.len());
    for (child, parent) in layer_edges {
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

    #[test]
    fn get_bad_nodes_returns_children_with_multiple_parents() {
        let input = vec![
            vec![real(1), real(1)],
            vec![real(2), real(1)],
            vec![real(2), real(2)],
        ];
        let (store, path_ids) = store_from_paths(&input);
        let edges = construct_tree_from_ids(&store, &path_ids);

        assert_eq!(
            get_bad_nodes(&edges),
            BTreeSet::from([NodeId {
                layer: 1,
                label: real(1),
            }])
        );
    }

    #[test]
    fn construct_tree_orders_edges_by_layer_then_child_then_parent() {
        let input = vec![
            vec![real(2), real(2), real(1)],
            vec![real(1), real(2), real(2)],
            vec![real(1), real(1), real(2)],
            vec![real(2), real(1), real(1)],
        ];
        let (store, path_ids) = store_from_paths(&input);
        let edges = construct_tree_from_ids(&store, &path_ids);

        let edge_order = edges
            .iter()
            .map(|edge| (edge.start.layer, edge.start.label, edge.end.label))
            .collect::<Vec<_>>();

        assert_eq!(
            edge_order,
            vec![
                (0, real(1), real(1)),
                (0, real(2), real(1)),
                (0, real(1), real(2)),
                (0, real(2), real(2)),
                (1, real(1), real(1)),
                (1, real(2), real(1)),
                (1, real(1), real(2)),
                (1, real(2), real(2)),
            ]
        );
    }

    #[test]
    fn get_bad_nodes_ignores_duplicate_edges_from_same_parent() {
        let parent = NodeId {
            layer: 0,
            label: real(1),
        };
        let child = NodeId {
            layer: 1,
            label: real(2),
        };

        let edges = vec![
            Edge {
                start: parent.clone(),
                end: child.clone(),
            },
            Edge {
                start: parent,
                end: child,
            },
        ];

        assert!(get_bad_nodes(&edges).is_empty());
    }
}
