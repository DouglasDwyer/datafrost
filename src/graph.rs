use bitvec::prelude::*;
use slab::*;
use std::ops::*;

/// The index of a node in a [`DirectedAcyclicGraph`].
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct NodeId(u16);

/// Used to represent a graph of objects of type `T`
/// without any cycles.
#[derive(Clone, Debug)]
pub struct DirectedAcyclicGraph<T> {
    /// The list of nodes in the graph.
    nodes: Slab<NodeEntry<T>>,
    /// Holds linked lists of parents and children within the graph.
    relatives: Slab<NodeListEntry>,
}

impl<T> DirectedAcyclicGraph<T> {
    /// Creates a new, initially empty graph.
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    /// Creates a new, empty graph which can store `capacity` nodes.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            nodes: Slab::with_capacity(capacity),
            relatives: Slab::with_capacity(capacity * (capacity + 1) / 2),
        }
    }

    /// Gets the ID of the next node that will be allocated when calling `insert`.
    pub fn vacant_node(&self) -> NodeId {
        NodeId(self.nodes.vacant_key() as u16)
    }

    /// Inserts the provided node into the graph, with the given parents.
    pub fn insert(&mut self, value: T, parents: &[NodeId]) -> NodeId {
        let node = self.vacant_node();

        let mut first_parent = u16::MAX;
        for &parent in parents {
            self.add_child_to_parent(parent, node);

            first_parent = self.relatives.insert(NodeListEntry {
                node: parent,
                next_entry: first_parent,
            }) as u16;
        }

        self.nodes.insert(NodeEntry {
            value,
            first_child_entry: u16::MAX,
            first_parent_entry: first_parent,
        });

        node
    }

    /// Determines whether the graph is empty.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Removes a node from the graph. The node is assumed to have no parents.
    pub fn pop(&mut self, node: NodeId) -> T {
        let result = self.nodes.remove(node.0 as usize);
        assert!(
            result.first_parent_entry == u16::MAX,
            "Cannot pop node that has dependencies."
        );

        let mut current_entry = result.first_child_entry;
        while current_entry != u16::MAX {
            let relative = self.relatives[current_entry as usize];
            self.relatives.remove(current_entry as usize);
            self.remove_parent_from_child(node, relative.node);
            current_entry = relative.next_entry;
        }

        result.value
    }

    /// Gets the children of the given node.
    pub fn children(&self, node: NodeId) -> impl '_ + Iterator<Item = NodeId> {
        DirectedAcyclicGraphIter {
            graph: self,
            current_entry: self.nodes[node.0 as usize].first_child_entry,
        }
    }

    /// Gets the parents of the given node.
    pub fn parents(&self, node: NodeId) -> impl '_ + Iterator<Item = NodeId> {
        DirectedAcyclicGraphIter {
            graph: self,
            current_entry: self.nodes[node.0 as usize].first_parent_entry,
        }
    }

    /// Adds a parent to the provided node. It is a logical error for
    /// this operation to create a cycle.
    pub fn add_parent(&mut self, parent: NodeId, node: NodeId) {
        self.add_child_to_parent(parent, node);
        let node = &mut self.nodes[node.0 as usize];
        let new_entry = self.relatives.insert(NodeListEntry {
            node: parent,
            next_entry: node.first_parent_entry,
        });
        node.first_parent_entry = new_entry as u16;
    }

    /// Adds the provided child to the front of the parent's child list.
    fn add_child_to_parent(&mut self, parent: NodeId, node: NodeId) {
        let parent_node = &mut self.nodes[parent.0 as usize];
        let new_entry = self.relatives.insert(NodeListEntry {
            node,
            next_entry: parent_node.first_child_entry,
        });
        parent_node.first_child_entry = new_entry as u16;
    }

    /// Removes the provided parent from the child's parent list.
    fn remove_parent_from_child(&mut self, parent: NodeId, node: NodeId) {
        let mut last_entry = &mut self.nodes[node.0 as usize].first_parent_entry;
        let mut next_entry = self.relatives[*last_entry as usize];

        while next_entry.node != parent {
            let key = *last_entry;
            let (current, next) = self
                .relatives
                .get2_mut(key as usize, next_entry.next_entry as usize)
                .expect("Failed to get relative nodes.");
            next_entry = *next;
            last_entry = &mut current.next_entry;
        }

        let to_remove = *last_entry;
        *last_entry = next_entry.next_entry;
        self.relatives.remove(to_remove as usize);
    }
}

impl<T> Default for DirectedAcyclicGraph<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Index<NodeId> for DirectedAcyclicGraph<T> {
    type Output = T;

    fn index(&self, index: NodeId) -> &Self::Output {
        &self.nodes[index.0 as usize].value
    }
}

impl<T> IndexMut<NodeId> for DirectedAcyclicGraph<T> {
    fn index_mut(&mut self, index: NodeId) -> &mut Self::Output {
        &mut self.nodes[index.0 as usize].value
    }
}

/// Allows for iterating over the nodes of a graph.
pub struct DirectedAcyclicGraphIter<'a, T> {
    /// The graph to iterate.
    graph: &'a DirectedAcyclicGraph<T>,
    /// The index of the current entry in a linked list of entries.
    current_entry: u16,
}

impl<'a, T> Iterator for DirectedAcyclicGraphIter<'a, T> {
    type Item = NodeId;

    fn next(&mut self) -> Option<Self::Item> {
        (self.current_entry != u16::MAX).then(|| {
            let value = self.graph.relatives[self.current_entry as usize];
            self.current_entry = value.next_entry;
            value.node
        })
    }
}

/// Stores a set of flags - one for each node of a [`DirectedAcyclicGraph`].
#[derive(Debug, Default)]
pub struct DirectedAcyclicGraphFlags {
    /// The inner set of flas.
    flags: BitVec,
}

impl DirectedAcyclicGraphFlags {
    /// Creates a new set of graph flags.
    pub fn new() -> Self {
        Self {
            flags: BitVec::new(),
        }
    }

    /// Resizes to hold data about all of the nodes in the provided graph.
    /// Any newly-created nodes are assigned the value of `false`.
    pub fn resize_for<T>(&mut self, graph: &DirectedAcyclicGraph<T>) {
        self.flags
            .resize(self.flags.len().max(graph.nodes.capacity()), false);
    }

    /// Sets a flag on the provided node.
    pub fn set(&mut self, node: NodeId, value: bool) -> bool {
        self.flags.replace(node.0 as usize, value)
    }

    /// Gets the flag associated with the provided node.
    pub fn get(&self, node: NodeId) -> bool {
        self.flags[node.0 as usize]
    }

    /// Gets the first node in this set, if any.
    pub fn first_set_node(&self) -> Option<NodeId> {
        self.flags.first_one().map(|x| NodeId(x as u16))
    }

    /// Sets the contents of this flags list as the logical
    /// "and" of the two input sets.
    pub fn and(&mut self, a: &Self, b: &Self) {
        self.flags.clear();
        self.flags.extend_from_bitslice(&a.flags);
        self.flags &= &b.flags;
    }
}

/// Represents a node in a graph.
#[derive(Copy, Clone, Debug)]
struct NodeEntry<T> {
    /// The value of this node.
    pub value: T,
    /// The ID of the first child in the relatives list, if any.
    pub first_child_entry: u16,
    /// The ID of the first parent in the relatives list, if any.
    pub first_parent_entry: u16,
}

/// Represents an entry in a linked list of nodes.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
struct NodeListEntry {
    /// The ID of the node.
    pub node: NodeId,
    /// The next entry in the list.
    pub next_entry: u16,
}
