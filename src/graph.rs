use bitvec::prelude::*;
use slab::*;
use std::ops::*;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct NodeId(u16);

#[derive(Clone, Debug)]
pub struct DirectedAcyclicGraph<T> {
    nodes: Slab<NodeEntry<T>>,
    relatives: Slab<NodeListEntry>
}

impl<T> DirectedAcyclicGraph<T> {
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            nodes: Slab::with_capacity(capacity),
            relatives: Slab::with_capacity(capacity * (capacity + 1) / 2)
        }
    }

    pub fn vacant_node(&self) -> NodeId {
        NodeId(self.nodes.vacant_key() as u16)
    }

    pub fn insert(&mut self, value: T, parents: &[NodeId]) -> NodeId {
        let node = self.vacant_node();

        let mut first_parent = u16::MAX;
        for &parent in parents {
            self.add_child_to_parent(parent, node);
            
            first_parent = self.relatives.insert(NodeListEntry {
                node: parent,
                next_entry: first_parent
            }) as u16;
        }

        self.nodes.insert(NodeEntry {
            value,
            first_child_entry: u16::MAX,
            first_parent_entry: first_parent
        });

        node
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn pop(&mut self, node: NodeId) -> T {
        let result = self.nodes.remove(node.0 as usize);
        assert!(result.first_parent_entry == u16::MAX, "Cannot pop node that has dependencies.");

        let mut current_entry = result.first_child_entry;
        while current_entry != u16::MAX {
            let relative = self.relatives[current_entry as usize];
            self.relatives.remove(current_entry as usize);
            self.remove_parent_from_child(node, relative.node);
            current_entry = relative.next_entry;
        }

        result.value
    }

    pub fn nodes(&self) -> impl '_ + Iterator<Item = NodeId> {
        self.nodes.iter().map(|(id, _)| NodeId(id as u16))
    }

    pub fn children(&self, node: NodeId) -> impl '_ + Iterator<Item = NodeId> {
        DirectedAcyclicGraphIter {
            graph: self,
            current_entry: self.nodes[node.0 as usize].first_child_entry
        }
    }

    pub fn parents(&self, node: NodeId) -> impl '_ + Iterator<Item = NodeId> {
        DirectedAcyclicGraphIter {
            graph: self,
            current_entry: self.nodes[node.0 as usize].first_parent_entry
        }
    }

    pub fn add_parent(&mut self, parent: NodeId, node: NodeId) {
        self.add_child_to_parent(parent, node);
        let node = &mut self.nodes[node.0 as usize];
        let new_entry = self.relatives.insert(NodeListEntry { node: parent, next_entry: node.first_parent_entry });
        node.first_parent_entry = new_entry as u16;
    }
    
    pub fn top_level(&self, node: NodeId) -> bool {
        self.nodes[node.0 as usize].first_parent_entry == u16::MAX
    }

    fn add_child_to_parent(&mut self, parent: NodeId, node: NodeId) {
        let mut parent_node = &mut self.nodes[parent.0 as usize];
        let new_entry = self.relatives.insert(NodeListEntry { node, next_entry: parent_node.first_child_entry });
        parent_node.first_child_entry = new_entry as u16;
    }

    fn remove_parent_from_child(&mut self, parent: NodeId, node: NodeId) {
        let mut last_entry = &mut self.nodes[node.0 as usize].first_parent_entry;
        let mut next_entry = self.relatives[*last_entry as usize];

        while next_entry.node != parent {
            let key = *last_entry;
            let (current, next) = self.relatives.get2_mut(key as usize, next_entry.next_entry as usize)
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

pub struct DirectedAcyclicGraphIter<'a, T> {
    graph: &'a DirectedAcyclicGraph<T>,
    current_entry: u16
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

#[derive(Debug, Default)]
pub struct DirectedAcyclicGraphFlags {
    flags: BitVec
}

impl DirectedAcyclicGraphFlags {
    pub fn new() -> Self {
        Self {
            flags: BitVec::new()
        }
    }

    pub fn clear(&mut self) {
        self.flags.clear();
    }

    pub fn resize_for<T>(&mut self, graph: &DirectedAcyclicGraph<T>) {
        self.flags.resize(self.flags.len().max(graph.nodes.capacity()), false);
    }

    pub fn set(&mut self, node: NodeId, value: bool) -> bool {
        self.flags.replace(node.0 as usize, value)
    }

    pub fn get(&self, node: NodeId) -> bool {
        self.flags[node.0 as usize]
    }

    pub fn first_set_node(&self) -> Option<NodeId> {
        self.flags.first_one().map(|x| NodeId(x as u16))
    }

    pub fn and(&mut self, a: &Self, b: &Self) {
        self.flags.clear();
        self.flags.extend_from_bitslice(&a.flags);
        self.flags &= &b.flags;
    }
}

#[derive(Copy, Clone, Debug)]
struct NodeEntry<T> {
    pub value: T,
    pub first_child_entry: u16,
    pub first_parent_entry: u16
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
struct NodeListEntry {
    pub node: NodeId,
    pub next_entry: u16
}