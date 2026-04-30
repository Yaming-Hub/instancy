//! Scope trait and implementations.
//!
//! A Scope represents a region of the dataflow graph that shares a common
//! timestamp type. Scopes can be nested (for loops) where inner scopes
//! have timestamps that extend the outer scope's timestamp.
//!
//! Scopes use shared interior state so that cloning a Scope (e.g., when
//! embedded in multiple DataStream values) shares operator/region allocation.

use std::fmt;
use std::sync::{Arc, Mutex};

use crate::error::Result;
use crate::order::Product;
use crate::progress::timestamp::Timestamp;

use super::region::{Region, RegionAllocator, RegionId};

/// A unique address within the dataflow graph.
/// Each element identifies a nesting level; the full path locates
/// an operator within possibly nested scopes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ScopeAddr(Vec<usize>);

impl ScopeAddr {
    /// Create a root-level address.
    pub fn root() -> Self {
        Self(Vec::new())
    }

    /// Create an address from components.
    pub fn from_parts(parts: Vec<usize>) -> Self {
        Self(parts)
    }

    /// Create a child address by appending an index.
    pub fn child(&self, index: usize) -> Self {
        let mut parts = self.0.clone();
        parts.push(index);
        Self(parts)
    }

    /// The nesting depth (0 = root scope).
    pub fn depth(&self) -> usize {
        self.0.len()
    }

    /// Get the address components.
    pub fn parts(&self) -> &[usize] {
        &self.0
    }
}

impl fmt::Display for ScopeAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let parts: Vec<String> = self.0.iter().map(|p| p.to_string()).collect();
        write!(f, "[{}]", parts.join("."))
    }
}

/// The Scope trait defines a region of the dataflow graph with a
/// uniform timestamp type.
///
/// Scopes manage operator registration, index allocation, and
/// provide the structural context for building dataflows.
///
/// Cloning a scope produces a handle to the same shared state, so
/// operator indices and regions remain consistent across all clones.
pub trait Scope: Clone + 'static {
    /// The timestamp type for this scope.
    type Timestamp: Timestamp;

    /// The human-readable name of this scope.
    fn name(&self) -> String;

    /// The address of this scope within the dataflow graph.
    fn addr(&self) -> ScopeAddr;

    /// Allocate a new operator index within this scope.
    fn allocate_operator_index(&mut self) -> usize;

    /// Get the number of operators registered in this scope.
    fn operator_count(&self) -> usize;

    /// Get the current default execution region for new operators.
    fn current_region(&self) -> Region;

    /// Get a region by its ID.
    fn region(&self, id: RegionId) -> Option<Region>;

    /// Create a new execution region with the given parallelism.
    /// Returns the region ID for use with subsequent operators.
    fn new_region(&mut self, parallelism: usize) -> RegionId;

    /// Allocate the next ingress (enter) slot index for the scope boundary.
    fn allocate_ingress_slot(&mut self) -> usize;

    /// Allocate the next egress (leave) slot index for the scope boundary.
    fn allocate_egress_slot(&mut self) -> usize;
}

/// Mutable state shared across all clones of a scope.
#[derive(Debug)]
struct ScopeState {
    /// Next operator index to allocate.
    next_operator_index: usize,
    /// Region allocator for execution regions.
    region_allocator: RegionAllocator,
    /// All regions created within this scope.
    regions: Vec<Region>,
    /// The index of the current default region.
    current_region_index: usize,
    /// Next ingress (enter) slot index for the scope boundary operator.
    next_ingress_slot: usize,
    /// Next egress (leave) slot index for the scope boundary operator.
    next_egress_slot: usize,
}

/// The root-level scope for a dataflow.
///
/// This is the top-level scope provided to the dataflow builder closure.
/// It has a single timestamp type and manages operator indexing and regions.
/// Cloning produces a handle to the same shared state.
#[derive(Debug, Clone)]
pub struct RootScope<T: Timestamp> {
    /// Name of this scope (immutable, shared by reference).
    name: Arc<String>,
    /// Address of this scope (immutable).
    addr: ScopeAddr,
    /// Shared mutable state.
    state: Arc<Mutex<ScopeState>>,
    /// Phantom for timestamp type.
    _phantom: std::marker::PhantomData<T>,
}

impl<T: Timestamp> RootScope<T> {
    /// Create a new root scope with a default region.
    pub fn new(name: impl Into<String>, default_parallelism: usize) -> Self {
        let mut region_allocator = RegionAllocator::new();
        let default_region = region_allocator.allocate(default_parallelism);

        Self {
            name: Arc::new(name.into()),
            addr: ScopeAddr::root(),
            state: Arc::new(Mutex::new(ScopeState {
                next_operator_index: 0,
                region_allocator,
                regions: vec![default_region],
                current_region_index: 0,
                next_ingress_slot: 0,
                next_egress_slot: 0,
            })),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Set the current default region for new operators.
    pub fn set_current_region(&mut self, id: RegionId) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        if let Some(idx) = state.regions.iter().position(|r| r.id() == id) {
            state.current_region_index = idx;
            Ok(())
        } else {
            Err(crate::error::Error::Custom(format!(
                "Region {} not found in scope '{}'",
                id, self.name
            )))
        }
    }

    /// Get all regions in this scope (snapshot).
    pub fn regions(&self) -> Vec<Region> {
        self.state.lock().unwrap().regions.clone()
    }

    /// Create a nested child scope for iterative computation.
    ///
    /// The child scope uses `Product<T, TInner>` timestamps, where `TInner`
    /// tracks the iteration counter. Operators inside the child scope see
    /// the combined timestamp and can distinguish iterations.
    ///
    /// The child scope inherits the same parallelism as the parent's current region.
    pub fn iterative<TInner: Timestamp>(&mut self, name: impl Into<String>) -> ChildScope<Product<T, TInner>>
    where
        Product<T, TInner>: Timestamp,
    {
        let child_index = self.allocate_operator_index();
        let parallelism = self.current_region().parallelism();
        ChildScope::new(name, &self.addr(), child_index, parallelism)
    }
}

impl<T: Timestamp> Scope for RootScope<T> {
    type Timestamp = T;

    fn name(&self) -> String {
        (*self.name).clone()
    }

    fn addr(&self) -> ScopeAddr {
        self.addr.clone()
    }

    fn allocate_operator_index(&mut self) -> usize {
        let mut state = self.state.lock().unwrap();
        let idx = state.next_operator_index;
        state.next_operator_index += 1;
        idx
    }

    fn operator_count(&self) -> usize {
        self.state.lock().unwrap().next_operator_index
    }

    fn current_region(&self) -> Region {
        let state = self.state.lock().unwrap();
        state.regions[state.current_region_index].clone()
    }

    fn region(&self, id: RegionId) -> Option<Region> {
        let state = self.state.lock().unwrap();
        state.regions.iter().find(|r| r.id() == id).cloned()
    }

    fn new_region(&mut self, parallelism: usize) -> RegionId {
        let mut state = self.state.lock().unwrap();
        let region = state.region_allocator.allocate(parallelism);
        let id = region.id();
        state.regions.push(region);
        id
    }

    fn allocate_ingress_slot(&mut self) -> usize {
        let mut state = self.state.lock().unwrap();
        let slot = state.next_ingress_slot;
        state.next_ingress_slot += 1;
        slot
    }

    fn allocate_egress_slot(&mut self) -> usize {
        let mut state = self.state.lock().unwrap();
        let slot = state.next_egress_slot;
        state.next_egress_slot += 1;
        slot
    }
}
///
/// The child scope has a timestamp type that extends the parent's timestamp
/// (typically `Product<TOuter, TInner>`). Cloning shares state.
#[derive(Debug, Clone)]
pub struct ChildScope<T: Timestamp> {
    /// Name of this child scope.
    name: Arc<String>,
    /// Address of this scope (includes parent path + child index).
    addr: ScopeAddr,
    /// Shared mutable state.
    state: Arc<Mutex<ScopeState>>,
    /// Phantom for timestamp type.
    _phantom: std::marker::PhantomData<T>,
}

impl<T: Timestamp> ChildScope<T> {
    /// Create a new child scope nested under a parent.
    ///
    /// Operator index 0 is reserved for the scope boundary (ingress/egress
    /// metadata used by the progress tracker). User operators start at index 1.
    pub fn new(
        name: impl Into<String>,
        parent_addr: &ScopeAddr,
        child_index: usize,
        parallelism: usize,
    ) -> Self {
        let mut region_allocator = RegionAllocator::new();
        let default_region = region_allocator.allocate(parallelism);

        Self {
            name: Arc::new(name.into()),
            addr: parent_addr.child(child_index),
            state: Arc::new(Mutex::new(ScopeState {
                // Start at 1: index 0 is reserved for scope boundary metadata.
                next_operator_index: 1,
                region_allocator,
                regions: vec![default_region],
                current_region_index: 0,
                next_ingress_slot: 0,
                next_egress_slot: 0,
            })),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Create a further-nested child scope for iterative computation.
    ///
    /// Enables nested loops: an iterative scope inside another iterative scope.
    pub fn iterative<TInner: Timestamp>(&mut self, name: impl Into<String>) -> ChildScope<Product<T, TInner>>
    where
        Product<T, TInner>: Timestamp,
    {
        let child_index = self.allocate_operator_index();
        let parallelism = self.current_region().parallelism();
        ChildScope::new(name, &self.addr(), child_index, parallelism)
    }
}

impl<T: Timestamp> Scope for ChildScope<T> {
    type Timestamp = T;

    fn name(&self) -> String {
        (*self.name).clone()
    }

    fn addr(&self) -> ScopeAddr {
        self.addr.clone()
    }

    fn allocate_operator_index(&mut self) -> usize {
        let mut state = self.state.lock().unwrap();
        let idx = state.next_operator_index;
        state.next_operator_index += 1;
        idx
    }

    fn operator_count(&self) -> usize {
        self.state.lock().unwrap().next_operator_index
    }

    fn current_region(&self) -> Region {
        let state = self.state.lock().unwrap();
        state.regions[state.current_region_index].clone()
    }

    fn region(&self, id: RegionId) -> Option<Region> {
        let state = self.state.lock().unwrap();
        state.regions.iter().find(|r| r.id() == id).cloned()
    }

    fn new_region(&mut self, parallelism: usize) -> RegionId {
        let mut state = self.state.lock().unwrap();
        let region = state.region_allocator.allocate(parallelism);
        let id = region.id();
        state.regions.push(region);
        id
    }

    fn allocate_ingress_slot(&mut self) -> usize {
        let mut state = self.state.lock().unwrap();
        let slot = state.next_ingress_slot;
        state.next_ingress_slot += 1;
        slot
    }

    fn allocate_egress_slot(&mut self) -> usize {
        let mut state = self.state.lock().unwrap();
        let slot = state.next_egress_slot;
        state.next_egress_slot += 1;
        slot
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_addr_root() {
        let addr = ScopeAddr::root();
        assert_eq!(addr.depth(), 0);
        assert_eq!(addr.parts(), &[]);
        assert_eq!(format!("{}", addr), "[]");
    }

    #[test]
    fn scope_addr_child() {
        let root = ScopeAddr::root();
        let child = root.child(3);
        assert_eq!(child.depth(), 1);
        assert_eq!(child.parts(), &[3]);
        assert_eq!(format!("{}", child), "[3]");

        let grandchild = child.child(7);
        assert_eq!(grandchild.depth(), 2);
        assert_eq!(grandchild.parts(), &[3, 7]);
        assert_eq!(format!("{}", grandchild), "[3.7]");
    }

    #[test]
    fn root_scope_basic() {
        let mut scope = RootScope::<u64>::new("test", 4);
        assert_eq!(scope.name(), "test");
        assert_eq!(scope.addr().depth(), 0);
        assert_eq!(scope.operator_count(), 0);
        assert_eq!(scope.current_region().parallelism(), 4);

        // Allocate operators
        assert_eq!(scope.allocate_operator_index(), 0);
        assert_eq!(scope.allocate_operator_index(), 1);
        assert_eq!(scope.operator_count(), 2);
    }

    #[test]
    fn root_scope_multiple_regions() {
        let mut scope = RootScope::<u64>::new("multi_region", 4);

        // Default region (parallelism=4)
        let default_region_id = scope.current_region().id();
        assert_eq!(scope.current_region().parallelism(), 4);

        // Create new region with different parallelism
        let new_id = scope.new_region(8);
        assert_ne!(new_id, default_region_id);

        // Switch to the new region
        scope.set_current_region(new_id).unwrap();
        assert_eq!(scope.current_region().parallelism(), 8);

        // Switch back
        scope.set_current_region(default_region_id).unwrap();
        assert_eq!(scope.current_region().parallelism(), 4);
    }

    #[test]
    fn root_scope_invalid_region() {
        let mut scope = RootScope::<u64>::new("test", 4);
        let bad_id = RegionId::new(999);
        assert!(scope.set_current_region(bad_id).is_err());
    }

    #[test]
    fn child_scope_nested_address() {
        let parent_addr = ScopeAddr::root();
        let scope = ChildScope::<u64>::new("loop", &parent_addr, 5, 2);

        assert_eq!(scope.name(), "loop");
        assert_eq!(scope.addr().depth(), 1);
        assert_eq!(scope.addr().parts(), &[5]);
        assert_eq!(scope.current_region().parallelism(), 2);
    }

    #[test]
    fn child_scope_operator_allocation() {
        let parent_addr = ScopeAddr::root();
        let mut scope = ChildScope::<u64>::new("inner", &parent_addr, 0, 4);

        // Index 0 is reserved for scope boundary; first user allocation is 1
        assert_eq!(scope.allocate_operator_index(), 1);
        assert_eq!(scope.allocate_operator_index(), 2);
        assert_eq!(scope.operator_count(), 3); // includes reserved index 0
    }

    #[test]
    fn child_scope_independent_regions() {
        let parent_addr = ScopeAddr::root();
        let mut scope = ChildScope::<u64>::new("inner", &parent_addr, 0, 4);

        // Child scope has its own region allocator
        let r1 = scope.new_region(2);
        let r2 = scope.new_region(16);
        assert_ne!(r1, r2);

        // Both regions exist
        assert!(scope.region(r1).is_some());
        assert!(scope.region(r2).is_some());
    }

    #[test]
    fn scope_clone_shares_state() {
        let mut scope = RootScope::<u64>::new("shared", 4);
        let mut cloned = scope.clone();

        // Allocate from original
        assert_eq!(scope.allocate_operator_index(), 0);

        // Clone sees the allocation
        assert_eq!(cloned.operator_count(), 1);

        // Allocate from clone
        assert_eq!(cloned.allocate_operator_index(), 1);

        // Original sees it too
        assert_eq!(scope.operator_count(), 2);
    }

    #[test]
    fn scope_clone_shares_regions() {
        let mut scope = RootScope::<u64>::new("shared", 4);
        let cloned = scope.clone();

        // Add region from original
        let new_id = scope.new_region(8);

        // Clone sees the new region
        let region = cloned.region(new_id);
        assert!(region.is_some());
        assert_eq!(region.unwrap().parallelism(), 8);
    }
}
