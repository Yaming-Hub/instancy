//! Stage-oriented scope trait and implementations.
//!
//! A Scope represents a stage of the dataflow graph that shares a common
//! timestamp type. Scopes can be nested (for loops) where inner scopes have
//! timestamps that extend the outer scope's timestamp.

use std::fmt;
use std::sync::{Arc, Mutex};

use crate::error::Result;
use crate::order::Product;
use crate::progress::timestamp::Timestamp;

use super::graph::{DataflowGraph, EdgeInfo, OperatorInfo};
use super::stage::StageId;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ScopeAddr(Vec<usize>);

impl ScopeAddr {
    pub fn root() -> Self {
        Self(Vec::new())
    }

    pub fn from_parts(parts: Vec<usize>) -> Self {
        Self(parts)
    }

    pub fn child(&self, index: usize) -> Self {
        let mut parts = self.0.clone();
        parts.push(index);
        Self(parts)
    }

    pub fn depth(&self) -> usize {
        self.0.len()
    }

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

pub trait Scope: Clone + 'static {
    type Timestamp: Timestamp;

    fn name(&self) -> String;
    fn addr(&self) -> ScopeAddr;
    fn allocate_operator_index(&mut self) -> usize;
    fn operator_count(&self) -> usize;
    fn current_stage_id(&self) -> StageId;
    fn stage_parallelism(&self, id: StageId) -> Option<usize>;
    fn new_stage(&mut self, parallelism: usize) -> StageId;
    fn allocate_ingress_slot(&mut self) -> usize;
    fn allocate_egress_slot(&mut self) -> usize;
    fn register_operator(&mut self, info: OperatorInfo) -> Result<()>;
    fn add_edge(&mut self, edge: EdgeInfo);
    fn set_exchange_parallelism(&mut self, operator_index: usize, parallelism: usize);
    fn increment_operator_input_count(&mut self, operator_index: usize);
    fn increment_operator_output_count(&mut self, operator_index: usize);
    fn graph(&self) -> DataflowGraph;
}

#[derive(Debug)]
struct ScopeState {
    next_operator_index: usize,
    current_stage_id: StageId,
    next_stage_id: usize,
    stage_parallelism: Vec<usize>,
    next_ingress_slot: usize,
    next_egress_slot: usize,
    graph: DataflowGraph,
}

impl ScopeState {
    fn new(default_parallelism: usize, graph: DataflowGraph) -> Self {
        Self {
            next_operator_index: 1,
            current_stage_id: StageId::INITIAL,
            next_stage_id: 1,
            stage_parallelism: vec![default_parallelism],
            next_ingress_slot: 0,
            next_egress_slot: 0,
            graph,
        }
    }

    fn stage_parallelism(&self, id: StageId) -> Option<usize> {
        self.stage_parallelism.get(id.index()).copied()
    }

    fn new_stage(&mut self, parallelism: usize) -> StageId {
        let id = StageId::new(self.next_stage_id);
        self.next_stage_id += 1;
        self.stage_parallelism.push(parallelism);
        id
    }
}

#[derive(Debug, Clone)]
pub struct RootScope<T: Timestamp> {
    name: Arc<String>,
    addr: ScopeAddr,
    state: Arc<Mutex<ScopeState>>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: Timestamp> RootScope<T> {
    pub fn new(name: impl Into<String>, default_parallelism: usize) -> Self {
        Self {
            name: Arc::new(name.into()),
            addr: ScopeAddr::root(),
            state: Arc::new(Mutex::new(ScopeState::new(
                default_parallelism,
                DataflowGraph::new(),
            ))),
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn set_current_stage_id(&mut self, id: StageId) -> Result<()> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.stage_parallelism(id).is_some() {
            state.current_stage_id = id;
            Ok(())
        } else {
            Err(crate::error::Error::Custom(format!(
                "Stage {} not found in scope '{}'",
                id, self.name
            )))
        }
    }

    pub fn iterative<TInner: Timestamp>(
        &mut self,
        name: impl Into<String>,
    ) -> ChildScope<Product<T, TInner>>
    where
        Product<T, TInner>: Timestamp,
    {
        let name = name.into();
        let child_index = self.allocate_operator_index();
        let stage_id = self.current_stage_id();
        let parallelism = self
            .stage_parallelism(stage_id)
            // SAFETY: current_stage_id() returns a stage registered in this scope's state
            .expect("current stage must exist in scope state");

        self.register_operator(OperatorInfo::new(
            child_index,
            format!("subscope:{}", name),
            stage_id,
            0,
            0,
        ))
        // SAFETY: index was freshly allocated by allocate_operator_index() on the previous line
        .expect("child scope operator index was just allocated, cannot conflict");

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
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let idx = state.next_operator_index;
        state.next_operator_index += 1;
        idx
    }

    fn operator_count(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .next_operator_index
            - 1
    }

    fn current_stage_id(&self) -> StageId {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .current_stage_id
    }

    fn stage_parallelism(&self, id: StageId) -> Option<usize> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .stage_parallelism(id)
    }

    fn new_stage(&mut self, parallelism: usize) -> StageId {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .new_stage(parallelism)
    }

    fn allocate_ingress_slot(&mut self) -> usize {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let slot = state.next_ingress_slot;
        state.next_ingress_slot += 1;
        slot
    }

    fn allocate_egress_slot(&mut self) -> usize {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let slot = state.next_egress_slot;
        state.next_egress_slot += 1;
        slot
    }

    fn register_operator(&mut self, info: OperatorInfo) -> Result<()> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .graph
            .register_operator(info)
    }

    fn add_edge(&mut self, edge: EdgeInfo) {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .graph
            .add_edge(edge);
    }

    fn set_exchange_parallelism(&mut self, operator_index: usize, parallelism: usize) {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .graph
            .set_exchange_parallelism(operator_index, parallelism);
    }

    fn increment_operator_input_count(&mut self, operator_index: usize) {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .graph
            .increment_input_count(operator_index);
    }

    fn increment_operator_output_count(&mut self, operator_index: usize) {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .graph
            .increment_output_count(operator_index);
    }

    fn graph(&self) -> DataflowGraph {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .graph
            .clone()
    }
}

#[derive(Debug, Clone)]
pub struct ChildScope<T: Timestamp> {
    name: Arc<String>,
    addr: ScopeAddr,
    state: Arc<Mutex<ScopeState>>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: Timestamp> ChildScope<T> {
    pub fn new(
        name: impl Into<String>,
        parent_addr: &ScopeAddr,
        child_index: usize,
        parallelism: usize,
    ) -> Self {
        let mut graph = DataflowGraph::new();
        graph
            .register_operator(OperatorInfo::new(
                0,
                "scope-boundary",
                StageId::INITIAL,
                0,
                0,
            ))
            // SAFETY: graph is freshly created, index 0 cannot conflict
            .expect("scope-boundary registration on fresh graph cannot fail");

        Self {
            name: Arc::new(name.into()),
            addr: parent_addr.child(child_index),
            state: Arc::new(Mutex::new(ScopeState::new(parallelism, graph))),
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn iterative<TInner: Timestamp>(
        &mut self,
        name: impl Into<String>,
    ) -> ChildScope<Product<T, TInner>>
    where
        Product<T, TInner>: Timestamp,
    {
        let name = name.into();
        let child_index = self.allocate_operator_index();
        let stage_id = self.current_stage_id();
        let parallelism = self
            .stage_parallelism(stage_id)
            // SAFETY: current_stage_id() returns a stage registered in this scope's state
            .expect("current stage must exist in scope state");

        self.register_operator(OperatorInfo::new(
            child_index,
            format!("subscope:{}", name),
            stage_id,
            0,
            0,
        ))
        // SAFETY: index was freshly allocated by allocate_operator_index() on the previous line
        .expect("child scope operator index was just allocated, cannot conflict");

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
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let idx = state.next_operator_index;
        state.next_operator_index += 1;
        idx
    }

    fn operator_count(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .next_operator_index
            - 1
    }

    fn current_stage_id(&self) -> StageId {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .current_stage_id
    }

    fn stage_parallelism(&self, id: StageId) -> Option<usize> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .stage_parallelism(id)
    }

    fn new_stage(&mut self, parallelism: usize) -> StageId {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .new_stage(parallelism)
    }

    fn allocate_ingress_slot(&mut self) -> usize {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let slot = state.next_ingress_slot;
        state.next_ingress_slot += 1;
        state.graph.increment_output_count(0);
        slot
    }

    fn allocate_egress_slot(&mut self) -> usize {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let slot = state.next_egress_slot;
        state.next_egress_slot += 1;
        state.graph.increment_input_count(0);
        slot
    }

    fn register_operator(&mut self, info: OperatorInfo) -> Result<()> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .graph
            .register_operator(info)
    }

    fn add_edge(&mut self, edge: EdgeInfo) {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .graph
            .add_edge(edge);
    }

    fn set_exchange_parallelism(&mut self, operator_index: usize, parallelism: usize) {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .graph
            .set_exchange_parallelism(operator_index, parallelism);
    }

    fn increment_operator_input_count(&mut self, operator_index: usize) {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .graph
            .increment_input_count(operator_index);
    }

    fn increment_operator_output_count(&mut self, operator_index: usize) {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .graph
            .increment_output_count(operator_index);
    }

    fn graph(&self) -> DataflowGraph {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .graph
            .clone()
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
    }

    #[test]
    fn root_scope_basic() {
        let scope = RootScope::<u64>::new("test", 4);
        assert_eq!(scope.name(), "test");
        assert_eq!(scope.addr().depth(), 0);
        assert_eq!(scope.operator_count(), 0);
        assert_eq!(scope.current_stage_id(), StageId::INITIAL);
        assert_eq!(scope.stage_parallelism(StageId::INITIAL), Some(4));
    }

    #[test]
    fn stages_allocate_sequentially() {
        let mut scope = RootScope::<u64>::new("test", 4);
        let s1 = scope.new_stage(8);
        let s2 = scope.new_stage(1);
        assert_eq!(s1, StageId::new(1));
        assert_eq!(s2, StageId::new(2));
        assert_eq!(scope.stage_parallelism(s1), Some(8));
        assert_eq!(scope.stage_parallelism(s2), Some(1));
    }

    #[test]
    fn set_current_stage_id_switches_default_stage() {
        let mut scope = RootScope::<u64>::new("test", 4);
        let s1 = scope.new_stage(8);
        scope.set_current_stage_id(s1).unwrap();
        assert_eq!(scope.current_stage_id(), s1);
    }

    #[test]
    fn child_scope_registers_boundary_operator() {
        let child = ChildScope::<u64>::new("child", &ScopeAddr::root(), 2, 3);
        let graph = child.graph();
        let boundary = graph.operator(0).expect("boundary operator should exist");
        assert_eq!(boundary.stage_id, StageId::INITIAL);
        assert_eq!(child.stage_parallelism(StageId::INITIAL), Some(3));
    }

    #[test]
    fn iterative_child_inherits_current_stage_parallelism() {
        let mut root = RootScope::<u64>::new("root", 4);
        let s1 = root.new_stage(7);
        root.set_current_stage_id(s1).unwrap();
        let child = root.iterative::<u32>("loop");
        assert_eq!(child.stage_parallelism(child.current_stage_id()), Some(7));
    }
}
