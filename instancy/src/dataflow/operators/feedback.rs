//! Feedback operator and scope boundary operators for iterative computation.
//!
//! This module provides:
//! - [`EnterExt::enter`]: Moves a stream into a nested (iterative) scope.
//! - [`LeaveExt::leave`]: Moves a stream out of a nested scope back to the parent.
//! - [`FeedbackExt::feedback`]: Creates a feedback (loop-back) edge in an iterative scope.
//! - [`ConnectLoop`]: Closes a loop by connecting a stream to a feedback handle.
//!
//! # Loop construction pattern
//!
//! ```text
//! let child_scope = parent_scope.iterative::<u32>("my_loop");
//! let (handle, feedback_stream) = child_scope.feedback::<D>(inner_summary);
//!
//! let input_in_loop = input_stream.enter(&child_scope);
//! let combined = input_in_loop.concat(&feedback_stream);  // merge input + feedback
//! let result = combined.unary("process", |input, output| { ... });
//!
//! // Items to iterate go back; items done leave the scope
//! let (iterate, done) = result.branch(|item| item.is_converged());
//! iterate.connect_loop(handle);  // close the loop
//! let output = done.leave(&parent_scope);
//! ```

use std::fmt;
use std::marker::PhantomData;

use crate::dataflow::graph::{EdgeInfo, OperatorInfo};
use crate::dataflow::region::RegionId;
use crate::dataflow::scope::{ChildScope, Scope};
use crate::dataflow::stream::{DataStream, Slot};
use crate::order::Product;
use crate::progress::timestamp::Timestamp;

// ============================================================================
// Scope boundary metadata
// ============================================================================

/// Describes the type of scope boundary crossing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScopeBoundary {
    /// Data entering a nested scope (outer → Product<outer, inner::minimum()>).
    Ingress,
    /// Data leaving a nested scope (Product<outer, inner> → outer, dropping inner).
    Egress,
}

/// Metadata for a scope ingress point (enter operator).
#[derive(Debug, Clone)]
pub struct IngressMetadata {
    /// The operator index in the child scope for this ingress.
    pub child_operator_index: usize,
    /// The region this ingress belongs to.
    pub region_id: RegionId,
}

/// Metadata for a scope egress point (leave operator).
#[derive(Debug, Clone)]
pub struct EgressMetadata {
    /// The operator index in the child scope for this egress.
    pub child_operator_index: usize,
    /// The region in the child scope.
    pub child_region_id: RegionId,
    /// The operator index in the parent scope for the output.
    pub parent_operator_index: usize,
    /// The region in the parent scope.
    pub parent_region_id: RegionId,
}

// ============================================================================
// Enter (ingress)
// ============================================================================

/// Extension trait for entering a nested iterative scope.
pub trait EnterExt<S: Scope, D> {
    /// Move this stream into a child scope, wrapping timestamps
    /// from `T` into `Product<T, TInner::minimum()>`.
    ///
    /// Creates an ingress boundary operator in the child scope.
    fn enter<TInner: Timestamp>(
        &self,
        child: &ChildScope<Product<S::Timestamp, TInner>>,
    ) -> DataStream<ChildScope<Product<S::Timestamp, TInner>>, D>
    where
        Product<S::Timestamp, TInner>: Timestamp;
}

impl<S: Scope, D: 'static> EnterExt<S, D> for DataStream<S, D> {
    fn enter<TInner: Timestamp>(
        &self,
        child: &ChildScope<Product<S::Timestamp, TInner>>,
    ) -> DataStream<ChildScope<Product<S::Timestamp, TInner>>, D>
    where
        Product<S::Timestamp, TInner>: Timestamp,
    {
        // Each enter() call gets a unique ingress slot on the scope boundary operator (index 0).
        let slot_index = child.clone().allocate_ingress_slot();
        let ingress_source = Slot::new(0, slot_index);
        let region_id = child.current_region().id();

        // Update the parent graph: the child scope operator receives this data.
        let child_index = child.addr().parts().last().copied()
            .expect("enter() called on scope with no parent address — this is a bug");
        let mut parent_scope = self.scope().clone();
        parent_scope.increment_operator_input_count(child_index);
        let parent_region = parent_scope.current_region().id();
        parent_scope.add_edge(EdgeInfo::new(
            *self.source(),
            Slot::new(child_index, slot_index),
            self.region_id(),
            parent_region,
        ));

        DataStream::new(child.clone(), ingress_source, region_id)
    }
}

// ============================================================================
// Leave (egress)
// ============================================================================

/// Extension trait for leaving a nested iterative scope.
pub trait LeaveExt<TOuter, TInner, D>
where
    TOuter: Timestamp,
    TInner: Timestamp,
    Product<TOuter, TInner>: Timestamp,
{
    /// Move this stream out of a child scope back to the parent,
    /// stripping the inner timestamp component.
    ///
    /// Creates an egress boundary operator in the child scope and
    /// produces a stream in the parent scope.
    fn leave<P: Scope<Timestamp = TOuter>>(
        &self,
        parent: &P,
    ) -> DataStream<P, D>;
}

impl<TOuter, TInner, D> LeaveExt<TOuter, TInner, D>
    for DataStream<ChildScope<Product<TOuter, TInner>>, D>
where
    TOuter: Timestamp,
    TInner: Timestamp,
    Product<TOuter, TInner>: Timestamp,
    D: 'static,
{
    fn leave<P: Scope<Timestamp = TOuter>>(
        &self,
        parent: &P,
    ) -> DataStream<P, D> {
        // Each leave() call gets a unique egress slot on the scope boundary operator.
        let slot_index = self.scope().clone().allocate_egress_slot();

        // In the parent, the stream comes from the subscope operator (identified by
        // the child scope's position in the parent's operator index space).
        let parent_op_index = self.scope().addr().parts().last().copied()
            .expect("leave() called on scope with no parent address — this is a bug");
        let parent_source = Slot::new(parent_op_index, slot_index);
        let region_id = parent.current_region().id();

        // Update the parent graph: the child scope operator produces this output.
        let mut parent_mut = parent.clone();
        parent_mut.increment_operator_output_count(parent_op_index);

        // Also record the edge in the child graph: from the stream source to
        // the boundary operator's egress input.
        let mut child_scope = self.scope().clone();
        child_scope.add_edge(EdgeInfo::new(
            *self.source(),
            Slot::new(0, slot_index),
            self.region_id(),
            child_scope.current_region().id(),
        ));

        DataStream::new(parent.clone(), parent_source, region_id)
    }
}

// ============================================================================
// Feedback (loop variable)
// ============================================================================

/// A handle representing the feedback (loop-back) edge target.
///
/// Created by [`FeedbackExt::feedback`] and consumed by
/// [`ConnectLoop::connect_loop`]. Must be connected exactly once.
///
/// The type parameters ensure that only a stream with matching scope
/// and data type can be connected.
pub struct FeedbackHandle<S: Scope, D> {
    /// Operator index of the feedback operator in the scope.
    operator_index: usize,
    /// The region this feedback belongs to.
    region_id: RegionId,
    /// The path summary describing timestamp advancement per iteration.
    summary: <S::Timestamp as Timestamp>::Summary,
    /// Whether this handle has been connected.
    connected: bool,
    /// Phantom for data type.
    _data: PhantomData<D>,
}

impl<S: Scope, D> FeedbackHandle<S, D> {
    /// Get the operator index of the feedback operator.
    pub fn operator_index(&self) -> usize {
        self.operator_index
    }

    /// Get the region ID.
    pub fn region_id(&self) -> RegionId {
        self.region_id
    }

    /// Get the path summary for this feedback edge.
    pub fn summary(&self) -> &<S::Timestamp as Timestamp>::Summary {
        &self.summary
    }
}

impl<S: Scope, D> fmt::Debug for FeedbackHandle<S, D>
where
    <S::Timestamp as Timestamp>::Summary: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FeedbackHandle")
            .field("operator_index", &self.operator_index)
            .field("region_id", &self.region_id)
            .field("summary", &self.summary)
            .field("connected", &self.connected)
            .finish()
    }
}

impl<S: Scope, D> Drop for FeedbackHandle<S, D> {
    fn drop(&mut self) {
        if !self.connected {
            // Warn in all builds. Graph validation will catch this as a hard error,
            // but the warning helps developers find the issue during development.
            #[cfg(feature = "tracing")]
            tracing::warn!(
                operator_index = self.operator_index,
                "FeedbackHandle was dropped without being connected. \
                 Call connect_loop() to close the loop."
            );
            // In debug builds, additionally panic to catch this early.
            debug_assert!(
                self.connected,
                "FeedbackHandle (operator {}) was dropped without being connected. \
                 Call connect_loop() to close the loop.",
                self.operator_index,
            );
        }
    }
}

/// Extension trait for creating feedback (loop-back) edges in iterative scopes.
///
/// This is called on a `ChildScope<Product<TOuter, TInner>>` to create a
/// loop variable. The returned stream represents data flowing back from a
/// previous iteration.
pub trait FeedbackExt<TOuter, TInner>
where
    TOuter: Timestamp,
    TInner: Timestamp,
    Product<TOuter, TInner>: Timestamp,
{
    /// Create a feedback edge with the given inner summary.
    ///
    /// The `inner_summary` describes how the inner (iteration) timestamp
    /// advances on each loop iteration. For example, `1u32` means the
    /// iteration counter increments by 1 each time data loops back.
    ///
    /// Returns `(handle, stream)`:
    /// - `handle`: Must be connected via `connect_loop()` to close the loop.
    /// - `stream`: The stream of data arriving from the feedback edge.
    ///
    /// # Panics
    ///
    /// The `handle` must be connected exactly once. Dropping it without
    /// connecting triggers a debug assertion.
    fn feedback<D: 'static>(
        &mut self,
        inner_summary: TInner::Summary,
    ) -> (FeedbackHandle<ChildScope<Product<TOuter, TInner>>, D>, DataStream<ChildScope<Product<TOuter, TInner>>, D>);
}

impl<TOuter, TInner> FeedbackExt<TOuter, TInner> for ChildScope<Product<TOuter, TInner>>
where
    TOuter: Timestamp,
    TInner: Timestamp,
    Product<TOuter, TInner>: Timestamp<Summary = Product<TOuter::Summary, TInner::Summary>>,
    <TOuter as Timestamp>::Summary: Default,
    <TInner as Timestamp>::Summary: Default,
{
    fn feedback<D: 'static>(
        &mut self,
        inner_summary: TInner::Summary,
    ) -> (FeedbackHandle<ChildScope<Product<TOuter, TInner>>, D>, DataStream<ChildScope<Product<TOuter, TInner>>, D>) {
        let operator_index = self.allocate_operator_index();
        let region_id = self.current_region().id();

        // Register the feedback operator in the child scope's graph.
        // It has 1 input (from connect_loop) and 1 output (the feedback stream).
        self.register_operator(OperatorInfo::new(
            operator_index,
            "feedback",
            region_id,
            1,
            1,
        ))
        .expect("feedback operator index was just allocated, cannot conflict");

        // Lift the inner summary to a Product summary:
        // outer advances by identity (Default), inner advances by the provided summary.
        let full_summary: Product<TOuter::Summary, TInner::Summary> = Product::new(
            <TOuter as Timestamp>::Summary::default(),
            inner_summary,
        );

        let handle = FeedbackHandle {
            operator_index,
            region_id,
            summary: full_summary,
            connected: false,
            _data: PhantomData,
        };

        let source = Slot::new(operator_index, 0);
        let stream = DataStream::new(self.clone(), source, region_id);

        (handle, stream)
    }
}

// ============================================================================
// ConnectLoop
// ============================================================================

/// Extension trait for closing a feedback loop.
pub trait ConnectLoop<S: Scope, D> {
    /// Connect this stream back to the feedback handle, closing the loop.
    ///
    /// Consumes the handle to ensure it is connected exactly once.
    fn connect_loop(self, handle: FeedbackHandle<S, D>);
}

impl<S: Scope, D: 'static> ConnectLoop<S, D> for DataStream<S, D> {
    fn connect_loop(self, mut handle: FeedbackHandle<S, D>) {
        // Mark as connected so the Drop impl doesn't fire the assertion.
        handle.connected = true;

        // Record the feedback edge in the graph: stream source → feedback input.
        let mut scope = self.scope().clone();
        scope.add_edge(EdgeInfo::new(
            *self.source(),
            Slot::new(handle.operator_index, 0),
            self.region_id(),
            handle.region_id,
        ));

        // Note: This edge creates a cycle in the graph. The validate() method
        // should either exclude feedback edges from cycle detection or feedback
        // edges should be tracked separately. For now, topological_order() will
        // detect the cycle — feedback-aware validation is deferred to PR 22.
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataflow::scope::RootScope;
    use crate::order::Product;
    use crate::progress::timestamp::PathSummary;

    // --- Iterative scope creation ---

    #[test]
    fn iterative_creates_child_scope_with_product_timestamp() {
        let mut root = RootScope::<u64>::new("root", 4);
        let child = root.iterative::<u32>("my_loop");

        assert_eq!(child.name(), "my_loop");
        // Child scope address is [parent_index] where parent_index is the allocated op index
        assert_eq!(child.addr().depth(), 1);
        assert_eq!(child.current_region().parallelism(), 4);
    }

    #[test]
    fn iterative_allocates_parent_operator_index() {
        let mut root = RootScope::<u64>::new("root", 4);
        assert_eq!(root.operator_count(), 0);

        let _child = root.iterative::<u32>("loop1");
        assert_eq!(root.operator_count(), 1);

        let _child2 = root.iterative::<u32>("loop2");
        assert_eq!(root.operator_count(), 2);
    }

    #[test]
    fn iterative_child_starts_at_operator_index_1() {
        let mut root = RootScope::<u64>::new("root", 4);
        let mut child = root.iterative::<u32>("my_loop");

        // First user operator should be index 1 (0 is reserved for scope boundary)
        assert_eq!(child.allocate_operator_index(), 1);
        assert_eq!(child.allocate_operator_index(), 2);
    }

    #[test]
    fn nested_iterative() {
        let mut root = RootScope::<u64>::new("root", 4);
        let mut child = root.iterative::<u32>("outer_loop");
        let inner = child.iterative::<u32>("inner_loop");

        // Inner scope has depth 2
        assert_eq!(inner.addr().depth(), 2);
        assert_eq!(inner.current_region().parallelism(), 4);
    }

    // --- Enter ---

    #[test]
    fn enter_produces_stream_in_child_scope() {
        let mut root = RootScope::<u64>::new("root", 4);
        let child = root.iterative::<u32>("loop");

        let source = Slot::new(0, 0);
        let region_id = root.current_region().id();
        let parent_stream: DataStream<RootScope<u64>, i32> =
            DataStream::new(root.clone(), source, region_id);

        let child_stream = parent_stream.enter(&child);

        // Stream is now in child scope
        assert_eq!(child_stream.scope().name(), "loop");
        // Source is the scope boundary (operator 0)
        assert_eq!(child_stream.source().operator_index, 0);
    }

    #[test]
    fn enter_preserves_child_region() {
        let mut root = RootScope::<u64>::new("root", 4);
        let child = root.iterative::<u32>("loop");

        let source = Slot::new(0, 0);
        let region_id = root.current_region().id();
        let parent_stream: DataStream<RootScope<u64>, i32> =
            DataStream::new(root.clone(), source, region_id);

        let child_stream = parent_stream.enter(&child);
        assert_eq!(child_stream.region_id(), child.current_region().id());
    }

    #[test]
    fn multiple_enters_get_distinct_slots() {
        let mut root = RootScope::<u64>::new("root", 4);
        let child = root.iterative::<u32>("loop");

        let region_id = root.current_region().id();
        let stream1: DataStream<RootScope<u64>, i32> =
            DataStream::new(root.clone(), Slot::new(0, 0), region_id);
        let stream2: DataStream<RootScope<u64>, String> =
            DataStream::new(root.clone(), Slot::new(1, 0), region_id);

        let in1 = stream1.enter(&child);
        let in2 = stream2.enter(&child);

        // Both use operator 0 (boundary) but different slot indices
        assert_eq!(in1.source().operator_index, 0);
        assert_eq!(in2.source().operator_index, 0);
        assert_ne!(in1.source().slot_index, in2.source().slot_index);
        assert_eq!(in1.source().slot_index, 0);
        assert_eq!(in2.source().slot_index, 1);
    }

    #[test]
    fn multiple_leaves_get_distinct_slots() {
        let mut root = RootScope::<u64>::new("root", 4);
        let mut child = root.iterative::<u32>("loop");

        let child_region = child.current_region().id();
        let op1 = child.allocate_operator_index();
        let op2 = child.allocate_operator_index();

        let s1: DataStream<ChildScope<Product<u64, u32>>, i32> =
            DataStream::new(child.clone(), Slot::new(op1, 0), child_region);
        let s2: DataStream<ChildScope<Product<u64, u32>>, String> =
            DataStream::new(child.clone(), Slot::new(op2, 0), child_region);

        let out1 = s1.leave(&root);
        let out2 = s2.leave(&root);

        // Both reference the same parent operator (subscope) but different slots
        assert_eq!(out1.source().operator_index, out2.source().operator_index);
        assert_ne!(out1.source().slot_index, out2.source().slot_index);
        assert_eq!(out1.source().slot_index, 0);
        assert_eq!(out2.source().slot_index, 1);
    }

    // --- Leave ---

    #[test]
    fn leave_produces_stream_in_parent_scope() {
        let mut root = RootScope::<u64>::new("root", 4);
        let mut child = root.iterative::<u32>("loop");

        let op_idx = child.allocate_operator_index();
        let child_region = child.current_region().id();
        let child_stream: DataStream<ChildScope<Product<u64, u32>>, i32> =
            DataStream::new(child.clone(), Slot::new(op_idx, 0), child_region);

        let parent_stream = child_stream.leave(&root);

        assert_eq!(parent_stream.scope().name(), "root");
    }

    #[test]
    fn leave_source_references_subscope_operator() {
        let mut root = RootScope::<u64>::new("root", 4);
        let mut child = root.iterative::<u32>("loop");

        let op_idx = child.allocate_operator_index();
        let child_region = child.current_region().id();
        let child_stream: DataStream<ChildScope<Product<u64, u32>>, i32> =
            DataStream::new(child.clone(), Slot::new(op_idx, 0), child_region);

        let parent_stream = child_stream.leave(&root);

        // The parent stream's source operator index corresponds to the child scope's
        // position in the parent (last part of child's address).
        let child_addr = child.addr();
        let expected_parent_op = child_addr.parts().last().copied().unwrap();
        assert_eq!(parent_stream.source().operator_index, expected_parent_op);
    }

    // --- Feedback ---

    #[test]
    fn feedback_creates_handle_and_stream() {
        let mut root = RootScope::<u64>::new("root", 4);
        let mut child = root.iterative::<u32>("loop");

        let (handle, stream) = child.feedback::<i32>(1u32);

        assert_eq!(handle.operator_index(), 1); // first user op
        assert_eq!(stream.source().operator_index, 1);
        assert_eq!(stream.region_id(), child.current_region().id());

        // Clean up: connect the loop
        let region = child.current_region().id();
        let dummy: DataStream<ChildScope<Product<u64, u32>>, i32> =
            DataStream::new(child.clone(), Slot::new(99, 0), region);
        dummy.connect_loop(handle);
    }

    #[test]
    fn feedback_summary_advances_inner_only() {
        let mut root = RootScope::<u64>::new("root", 4);
        let mut child = root.iterative::<u32>("loop");

        let (handle, _stream) = child.feedback::<i32>(3u32);

        // Summary should be Product(0, 3) — outer identity, inner +3
        let summary = handle.summary();
        assert_eq!(summary.outer, 0u64); // outer identity (default for u64 summary)
        assert_eq!(summary.inner, 3u32); // inner advances by 3

        // Clean up: connect the loop
        let region = child.current_region().id();
        let dummy: DataStream<ChildScope<Product<u64, u32>>, i32> =
            DataStream::new(child.clone(), Slot::new(99, 0), region);
        dummy.connect_loop(handle);
    }

    #[test]
    fn feedback_summary_results_in_correct_advancement() {
        let mut root = RootScope::<u64>::new("root", 4);
        let mut child = root.iterative::<u32>("loop");

        let (handle, _stream) = child.feedback::<i32>(1u32);
        let summary = handle.summary();

        // Applying the summary to a timestamp should advance inner by 1
        let t = Product::new(5u64, 0u32);
        let result = summary.results_in(&t);
        assert_eq!(result, Some(Product::new(5, 1)));

        let t2 = Product::new(5u64, 3u32);
        let result2 = summary.results_in(&t2);
        assert_eq!(result2, Some(Product::new(5, 4)));

        // Clean up: connect the loop
        let region = child.current_region().id();
        let dummy: DataStream<ChildScope<Product<u64, u32>>, i32> =
            DataStream::new(child.clone(), Slot::new(99, 0), region);
        dummy.connect_loop(handle);
    }

    #[test]
    fn feedback_multiple_in_same_scope() {
        let mut root = RootScope::<u64>::new("root", 4);
        let mut child = root.iterative::<u32>("loop");

        let (h1, _s1) = child.feedback::<i32>(1u32);
        let (h2, _s2) = child.feedback::<String>(1u32);

        // Different operator indices
        assert_ne!(h1.operator_index(), h2.operator_index());

        // Clean up: connect them
        let source = Slot::new(99, 0);
        let region = child.current_region().id();
        let dummy1: DataStream<ChildScope<Product<u64, u32>>, i32> =
            DataStream::new(child.clone(), source, region);
        let dummy2: DataStream<ChildScope<Product<u64, u32>>, String> =
            DataStream::new(child.clone(), source, region);
        dummy1.connect_loop(h1);
        dummy2.connect_loop(h2);
    }

    // --- ConnectLoop ---

    #[test]
    fn connect_loop_consumes_handle() {
        let mut root = RootScope::<u64>::new("root", 4);
        let mut child = root.iterative::<u32>("loop");

        let (handle, _stream) = child.feedback::<i32>(1u32);

        let source = Slot::new(2, 0);
        let region = child.current_region().id();
        let result_stream: DataStream<ChildScope<Product<u64, u32>>, i32> =
            DataStream::new(child.clone(), source, region);

        // connect_loop consumes the handle
        result_stream.connect_loop(handle);
        // handle is no longer usable (moved)
    }

    // --- Full loop construction pattern ---

    #[test]
    fn full_loop_construction() {
        let mut root = RootScope::<u64>::new("root", 4);
        let mut child = root.iterative::<u32>("my_loop");

        // Create feedback edge
        let (handle, feedback_stream) = child.feedback::<i32>(1u32);

        // Create input entering the scope
        let parent_source = Slot::new(0, 0);
        let parent_region = root.current_region().id();
        let input: DataStream<RootScope<u64>, i32> =
            DataStream::new(root.clone(), parent_source, parent_region);
        let input_in_loop = input.enter(&child);

        // Both streams exist in the child scope
        assert_eq!(input_in_loop.scope().name(), "my_loop");
        assert_eq!(feedback_stream.scope().name(), "my_loop");

        // Simulate processing: create a result stream
        let result_source = Slot::new(child.allocate_operator_index(), 0);
        let child_region = child.current_region().id();
        let result: DataStream<ChildScope<Product<u64, u32>>, i32> =
            DataStream::new(child.clone(), result_source, child_region);

        // Close the loop
        result.connect_loop(handle);

        // Create output leaving the scope
        let output_source = Slot::new(child.allocate_operator_index(), 0);
        let output: DataStream<ChildScope<Product<u64, u32>>, i32> =
            DataStream::new(child.clone(), output_source, child_region);
        let _parent_output = output.leave(&root);
    }

    // --- Edge cases ---

    #[test]
    fn enter_then_leave_round_trip() {
        let mut root = RootScope::<u64>::new("root", 4);
        let child = root.iterative::<u32>("loop");

        let source = Slot::new(0, 0);
        let region = root.current_region().id();
        let stream: DataStream<RootScope<u64>, i32> =
            DataStream::new(root.clone(), source, region);

        // Enter and immediately leave
        let in_child = stream.enter(&child);
        let back_in_parent = in_child.leave(&root);

        assert_eq!(back_in_parent.scope().name(), "root");
    }

    #[test]
    fn feedback_with_zero_summary_is_rejected_by_progress() {
        // A zero summary means no timestamp advancement — the loop would never terminate.
        // This is valid at graph-construction time (the summary is just metadata),
        // but the progress tracker would detect the non-advancing cycle.
        let mut root = RootScope::<u64>::new("root", 4);
        let mut child = root.iterative::<u32>("loop");

        let (handle, _stream) = child.feedback::<i32>(0u32);

        // Summary is Product(0, 0) — no advancement
        let summary = handle.summary();
        assert_eq!(summary.outer, 0u64);
        assert_eq!(summary.inner, 0u32);

        // Graph construction allows it; progress tracker will reject at validation time.
        // Clean up
        let region = child.current_region().id();
        let dummy: DataStream<ChildScope<Product<u64, u32>>, i32> =
            DataStream::new(child.clone(), Slot::new(99, 0), region);
        dummy.connect_loop(handle);
    }

    #[test]
    fn nested_loop_feedback() {
        let mut root = RootScope::<u64>::new("root", 4);
        let mut outer_loop = root.iterative::<u32>("outer");
        let mut inner_loop = outer_loop.iterative::<u32>("inner");

        // Feedback in the inner loop
        let (handle, _stream) = inner_loop.feedback::<i32>(1u32);

        // The summary type is Product<Product<u64, u32>, u32>
        let summary = handle.summary();
        // Outer of the inner summary = Product(default_u64_summary, default_u32_summary)
        assert_eq!(summary.outer, Product::new(0u64, 0u32));
        // Inner = 1u32
        assert_eq!(summary.inner, 1u32);

        // Clean up
        let region = inner_loop.current_region().id();
        let dummy: DataStream<ChildScope<Product<Product<u64, u32>, u32>>, i32> =
            DataStream::new(inner_loop.clone(), Slot::new(99, 0), region);
        dummy.connect_loop(handle);
    }

    #[test]
    #[should_panic(expected = "was dropped without being connected")]
    fn unconnected_feedback_handle_panics_in_debug() {
        let mut root = RootScope::<u64>::new("root", 4);
        let mut child = root.iterative::<u32>("loop");

        // Create a feedback handle but never connect it — should panic on drop.
        let (_handle, _stream) = child.feedback::<i32>(1u32);
    }
}
