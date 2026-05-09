//! Pluggable scheduling policies for the task queue.
//!
//! The [`SchedulePolicy`] trait determines the dequeue order for operator
//! activation tasks. Different policies trade off latency, throughput, and
//! fairness. See §12.7 in DESIGN.md.

use std::cmp::Ordering;
use std::time::Instant;

use crate::dataflow::id::DataflowId;

/// Metadata attached to each queued operator activation task.
///
/// Used by [`SchedulePolicy`] to determine scheduling order.
#[derive(Debug, Clone)]
pub struct TaskMeta {
    /// The dataflow this task belongs to.
    pub dataflow_id: DataflowId,
    /// Priority inherited from the dataflow (higher = scheduled sooner).
    pub priority: u32,
    /// Wall-clock time when this task was enqueued.
    pub created_at: Instant,
}

impl TaskMeta {
    /// Create new task metadata.
    pub fn new(dataflow_id: DataflowId, priority: u32) -> Self {
        Self {
            dataflow_id,
            priority,
            created_at: Instant::now(),
        }
    }
}

/// Determines task ordering in the queue.
///
/// The scheduler compares two tasks and returns which should run first.
/// Implementations can use priority, age, or any combination.
///
/// **When no policy is set** (the default), the scheduler uses a plain FIFO
/// queue with O(1) pop — no comparisons at all. Only set a policy if you need
/// priority-based or custom ordering.
///
/// **When a policy is set**, the scheduler uses a `BinaryHeap` for O(log n)
/// insert and dequeue. This is correct because task metadata (priority,
/// created_at) is stable while in the heap — `mark_enqueued()` only updates
/// `created_at` at insertion time, before the entry enters the heap.
///
/// Returns `Ordering::Less` if `a` should be scheduled before `b`.
pub trait SchedulePolicy: Send + Sync {
    /// Compare two tasks for scheduling order.
    ///
    /// Returns `Ordering::Less` if `a` should run before `b`.
    fn compare(&self, a: &TaskMeta, b: &TaskMeta) -> Ordering;
}

// ---------------------------------------------------------------------------
// FifoPolicy — pure FIFO ordering
// ---------------------------------------------------------------------------

/// Pure FIFO scheduling — tasks run in creation order regardless of priority.
///
/// Simple and fair, but cannot differentiate interactive vs batch workloads.
#[derive(Debug, Clone, Default)]
pub struct FifoPolicy;

impl SchedulePolicy for FifoPolicy {
    fn compare(&self, a: &TaskMeta, b: &TaskMeta) -> Ordering {
        // Earlier created_at → scheduled first
        a.created_at.cmp(&b.created_at)
    }
}

// ---------------------------------------------------------------------------
// PriorityPolicy — strict priority ordering
// ---------------------------------------------------------------------------

/// Strict priority scheduling — higher priority always wins.
///
/// Risk of starvation for low-priority tasks if high-priority work is constant.
#[derive(Debug, Clone, Default)]
pub struct PriorityPolicy;

impl SchedulePolicy for PriorityPolicy {
    fn compare(&self, a: &TaskMeta, b: &TaskMeta) -> Ordering {
        // Higher priority → scheduled first (reverse order)
        match b.priority.cmp(&a.priority) {
            Ordering::Equal => a.created_at.cmp(&b.created_at), // tie-break: FIFO
            ord => ord,
        }
    }
}

// ---------------------------------------------------------------------------
// PriorityWithAgingPolicy — priority with aging to prevent starvation
// ---------------------------------------------------------------------------

/// Priority scheduling with aging to prevent starvation.
///
/// Tasks gain effective priority as they wait. Even priority-0 tasks will
/// eventually run as their effective priority grows with wait time.
///
/// This is the **default** scheduling policy.
#[derive(Debug, Clone)]
pub struct PriorityWithAgingPolicy {
    /// How much effective priority a task gains per second of waiting.
    /// Default: 1 priority level per 10 seconds.
    pub aging_rate: f64,
}

impl Default for PriorityWithAgingPolicy {
    fn default() -> Self {
        Self { aging_rate: 0.1 }
    }
}

impl SchedulePolicy for PriorityWithAgingPolicy {
    fn compare(&self, a: &TaskMeta, b: &TaskMeta) -> Ordering {
        // Effective priority = base_priority + wait_time * aging_rate.
        // The ordering is stable because both priority and created_at are fixed
        // while a task is in the heap. The age difference between two tasks is
        // constant regardless of when compare is called:
        //   (a.age - b.age) = (b.created_at - a.created_at), always the same.
        let now = Instant::now();
        let score_a =
            a.priority as f64 + now.duration_since(a.created_at).as_secs_f64() * self.aging_rate;
        let score_b =
            b.priority as f64 + now.duration_since(b.created_at).as_secs_f64() * self.aging_rate;

        // Higher effective priority → scheduled first
        score_b.partial_cmp(&score_a).unwrap_or(Ordering::Equal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    fn make_meta(priority: u32) -> TaskMeta {
        TaskMeta {
            dataflow_id: DataflowId::new(),
            priority,
            created_at: Instant::now(),
        }
    }

    #[test]
    fn fifo_orders_by_creation_time() {
        let policy = FifoPolicy;
        let a = make_meta(100);
        thread::sleep(Duration::from_millis(1));
        let b = make_meta(0);

        // a was created first → a before b
        assert_eq!(policy.compare(&a, &b), Ordering::Less);
    }

    #[test]
    fn priority_orders_by_priority() {
        let policy = PriorityPolicy;
        let low = make_meta(1);
        let high = make_meta(100);

        // high priority → scheduled first
        assert_eq!(policy.compare(&high, &low), Ordering::Less);
        assert_eq!(policy.compare(&low, &high), Ordering::Greater);
    }

    #[test]
    fn priority_fifo_tiebreak() {
        let policy = PriorityPolicy;
        let a = make_meta(50);
        thread::sleep(Duration::from_millis(1));
        let b = make_meta(50);

        // Same priority → FIFO (a first)
        assert_eq!(policy.compare(&a, &b), Ordering::Less);
    }

    #[test]
    fn aging_eventually_promotes_low_priority() {
        let policy = PriorityWithAgingPolicy { aging_rate: 1000.0 };

        // Create a low-priority task that's been waiting
        let old_low = TaskMeta {
            dataflow_id: DataflowId::new(),
            priority: 0,
            created_at: Instant::now() - Duration::from_secs(1),
        };
        // Create a high-priority task that just arrived
        let new_high = make_meta(100);

        // With aging_rate=1000, 1 second of age = 1000 effective priority
        // old_low effective = 0 + 1000 = 1000
        // new_high effective = 100 + ~0 = 100
        // So old_low should be scheduled first
        assert_eq!(policy.compare(&old_low, &new_high), Ordering::Less);
    }

    #[test]
    fn default_aging_policy_respects_priority_for_fresh_tasks() {
        let policy = PriorityWithAgingPolicy::default();
        let low = make_meta(1);
        let high = make_meta(100);

        // Both fresh → priority dominates
        assert_eq!(policy.compare(&high, &low), Ordering::Less);
    }
}
