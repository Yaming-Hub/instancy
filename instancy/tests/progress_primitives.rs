//! Regression tests for progress-tracking primitives exposed publicly.

use instancy::progress::change_batch::ChangeBatch;

/// A lone zero-valued update used to survive compaction (the merge loop is
/// guarded by `len() > 1`), so `is_empty()` was false and `iter()` yielded
/// `(k, 0)`, contradicting "removing entries whose accumulated value is zero".
#[test]
fn change_batch_lone_zero_update_is_empty() {
    let mut b: ChangeBatch<u64> = ChangeBatch::new();
    b.update(5, 0);
    let items: Vec<(u64, i64)> = b.iter().cloned().collect();
    assert!(items.is_empty(), "iter yielded {items:?}");
    assert!(b.is_empty());
}

/// Control: zero updates mixed with live ones, and cancelling pairs, compact
/// to exactly the non-zero totals.
#[test]
fn change_batch_zero_updates_mixed_with_live_ones() {
    let mut b: ChangeBatch<u64> = ChangeBatch::new();
    b.update(1, 0);
    b.update(2, 3);
    b.update(3, 1);
    b.update(3, -1);
    b.update(4, 0);
    assert_eq!(b.iter().cloned().collect::<Vec<_>>(), vec![(2, 3)]);
    assert!(!b.is_empty());
    b.update(2, -3);
    assert!(b.is_empty());
    assert_eq!(b.len(), 0);
}
