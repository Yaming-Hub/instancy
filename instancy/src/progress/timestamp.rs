//! Timestamps and path summaries for progress tracking.
//!
//! A `Timestamp` represents a logical time in a dataflow computation.
//! A `PathSummary` describes how timestamps advance along dataflow edges.

use std::fmt::Debug;

use crate::order::{PartialOrder, Product};

/// A logical time in a dataflow computation.
///
/// Timestamps must be partially ordered (for progress tracking), totally ordered
/// (for canonical sorting in antichains and change batches), cloneable, and safe
/// to send across async task boundaries.
pub trait Timestamp:
    Clone + Eq + PartialOrder + Ord + Debug + Default + Send + Sync + 'static
{
    /// The type of path summary that describes how this timestamp advances.
    type Summary: PathSummary<Self> + Send + Sync + 'static;

    /// Returns the smallest possible timestamp value.
    fn minimum() -> Self;
}

/// Describes how a timestamp advances along a dataflow edge.
///
/// Path summaries compose: if edge A has summary `s1` and edge B has summary `s2`,
/// the composed summary for the path A→B is `s1.followed_by(&s2)`.
///
/// # Laws
///
/// 1. **Identity**: `Default::default().results_in(&t) == Some(t)` for all `t`.
/// 2. **Composition**: `s1.followed_by(&s2).results_in(&t) == s1.results_in(&t).and_then(|x| s2.results_in(&x))`.
/// 3. **Monotonicity**: If `t1.less_equal(&t2)` and both `s.results_in(&t1)` and
///    `s.results_in(&t2)` are `Some`, then `s.results_in(&t1).unwrap().less_equal(&s.results_in(&t2).unwrap())`.
pub trait PathSummary<T: Timestamp>:
    Clone + Eq + PartialOrder + Debug + Default + Send + Sync + 'static
{
    /// Applies this summary to a timestamp, returning the resulting timestamp.
    ///
    /// Returns `None` if the result would overflow or is otherwise invalid.
    fn results_in(&self, src: &T) -> Option<T>;

    /// Composes this summary with another, returning the combined summary.
    ///
    /// Returns `None` if the composition would overflow or is invalid.
    fn followed_by(&self, other: &Self) -> Option<Self>;
}

// --- Implementations for () ---

impl Timestamp for () {
    type Summary = ();

    fn minimum() -> Self {}
}

impl PathSummary<()> for () {
    fn results_in(&self, _src: &()) -> Option<()> {
        Some(())
    }

    fn followed_by(&self, _other: &()) -> Option<()> {
        Some(())
    }
}

// --- Macro for integer timestamps ---

macro_rules! impl_integer_timestamp {
    ($($t:ty),*) => {
        $(
            impl Timestamp for $t {
                type Summary = $t;

                fn minimum() -> Self {
                    <$t>::MIN
                }
            }

            impl PathSummary<$t> for $t {
                fn results_in(&self, src: &$t) -> Option<$t> {
                    src.checked_add(*self)
                }

                fn followed_by(&self, other: &$t) -> Option<$t> {
                    self.checked_add(*other)
                }
            }
        )*
    };
}

impl_integer_timestamp!(usize, u32, u64, i32, i64);

// --- Product timestamp for nested scopes ---

impl<TOuter, TInner> Timestamp for Product<TOuter, TInner>
where
    TOuter: Timestamp,
    TInner: Timestamp,
{
    type Summary = Product<TOuter::Summary, TInner::Summary>;

    fn minimum() -> Self {
        Product::new(TOuter::minimum(), TInner::minimum())
    }
}

impl<TOuter, TInner, TSummaryOuter, TSummaryInner>
    PathSummary<Product<TOuter, TInner>> for Product<TSummaryOuter, TSummaryInner>
where
    TOuter: Timestamp<Summary = TSummaryOuter>,
    TInner: Timestamp<Summary = TSummaryInner>,
    TSummaryOuter: PathSummary<TOuter> + Send + Sync + 'static,
    TSummaryInner: PathSummary<TInner> + Send + Sync + 'static,
{
    fn results_in(&self, src: &Product<TOuter, TInner>) -> Option<Product<TOuter, TInner>> {
        let outer = self.outer.results_in(&src.outer)?;
        let inner = self.inner.results_in(&src.inner)?;
        Some(Product::new(outer, inner))
    }

    fn followed_by(&self, other: &Self) -> Option<Self> {
        let outer = self.outer.followed_by(&other.outer)?;
        let inner = self.inner.followed_by(&other.inner)?;
        Some(Product::new(outer, inner))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Helper: verify PathSummary laws ---

    fn assert_identity_law<T: Timestamp>()
    where
        T::Summary: Default,
    {
        let default_summary = T::Summary::default();
        let timestamps = [T::minimum()];
        for t in &timestamps {
            assert_eq!(
                default_summary.results_in(t),
                Some(t.clone()),
                "identity law failed for {:?}",
                t
            );
        }
    }

    fn assert_composition_law<T: Timestamp>(s1: &T::Summary, s2: &T::Summary, t: &T) {
        let composed = s1.followed_by(s2);
        let sequential = s1.results_in(t).and_then(|mid| s2.results_in(&mid));
        match composed {
            Some(ref c) => assert_eq!(
                c.results_in(t),
                sequential,
                "composition law failed for s1={:?}, s2={:?}, t={:?}",
                s1,
                s2,
                t
            ),
            None => {
                // If composition overflows, sequential should also overflow
                // (or the composed summary is None, which is fine)
            }
        }
    }

    fn assert_monotonicity<T: Timestamp>(summary: &T::Summary, t1: &T, t2: &T) {
        if t1.less_equal(t2) {
            if let (Some(r1), Some(r2)) = (summary.results_in(t1), summary.results_in(t2)) {
                assert!(
                    r1.less_equal(&r2),
                    "monotonicity failed: summary {:?} applied to {:?} gave {:?}, applied to {:?} gave {:?}",
                    summary, t1, r1, t2, r2
                );
            }
        }
    }

    // --- Unit timestamp ---

    #[test]
    fn unit_timestamp() {
        assert_eq!(<()>::minimum(), ());
        assert_identity_law::<()>();
    }

    #[test]
    fn unit_path_summary() {
        assert_eq!(().results_in(&()), Some(()));
        assert_eq!(().followed_by(&()), Some(()));
    }

    // --- Integer timestamps ---

    #[test]
    fn u64_minimum() {
        assert_eq!(u64::minimum(), 0);
    }

    #[test]
    fn u64_identity_law() {
        assert_identity_law::<u64>();
        // Also test non-minimum values
        let default_summary: u64 = Default::default(); // 0
        assert_eq!(default_summary.results_in(&42u64), Some(42));
        assert_eq!(default_summary.results_in(&u64::MAX), Some(u64::MAX));
    }

    #[test]
    fn u64_results_in() {
        assert_eq!(5u64.results_in(&10), Some(15));
        assert_eq!(1u64.results_in(&0), Some(1));
        assert_eq!(0u64.results_in(&100), Some(100));
    }

    #[test]
    fn u64_results_in_overflow() {
        assert_eq!(1u64.results_in(&u64::MAX), None);
        assert_eq!(u64::MAX.results_in(&1), None);
    }

    #[test]
    fn u64_followed_by() {
        assert_eq!(3u64.followed_by(&7), Some(10));
        assert_eq!(0u64.followed_by(&0), Some(0));
    }

    #[test]
    fn u64_followed_by_overflow() {
        assert_eq!(u64::MAX.followed_by(&1), None);
    }

    #[test]
    fn u64_composition_law() {
        assert_composition_law::<u64>(&3, &7, &10);
        assert_composition_law::<u64>(&0, &0, &0);
        assert_composition_law::<u64>(&1, &1, &(u64::MAX - 2));
    }

    #[test]
    fn u64_monotonicity() {
        assert_monotonicity::<u64>(&5, &0, &10);
        assert_monotonicity::<u64>(&5, &10, &10);
        assert_monotonicity::<u64>(&0, &0, &u64::MAX);
    }

    #[test]
    fn i32_minimum() {
        assert_eq!(i32::minimum(), i32::MIN);
    }

    #[test]
    fn i32_results_in() {
        assert_eq!(5i32.results_in(&10), Some(15));
        assert_eq!((-3i32).results_in(&10), Some(7));
    }

    #[test]
    fn i32_overflow() {
        assert_eq!(1i32.results_in(&i32::MAX), None);
        assert_eq!((-1i32).results_in(&i32::MIN), None);
    }

    #[test]
    fn usize_timestamp() {
        assert_eq!(usize::minimum(), 0);
        assert_eq!(5usize.results_in(&10), Some(15));
        assert_eq!(1usize.results_in(&usize::MAX), None);
    }

    #[test]
    fn u32_timestamp() {
        assert_eq!(u32::minimum(), 0);
        assert_eq!(5u32.results_in(&10), Some(15));
        assert_eq!(1u32.results_in(&u32::MAX), None);
    }

    // --- Product timestamp ---

    #[test]
    fn product_minimum() {
        let min = <Product<u64, u32>>::minimum();
        assert_eq!(min.outer, 0u64);
        assert_eq!(min.inner, 0u32);
    }

    #[test]
    fn product_identity_law() {
        let default_summary: Product<u64, u32> = Default::default();
        let t = Product::new(5u64, 3u32);
        assert_eq!(default_summary.results_in(&t), Some(t));
    }

    #[test]
    fn product_results_in() {
        let summary = Product::new(1u64, 2u32);
        let t = Product::new(10u64, 5u32);
        assert_eq!(summary.results_in(&t), Some(Product::new(11, 7)));
    }

    #[test]
    fn product_results_in_overflow_outer() {
        let summary = Product::new(1u64, 0u32);
        let t = Product::new(u64::MAX, 5u32);
        assert_eq!(summary.results_in(&t), None);
    }

    #[test]
    fn product_results_in_overflow_inner() {
        let summary = Product::new(0u64, 1u32);
        let t = Product::new(5u64, u32::MAX);
        assert_eq!(summary.results_in(&t), None);
    }

    #[test]
    fn product_followed_by() {
        let s1 = Product::new(1u64, 2u64);
        let s2 = Product::new(3u64, 4u64);
        assert_eq!(s1.followed_by(&s2), Some(Product::new(4, 6)));
    }

    #[test]
    fn product_followed_by_overflow() {
        let s1 = Product::new(u64::MAX, 0u64);
        let s2 = Product::new(1u64, 0u64);
        assert_eq!(s1.followed_by(&s2), None);
    }

    #[test]
    fn product_composition_law() {
        let s1 = Product::new(1u64, 2u64);
        let s2 = Product::new(3u64, 4u64);
        let t = Product::new(10u64, 20u64);
        assert_composition_law::<Product<u64, u64>>(&s1, &s2, &t);
    }

    #[test]
    fn product_monotonicity() {
        let summary = Product::new(1u64, 1u64);
        let t1 = Product::new(1u64, 1u64);
        let t2 = Product::new(2u64, 3u64);
        assert_monotonicity::<Product<u64, u64>>(&summary, &t1, &t2);
    }

    // --- Send + Sync bounds ---

    #[test]
    fn timestamp_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<u64>();
        assert_send_sync::<Product<u64, u32>>();
    }

    #[test]
    fn path_summary_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<<u64 as Timestamp>::Summary>();
        assert_send_sync::<<Product<u64, u32> as Timestamp>::Summary>();
    }
}
