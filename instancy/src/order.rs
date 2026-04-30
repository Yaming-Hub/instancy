//! Partial ordering traits.
//!
//! Extends Rust's `PartialOrd` with an explicit `less_equal` method that
//! distinguishes "less-or-equal in the partial order" from "uncomparable".
//! This is the foundation for frontier and progress tracking.

use std::fmt::Debug;

/// A type with a well-defined partial order.
///
/// Unlike `PartialOrd`, which returns `Option<Ordering>`, this trait provides
/// explicit boolean methods for the partial order relationship. This is important
/// for timestamp comparisons where two values may be incomparable.
pub trait PartialOrder: PartialEq {
    /// Returns `true` if `self` is less than or equal to `other` in the partial order.
    fn less_equal(&self, other: &Self) -> bool;

    /// Returns `true` if `self` is strictly less than `other` in the partial order.
    fn less_than(&self, other: &Self) -> bool {
        self.less_equal(other) && self != other
    }
}

/// Marker trait for types whose partial order is a total order.
///
/// When `TotalOrder` is implemented, `less_equal` is equivalent to `<=`
/// and every pair of elements is comparable.
pub trait TotalOrder: PartialOrder + Ord {}

// --- Implementations for primitive types ---

macro_rules! impl_total_order {
    ($($t:ty),*) => {
        $(
            impl PartialOrder for $t {
                #[inline]
                fn less_equal(&self, other: &Self) -> bool {
                    *self <= *other
                }
            }

            impl TotalOrder for $t {}
        )*
    };
}

impl_total_order!((), usize, u32, u64, i32, i64);

// --- Product type for nested timestamps ---

/// A pair of timestamps used for nested scopes.
///
/// The partial order is component-wise: `(a1, b1) <= (a2, b2)` iff `a1 <= a2` AND `b1 <= b2`.
/// This means two products can be incomparable (e.g., `(1, 3)` and `(2, 2)`).
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct Product<TOuter, TInner> {
    /// The outer (enclosing scope) component.
    pub outer: TOuter,
    /// The inner (nested scope) component.
    pub inner: TInner,
}

impl<TOuter, TInner> Product<TOuter, TInner> {
    /// Create a new product timestamp.
    pub fn new(outer: TOuter, inner: TInner) -> Self {
        Self { outer, inner }
    }
}

impl<TOuter: PartialOrder, TInner: PartialOrder> PartialOrder for Product<TOuter, TInner> {
    #[inline]
    fn less_equal(&self, other: &Self) -> bool {
        self.outer.less_equal(&other.outer) && self.inner.less_equal(&other.inner)
    }
}

// Lexicographic Ord for Product — compatible with the partial order
// (if a.less_equal(b) then a <= b, but not vice versa).
impl<TOuter: Ord, TInner: Ord> Ord for Product<TOuter, TInner> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.outer
            .cmp(&other.outer)
            .then_with(|| self.inner.cmp(&other.inner))
    }
}

impl<TOuter: PartialOrd, TInner: PartialOrd> PartialOrd for Product<TOuter, TInner> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.outer.partial_cmp(&other.outer) {
            Some(std::cmp::Ordering::Equal) => self.inner.partial_cmp(&other.inner),
            other_cmp => other_cmp,
        }
    }
}

impl<TOuter: Debug, TInner: Debug> std::fmt::Display for Product<TOuter, TInner> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({:?}, {:?})", self.outer, self.inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- PartialOrder property tests ---

    fn assert_reflexive<T: PartialOrder + Debug>(values: &[T]) {
        for v in values {
            assert!(v.less_equal(v), "reflexivity failed for {:?}", v);
        }
    }

    fn assert_antisymmetric<T: PartialOrder + Debug>(pairs: &[(T, T)]) {
        for (a, b) in pairs {
            if a.less_equal(b) && b.less_equal(a) {
                assert_eq!(a, b, "antisymmetry failed for {:?} and {:?}", a, b);
            }
        }
    }

    fn assert_transitive<T: PartialOrder + Debug>(triples: &[(T, T, T)]) {
        for (a, b, c) in triples {
            if a.less_equal(b) && b.less_equal(c) {
                assert!(
                    a.less_equal(c),
                    "transitivity failed: {:?} <= {:?} <= {:?} but not {:?} <= {:?}",
                    a,
                    b,
                    c,
                    a,
                    c
                );
            }
        }
    }

    // --- Unit type ---

    #[test]
    fn unit_partial_order() {
        assert!(().less_equal(&()));
        assert!(!().less_than(&()));
    }

    // --- Primitive types ---

    #[test]
    fn u64_partial_order_properties() {
        let values: Vec<u64> = vec![0, 1, 2, 5, u64::MAX];
        assert_reflexive(&values);

        let pairs: Vec<(u64, u64)> = values
            .iter()
            .flat_map(|a| values.iter().map(move |b| (*a, *b)))
            .collect();
        assert_antisymmetric(&pairs);

        let triples: Vec<(u64, u64, u64)> = vec![
            (0, 1, 2),
            (0, 0, 0),
            (1, 5, u64::MAX),
            (0, u64::MAX, u64::MAX),
        ];
        assert_transitive(&triples);
    }

    #[test]
    fn u64_less_than() {
        assert!(0u64.less_than(&1));
        assert!(!1u64.less_than(&1));
        assert!(!2u64.less_than(&1));
    }

    #[test]
    fn i32_partial_order() {
        assert!((-1i32).less_equal(&0));
        assert!((-1i32).less_than(&0));
        assert!(!0i32.less_than(&-1));
    }

    // --- Product type ---

    #[test]
    fn product_comparable_pairs() {
        let a = Product::new(1u64, 2u64);
        let b = Product::new(2u64, 3u64);
        assert!(a.less_equal(&b));
        assert!(a.less_than(&b));
        assert!(!b.less_equal(&a));
    }

    #[test]
    fn product_incomparable_pairs() {
        // (1, 3) and (2, 2): 1 <= 2 but 3 > 2, so incomparable
        let a = Product::new(1u64, 3u64);
        let b = Product::new(2u64, 2u64);
        assert!(!a.less_equal(&b));
        assert!(!b.less_equal(&a));
    }

    #[test]
    fn product_equal() {
        let a = Product::new(5u32, 10u32);
        let b = Product::new(5u32, 10u32);
        assert!(a.less_equal(&b));
        assert!(!a.less_than(&b));
        assert_eq!(a, b);
    }

    #[test]
    fn product_reflexivity() {
        let values = vec![
            Product::new(0u64, 0u64),
            Product::new(1, 0),
            Product::new(0, 1),
            Product::new(5, 5),
        ];
        assert_reflexive(&values);
    }

    #[test]
    fn product_transitivity() {
        let triples = vec![
            (
                Product::new(0u64, 0u64),
                Product::new(1, 1),
                Product::new(2, 2),
            ),
            (
                Product::new(1, 1),
                Product::new(1, 1),
                Product::new(1, 1),
            ),
            (
                Product::new(0, 0),
                Product::new(0, 5),
                Product::new(0, 10),
            ),
        ];
        assert_transitive(&triples);
    }

    #[test]
    fn product_ord_is_lexicographic() {
        use std::cmp::Ordering;

        let a = Product::new(1u64, 3u64);
        let b = Product::new(2u64, 1u64);
        // Lexicographic: outer 1 < 2, so a < b regardless of inner
        assert_eq!(a.cmp(&b), Ordering::Less);

        let c = Product::new(2u64, 1u64);
        let d = Product::new(2u64, 3u64);
        // Same outer, compare inner: 1 < 3
        assert_eq!(c.cmp(&d), Ordering::Less);
    }

    #[test]
    fn product_partial_order_implies_ord() {
        // If a.less_equal(b), then a <= b in Ord.
        // The reverse need not hold (Ord may compare incomparable elements).
        let a = Product::new(1u64, 2u64);
        let b = Product::new(2u64, 3u64);
        assert!(a.less_equal(&b));
        assert!(a <= b);

        // Incomparable in partial order but comparable in Ord
        let c = Product::new(1u64, 3u64);
        let d = Product::new(2u64, 2u64);
        assert!(!c.less_equal(&d));
        assert!(!d.less_equal(&c));
        // Ord still gives a result (lexicographic)
        assert!(c < d); // 1 < 2 in outer
    }

    // --- TotalOrder ---

    #[test]
    fn total_order_primitives() {
        fn assert_total<T: TotalOrder>() {}
        assert_total::<()>();
        assert_total::<usize>();
        assert_total::<u32>();
        assert_total::<u64>();
        assert_total::<i32>();
        assert_total::<i64>();
    }

    // Product is NOT TotalOrder — this is a compile-time guarantee.
    // No impl exists, so Product<u64, u64>: TotalOrder would fail to compile.
}
