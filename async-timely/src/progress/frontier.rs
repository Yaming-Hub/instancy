//! Antichain — a minimal set of mutually incomparable elements.
//!
//! An `Antichain<T>` maintains a set of elements from a partial order such that
//! no element is less than or equal to any other element in the set. This is used
//! to represent frontiers in progress tracking.

use crate::order::{PartialOrder, TotalOrder};

/// A set of mutually incomparable elements of a partial order.
///
/// An antichain maintains the *minimal* elements: inserting an element that is
/// greater than or equal to an existing element is a no-op, and inserting an
/// element that is less than existing elements evicts those existing elements.
///
/// Two antichains are equal if they contain the same set of elements,
/// regardless of internal ordering.
#[derive(Clone, Debug)]
pub struct Antichain<T> {
    elements: Vec<T>,
}

impl<T> Antichain<T> {
    /// Creates a new empty antichain.
    pub fn new() -> Self {
        Antichain {
            elements: Vec::new(),
        }
    }

    /// Creates a new singleton antichain containing one element.
    pub fn from_elem(element: T) -> Self {
        Antichain {
            elements: vec![element],
        }
    }

    /// Creates an antichain from an iterator of elements, filtering to keep only minimal ones.
    pub fn from_elem_iter(iter: impl IntoIterator<Item = T>) -> Self
    where
        T: PartialOrder,
    {
        iter.into_iter().collect()
    }

    /// Returns a slice of the elements in the antichain.
    #[inline]
    pub fn elements(&self) -> &[T] {
        &self.elements
    }

    /// Returns `true` if the antichain contains no elements.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    /// Returns the number of elements in the antichain.
    #[inline]
    pub fn len(&self) -> usize {
        self.elements.len()
    }

    /// Removes all elements from the antichain.
    pub fn clear(&mut self) {
        self.elements.clear();
    }
}

impl<T: PartialOrder> Antichain<T> {
    /// Inserts an element into the antichain.
    ///
    /// The element is added only if no existing element is less than or equal to it.
    /// If the element is added, any existing elements that are greater than or equal
    /// to the new element are removed.
    ///
    /// Returns `true` if the element was added.
    pub fn insert(&mut self, element: T) -> bool {
        if self.elements.iter().any(|x| x.less_equal(&element)) {
            return false;
        }
        self.elements.retain(|x| !element.less_equal(x));
        self.elements.push(element);
        true
    }

    /// Inserts an element by reference, cloning if needed.
    pub fn insert_ref(&mut self, element: &T) -> bool
    where
        T: Clone,
    {
        if self.elements.iter().any(|x| x.less_equal(element)) {
            return false;
        }
        self.elements.retain(|x| !element.less_equal(x));
        self.elements.push(element.clone());
        true
    }

    /// Returns `true` if any element in the antichain is strictly less than `time`.
    #[inline]
    pub fn less_than(&self, time: &T) -> bool {
        self.elements.iter().any(|x| x.less_than(time))
    }

    /// Returns `true` if any element in the antichain is less than or equal to `time`.
    #[inline]
    pub fn less_equal(&self, time: &T) -> bool {
        self.elements.iter().any(|x| x.less_equal(time))
    }
}

impl<T: PartialOrder> Extend<T> for Antichain<T> {
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        for element in iter {
            self.insert(element);
        }
    }
}

impl<T: PartialOrder> FromIterator<T> for Antichain<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut antichain = Antichain::new();
        antichain.extend(iter);
        antichain
    }
}

impl<T: PartialOrder> From<Vec<T>> for Antichain<T> {
    fn from(vec: Vec<T>) -> Self {
        vec.into_iter().collect()
    }
}

impl<T> Default for Antichain<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: PartialEq> PartialEq for Antichain<T> {
    fn eq(&self, other: &Self) -> bool {
        self.elements.len() == other.elements.len()
            && (self
                .elements
                .iter()
                .zip(other.elements.iter())
                .all(|(a, b)| a == b)
                || self
                    .elements
                    .iter()
                    .all(|a| other.elements.iter().any(|b| a == b)))
    }
}

impl<T: Eq> Eq for Antichain<T> {}

impl<T: PartialOrder> PartialOrder for Antichain<T> {
    /// Antichain A ≤ Antichain B iff every element of B has some element of A ≤ it.
    fn less_equal(&self, other: &Self) -> bool {
        other
            .elements
            .iter()
            .all(|b| self.elements.iter().any(|a| a.less_equal(b)))
    }
}

impl<T: TotalOrder> Antichain<T> {
    /// For a totally ordered type, the antichain has at most one element.
    /// Returns `Some(&element)` if non-empty.
    pub fn as_option(&self) -> Option<&T> {
        debug_assert!(self.len() <= 1);
        self.elements.last()
    }

    /// Converts a total-order antichain into its single element, if present.
    pub fn into_option(mut self) -> Option<T> {
        debug_assert!(self.len() <= 1);
        self.elements.pop()
    }
}

impl<T> IntoIterator for Antichain<T> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.elements.into_iter()
    }
}

impl<'a, T> IntoIterator for &'a Antichain<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.elements.iter()
    }
}

impl<T> std::ops::Deref for Antichain<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        &self.elements
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::order::Product;

    // --- Basic operations ---

    #[test]
    fn empty_antichain() {
        let ac = Antichain::<u64>::new();
        assert!(ac.is_empty());
        assert_eq!(ac.len(), 0);
        assert_eq!(ac.elements(), &[]);
    }

    #[test]
    fn singleton() {
        let ac = Antichain::from_elem(5u64);
        assert!(!ac.is_empty());
        assert_eq!(ac.len(), 1);
        assert_eq!(ac.elements(), &[5]);
    }

    #[test]
    fn insert_smaller_evicts_larger() {
        let mut ac = Antichain::from_elem(5u64);
        assert!(ac.insert(3));
        assert_eq!(ac.elements(), &[3]);
    }

    #[test]
    fn insert_larger_is_noop() {
        let mut ac = Antichain::from_elem(3u64);
        assert!(!ac.insert(5));
        assert_eq!(ac.elements(), &[3]);
    }

    #[test]
    fn insert_equal_is_noop() {
        let mut ac = Antichain::from_elem(3u64);
        assert!(!ac.insert(3));
        assert_eq!(ac.elements(), &[3]);
    }

    #[test]
    fn insert_incomparable_keeps_both() {
        let mut ac = Antichain::new();
        ac.insert(Product::new(1u64, 3u64));
        ac.insert(Product::new(2u64, 2u64));
        assert_eq!(ac.len(), 2);
    }

    #[test]
    fn insert_dominated_by_existing_incomparable() {
        let mut ac: Antichain<Product<u64, u64>> = Antichain::new();
        ac.insert(Product::new(1, 1));
        // (2, 2) is greater than (1, 1) in product order
        assert!(!ac.insert(Product::new(2, 2)));
        assert_eq!(ac.len(), 1);
    }

    #[test]
    fn insert_evicts_multiple() {
        let mut ac = Antichain::new();
        ac.insert(Product::new(2u64, 1u64));
        ac.insert(Product::new(1u64, 2u64));
        assert_eq!(ac.len(), 2);

        // (1, 1) is less than both (2, 1) and (1, 2), should evict both
        ac.insert(Product::new(1u64, 1u64));
        assert_eq!(ac.len(), 1);
        assert_eq!(ac.elements(), &[Product::new(1, 1)]);
    }

    #[test]
    fn insert_ref_works() {
        let mut ac = Antichain::<u64>::new();
        assert!(ac.insert_ref(&5));
        assert!(!ac.insert_ref(&7));
        assert!(ac.insert_ref(&3));
        assert_eq!(ac.elements(), &[3]);
    }

    // --- less_than / less_equal ---

    #[test]
    fn less_than_and_less_equal() {
        let ac = Antichain::from_elem(5u64);
        assert!(!ac.less_than(&4));
        assert!(!ac.less_than(&5));
        assert!(ac.less_than(&6));

        assert!(!ac.less_equal(&4));
        assert!(ac.less_equal(&5));
        assert!(ac.less_equal(&6));
    }

    #[test]
    fn empty_antichain_not_less_than_anything() {
        let ac = Antichain::<u64>::new();
        assert!(!ac.less_than(&0));
        assert!(!ac.less_than(&u64::MAX));
        assert!(!ac.less_equal(&0));
    }

    // --- Equality ---

    #[test]
    fn equality_same_elements_different_order() {
        let a: Antichain<Product<u64, u64>> =
            vec![Product::new(1, 3), Product::new(3, 1)].into();
        let b: Antichain<Product<u64, u64>> =
            vec![Product::new(3, 1), Product::new(1, 3)].into();
        assert_eq!(a, b);
    }

    #[test]
    fn inequality_different_elements() {
        let a = Antichain::from_elem(1u64);
        let b = Antichain::from_elem(2u64);
        assert_ne!(a, b);
    }

    // --- PartialOrder on Antichain ---

    #[test]
    fn antichain_partial_order() {
        let a = Antichain::from_elem(1u64);
        let b = Antichain::from_elem(2u64);
        // {1} ≤ {2} because for every element in {2} (which is 2), there's an element in {1} (which is 1) ≤ 2
        assert!(PartialOrder::less_equal(&a, &b));
        assert!(!PartialOrder::less_equal(&b, &a));
    }

    #[test]
    fn antichain_partial_order_equal() {
        let a = Antichain::from_elem(3u64);
        let b = Antichain::from_elem(3u64);
        assert!(PartialOrder::less_equal(&a, &b));
        assert!(PartialOrder::less_equal(&b, &a));
    }

    // --- TotalOrder ---

    #[test]
    fn total_order_as_option() {
        let ac = Antichain::from_elem(42u64);
        assert_eq!(ac.as_option(), Some(&42));
    }

    #[test]
    fn total_order_into_option() {
        let ac = Antichain::from_elem(42u64);
        assert_eq!(ac.into_option(), Some(42));
    }

    #[test]
    fn total_order_empty_is_none() {
        let ac = Antichain::<u64>::new();
        assert_eq!(ac.as_option(), None);
    }

    // --- FromIterator / Extend ---

    #[test]
    fn from_iterator() {
        let ac: Antichain<u64> = vec![5, 3, 7, 1, 4].into_iter().collect();
        assert_eq!(ac.elements(), &[1]);
    }

    #[test]
    fn from_vec() {
        let ac: Antichain<u64> = vec![5, 3, 7].into();
        assert_eq!(ac.elements(), &[3]);
    }

    #[test]
    fn extend_from_iter() {
        let mut ac = Antichain::from_elem(5u64);
        ac.extend(vec![3, 7, 1]);
        assert_eq!(ac.elements(), &[1]);
    }

    // --- IntoIterator ---

    #[test]
    fn into_iter_owned() {
        let ac: Antichain<Product<u64, u64>> =
            vec![Product::new(1, 3), Product::new(3, 1)].into();
        let collected: Vec<_> = ac.into_iter().collect();
        assert_eq!(collected.len(), 2);
    }

    #[test]
    fn into_iter_ref() {
        let ac = Antichain::from_elem(5u64);
        let refs: Vec<&u64> = (&ac).into_iter().collect();
        assert_eq!(refs, vec![&5]);
    }

    // --- Deref ---

    #[test]
    fn deref_to_slice() {
        let ac = Antichain::from_elem(5u64);
        let slice: &[u64] = &ac;
        assert_eq!(slice, &[5]);
    }

    // --- Clear ---

    #[test]
    fn clear() {
        let mut ac = Antichain::from_elem(5u64);
        ac.clear();
        assert!(ac.is_empty());
    }
}
