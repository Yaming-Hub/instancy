//! Typed context storage for sharing configuration and services across operators.
//!
//! [`SharedContext`] is a type-keyed map that stores `Arc<T>` values. It allows
//! dataflow authors to attach typed context objects (configuration, metrics
//! collectors, schema registries, etc.) to a [`DataflowBuilder`] so that operator
//! closures can capture them at build time.
//!
//! # Usage
//!
//! ```rust
//! use instancy::dataflow::context::SharedContext;
//!
//! #[derive(Debug)]
//! struct MyConfig { batch_size: usize }
//!
//! let mut ctx = SharedContext::new();
//! ctx.insert(MyConfig { batch_size: 1024 });
//!
//! let cfg = ctx.get::<MyConfig>().unwrap();
//! assert_eq!(cfg.batch_size, 1024);
//! ```
//!
//! # Design
//!
//! - **Type-keyed**: Each concrete type `T` can have at most one entry.
//!   Use newtypes to store multiple values of the same underlying type.
//! - **Thread-safe**: Values are wrapped in `Arc` and require `Send + Sync`.
//! - **Immutable after capture**: Operators receive `Arc<T>`, ensuring
//!   shared read-only access without locks.
//!
//! [`DataflowBuilder`]: super::DataflowBuilder

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

/// A type-keyed context map for sharing typed values across operator closures.
///
/// Values are stored as `Arc<T>` so they can be cheaply cloned and captured
/// by multiple operator closures. Each type can have at most one entry — use
/// newtypes to distinguish multiple values of the same underlying type.
///
/// # Examples
///
/// ```
/// use instancy::dataflow::context::SharedContext;
///
/// struct BatchConfig { pub size: usize }
/// struct SchemaRegistry { pub version: u32 }
///
/// let mut ctx = SharedContext::new();
/// ctx.insert(BatchConfig { size: 512 });
/// ctx.insert(SchemaRegistry { version: 3 });
///
/// assert!(ctx.get::<BatchConfig>().is_some());
/// assert!(ctx.get::<SchemaRegistry>().is_some());
/// assert!(ctx.get::<String>().is_none());
/// ```
#[derive(Clone, Default)]
pub struct SharedContext {
    map: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
}

impl SharedContext {
    /// Create an empty context.
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a typed value into the context.
    ///
    /// If a value of the same type was previously inserted, it is replaced
    /// and the old value is returned. Set all context values before creating
    /// operators to ensure consistent captures.
    pub fn insert<T: Send + Sync + 'static>(&mut self, value: T) -> Option<Arc<T>> {
        self.map
            .insert(TypeId::of::<T>(), Arc::new(value))
            .and_then(|old| old.downcast::<T>().ok())
    }

    /// Insert a pre-existing `Arc<T>` into the context.
    ///
    /// Use this when you already have an `Arc<T>` (e.g., a shared service
    /// handle or connection pool) and want to avoid double-wrapping.
    pub fn insert_arc<T: Send + Sync + 'static>(&mut self, value: Arc<T>) -> Option<Arc<T>> {
        self.map
            .insert(TypeId::of::<T>(), value)
            .and_then(|old| old.downcast::<T>().ok())
    }

    /// Retrieve a previously stored value by type.
    ///
    /// Returns `Some(Arc<T>)` if a value of type `T` exists, `None` otherwise.
    pub fn get<T: Send + Sync + 'static>(&self) -> Option<Arc<T>> {
        self.map
            .get(&TypeId::of::<T>())
            .and_then(|arc| arc.clone().downcast::<T>().ok())
    }

    /// Check whether a value of type `T` exists in the context.
    pub fn contains<T: Send + Sync + 'static>(&self) -> bool {
        self.map.contains_key(&TypeId::of::<T>())
    }

    /// Remove a value by type, returning it if present.
    pub fn remove<T: Send + Sync + 'static>(&mut self) -> Option<Arc<T>> {
        self.map
            .remove(&TypeId::of::<T>())
            .and_then(|arc| arc.downcast::<T>().ok())
    }

    /// Returns the number of stored context values.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Returns `true` if no context values are stored.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

impl fmt::Debug for SharedContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharedContext")
            .field("entries", &self.map.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq)]
    struct Config {
        batch_size: usize,
    }

    #[derive(Debug, Clone, PartialEq)]
    struct Metrics {
        name: String,
    }

    #[test]
    fn insert_and_get() {
        let mut ctx = SharedContext::new();
        ctx.insert(Config { batch_size: 256 });

        let cfg = ctx.get::<Config>().expect("Config should exist");
        assert_eq!(cfg.batch_size, 256);
    }

    #[test]
    fn get_missing_returns_none() {
        let ctx = SharedContext::new();
        assert!(ctx.get::<Config>().is_none());
    }

    #[test]
    fn multiple_types() {
        let mut ctx = SharedContext::new();
        ctx.insert(Config { batch_size: 128 });
        ctx.insert(Metrics {
            name: "test".into(),
        });

        assert_eq!(ctx.get::<Config>().unwrap().batch_size, 128);
        assert_eq!(ctx.get::<Metrics>().unwrap().name, "test");
        assert_eq!(ctx.len(), 2);
    }

    #[test]
    fn overwrite_returns_old_value() {
        let mut ctx = SharedContext::new();
        let old = ctx.insert(Config { batch_size: 100 });
        assert!(old.is_none());

        let old = ctx.insert(Config { batch_size: 200 });
        assert_eq!(old.unwrap().batch_size, 100);

        assert_eq!(ctx.get::<Config>().unwrap().batch_size, 200);
    }

    #[test]
    fn contains_and_remove() {
        let mut ctx = SharedContext::new();
        assert!(!ctx.contains::<Config>());

        ctx.insert(Config { batch_size: 64 });
        assert!(ctx.contains::<Config>());

        let removed = ctx.remove::<Config>();
        assert_eq!(removed.unwrap().batch_size, 64);
        assert!(!ctx.contains::<Config>());
    }

    #[test]
    fn clone_shares_arcs() {
        let mut ctx = SharedContext::new();
        ctx.insert(Config { batch_size: 42 });

        let ctx2 = ctx.clone();
        let a = ctx.get::<Config>().unwrap();
        let b = ctx2.get::<Config>().unwrap();
        // Both point to the same allocation
        assert!(Arc::ptr_eq(&a, &b));
    }

    #[test]
    fn empty_context() {
        let ctx = SharedContext::new();
        assert!(ctx.is_empty());
        assert_eq!(ctx.len(), 0);
    }

    #[test]
    fn debug_format() {
        let mut ctx = SharedContext::new();
        ctx.insert(42u32);
        let dbg = format!("{:?}", ctx);
        assert!(dbg.contains("entries"));
        assert!(dbg.contains("1"));
    }
}
