//! Structured message envelopes for the dataflow graph.
//!
//! Messages flowing through operators carry either data or control signals,
//! along with optional user-defined metadata.

use std::fmt;

use crate::progress::timestamp::Timestamp;

/// A message flowing through the dataflow graph.
///
/// Carries data, control signals, and optional user-defined metadata.
/// The metadata type `M` defaults to `()` (zero-cost when not used).
#[derive(Debug, Clone, PartialEq)]
pub struct Envelope<T: Timestamp, D, M = ()> {
    /// The payload: data records or a control signal.
    pub payload: Payload<T, D>,
    /// User-defined metadata that flows alongside the data.
    /// Examples: current sorting order, partition strategy hints,
    /// lineage information, schema version.
    pub metadata: M,
}

impl<T: Timestamp, D> Envelope<T, D, ()> {
    /// Create a data envelope with no metadata.
    pub fn data(time: T, data: Vec<D>) -> Self {
        Self {
            payload: Payload::Data { time, data },
            metadata: (),
        }
    }

    /// Create a control envelope with no metadata.
    pub fn control(signal: ControlSignal<T>) -> Self {
        Self {
            payload: Payload::Control(signal),
            metadata: (),
        }
    }

    /// Create an error control envelope.
    pub fn error(source_operator: impl Into<String>, message: impl Into<String>) -> Self {
        Self::control(ControlSignal::Error {
            source_operator: source_operator.into(),
            message: message.into(),
        })
    }

    /// Create a watermark control envelope.
    pub fn watermark(time: T) -> Self {
        Self::control(ControlSignal::Watermark(time))
    }
}

impl<T: Timestamp, D, M> Envelope<T, D, M> {
    /// Create an envelope with custom metadata.
    pub fn with_metadata(payload: Payload<T, D>, metadata: M) -> Self {
        Self { payload, metadata }
    }

    /// Returns `true` if this is a data payload.
    pub fn is_data(&self) -> bool {
        matches!(self.payload, Payload::Data { .. })
    }

    /// Returns `true` if this is a control signal.
    pub fn is_control(&self) -> bool {
        matches!(self.payload, Payload::Control(_))
    }

    /// Get a reference to the data if this is a data payload.
    pub fn as_data(&self) -> Option<(&T, &Vec<D>)> {
        match &self.payload {
            Payload::Data { time, data } => Some((time, data)),
            _ => None,
        }
    }

    /// Get a reference to the control signal if this is a control payload.
    pub fn as_control(&self) -> Option<&ControlSignal<T>> {
        match &self.payload {
            Payload::Control(signal) => Some(signal),
            _ => None,
        }
    }

    /// Map the metadata type to a new type.
    pub fn map_metadata<M2>(self, f: impl FnOnce(M) -> M2) -> Envelope<T, D, M2> {
        Envelope {
            payload: self.payload,
            metadata: f(self.metadata),
        }
    }

    /// Strip metadata, replacing with `()`.
    pub fn strip_metadata(self) -> Envelope<T, D, ()> {
        Envelope {
            payload: self.payload,
            metadata: (),
        }
    }
}

/// The core payload of a message.
#[derive(Debug, Clone, PartialEq)]
pub enum Payload<T: Timestamp, D> {
    /// A batch of data records at the given timestamp.
    Data {
        /// The logical timestamp for this batch.
        time: T,
        /// The batch of data records.
        data: Vec<D>,
    },
    /// A control signal propagated through the dataflow.
    Control(ControlSignal<T>),
}

/// Control signals that flow in-band with data.
#[derive(Debug, Clone, PartialEq)]
pub enum ControlSignal<T: Timestamp> {
    /// An error occurred upstream. Downstream operators see this and
    /// can decide how to handle it based on the dataflow's error policy.
    Error {
        /// The operator that produced the error.
        source_operator: String,
        /// Human-readable error message.
        message: String,
    },
    /// Watermark: all future data will have timestamps >= this value.
    Watermark(T),
}

impl<T: Timestamp + fmt::Display> fmt::Display for ControlSignal<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ControlSignal::Error {
                source_operator,
                message,
            } => write!(f, "Error from '{}': {}", source_operator, message),
            ControlSignal::Watermark(t) => write!(f, "Watermark({})", t),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_envelope_creation() {
        let env: Envelope<u64, i32> = Envelope::data(42, vec![1, 2, 3]);
        assert!(env.is_data());
        assert!(!env.is_control());
        let (time, data) = env.as_data().unwrap();
        assert_eq!(*time, 42);
        assert_eq!(data, &vec![1, 2, 3]);
    }

    #[test]
    fn control_envelope_error() {
        let env: Envelope<u64, i32> = Envelope::error("map_op", "division by zero");
        assert!(env.is_control());
        assert!(!env.is_data());
        match env.as_control().unwrap() {
            ControlSignal::Error {
                source_operator,
                message,
            } => {
                assert_eq!(source_operator, "map_op");
                assert_eq!(message, "division by zero");
            }
            _ => panic!("expected error signal"),
        }
    }

    #[test]
    fn control_envelope_watermark() {
        let env: Envelope<u64, i32> = Envelope::watermark(100);
        assert!(env.is_control());
        match env.as_control().unwrap() {
            ControlSignal::Watermark(t) => assert_eq!(*t, 100),
            _ => panic!("expected watermark"),
        }
    }

    #[test]
    fn envelope_with_metadata() {
        #[derive(Debug, Clone, PartialEq)]
        struct SortOrder {
            column: String,
            ascending: bool,
        }

        let metadata = SortOrder {
            column: "id".to_string(),
            ascending: true,
        };
        let env = Envelope::with_metadata(
            Payload::Data {
                time: 10u64,
                data: vec![1, 2, 3],
            },
            metadata.clone(),
        );
        assert_eq!(env.metadata, metadata);
        assert!(env.is_data());
    }

    #[test]
    fn map_metadata() {
        let env: Envelope<u64, i32> = Envelope::data(1, vec![10]);
        let env2 = env.map_metadata(|()| "tagged".to_string());
        assert_eq!(env2.metadata, "tagged");
        assert!(env2.is_data());
    }

    #[test]
    fn strip_metadata() {
        let env = Envelope::with_metadata(
            Payload::Data {
                time: 5u64,
                data: vec![42],
            },
            "some_meta".to_string(),
        );
        let stripped = env.strip_metadata();
        assert_eq!(stripped.metadata, ());
        assert!(stripped.is_data());
    }

    #[test]
    fn control_signal_display() {
        let err = ControlSignal::<u64>::Error {
            source_operator: "filter".to_string(),
            message: "bad input".to_string(),
        };
        assert_eq!(err.to_string(), "Error from 'filter': bad input");

        let wm = ControlSignal::<u64>::Watermark(99);
        assert_eq!(wm.to_string(), "Watermark(99)");
    }
}
