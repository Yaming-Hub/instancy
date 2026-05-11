//! Chrome Trace JSON export for Perfetto UI visualization.
//!
//! Converts collected [`ActivationEvent`]s and [`ChannelMetrics`] into the
//! [Chrome Trace Event Format](https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU)
//! which can be opened in [Perfetto UI](https://ui.perfetto.dev/) or Chrome's
//! `chrome://tracing`.
//!
//! # Feature flag
//!
//! This module requires the `chrome-trace` feature:
//!
//! ```toml
//! [dependencies]
//! instancy = { version = "0.1", features = ["chrome-trace"] }
//! ```
//!
//! # Usage
//!
//! ```rust,ignore
//! use instancy::metrics::chrome_trace::ChromeTraceExporter;
//!
//! // After dataflow completes, collect events from metrics:
//! let events = metrics.drain_timeline_events();
//! let channels = metrics.channel_snapshots();
//! let operators: Vec<_> = metrics.operator_snapshots()
//!     .into_iter()
//!     .map(|op| (op.index, op.name.clone()))
//!     .collect();
//!
//! let exporter = ChromeTraceExporter::new("my-dataflow")
//!     .with_activations(&events, &operators)
//!     .with_channels(&channels);
//!
//! // Write to file (opens in Perfetto UI via drag-and-drop):
//! exporter.save("trace.json").unwrap();
//!
//! // Or get as bytes:
//! let json_bytes = exporter.to_bytes().unwrap();
//! ```

use serde::Serialize;
use std::io;
use std::path::Path;

use super::{ActivationEvent, ChannelMetrics, FrontierEvent, TransferEvent};

/// A Chrome Trace JSON event (subset of the spec we use).
#[derive(Serialize)]
struct TraceEvent {
    /// Event name.
    name: String,
    /// Event category.
    cat: String,
    /// Phase: "X" = complete event, "i" = instant, "C" = counter, "M" = metadata,
    /// "s" = flow start, "f" = flow finish.
    ph: String,
    /// Timestamp in microseconds.
    ts: u64,
    /// Duration in microseconds (only for "X" events).
    #[serde(skip_serializing_if = "Option::is_none")]
    dur: Option<u64>,
    /// Flow event ID — links "s" and "f" events into connected arrows.
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<u64>,
    /// Scope for instant events: "g" = global, "p" = process, "t" = thread.
    #[serde(skip_serializing_if = "Option::is_none")]
    s: Option<String>,
    /// Process ID — we map stage/dataflow to pid.
    pid: u64,
    /// Thread ID — we map worker_index to tid.
    tid: u64,
    /// Additional arguments.
    #[serde(skip_serializing_if = "Option::is_none")]
    args: Option<serde_json::Value>,
}

/// Builder for Chrome Trace JSON export.
///
/// Collects activation events, operator metadata, and channel metrics,
/// then serializes them as Chrome Trace JSON suitable for Perfetto UI.
pub struct ChromeTraceExporter {
    dataflow_name: String,
    trace_events: Vec<TraceEvent>,
}

impl ChromeTraceExporter {
    /// Create a new exporter for the given dataflow.
    pub fn new(dataflow_name: impl Into<String>) -> Self {
        Self {
            dataflow_name: dataflow_name.into(),
            trace_events: Vec::new(),
        }
    }

    /// Add activation events as Chrome Trace "X" (complete) events.
    ///
    /// Each activation becomes a duration event on the track identified
    /// by `(pid=0, tid=worker_index)`.
    pub fn with_activations(
        mut self,
        events: &[ActivationEvent],
        operator_names: &[(usize, String)],
    ) -> Self {
        // Build a lookup from operator_index → name.
        let name_map: std::collections::HashMap<usize, &str> = operator_names
            .iter()
            .map(|(idx, name)| (*idx, name.as_str()))
            .collect();

        for ev in events {
            let name = name_map
                .get(&ev.operator_index)
                .copied()
                .unwrap_or("unknown");

            self.trace_events.push(TraceEvent {
                name: name.to_string(),
                cat: "activation".to_string(),
                ph: "X".to_string(),
                ts: ev.start_us,
                dur: Some(ev.duration_us),
                id: None,
                s: None,
                pid: 0,
                tid: ev.worker_index as u64,
                args: Some(serde_json::json!({
                    "operator_index": ev.operator_index,
                })),
            });
        }
        self
    }

    /// Add frontier advance events as Chrome Trace instant events.
    ///
    /// Each frontier advance becomes an instant event on the operator's
    /// worker track, showing when frontiers change.
    pub fn with_frontiers(
        mut self,
        events: &[FrontierEvent],
        operator_names: &[(usize, String)],
    ) -> Self {
        let name_map: std::collections::HashMap<usize, &str> = operator_names
            .iter()
            .map(|(idx, name)| (*idx, name.as_str()))
            .collect();

        for ev in events {
            let op_name = name_map
                .get(&ev.operator_index)
                .copied()
                .unwrap_or("unknown");

            self.trace_events.push(TraceEvent {
                name: format!("frontier: {op_name}"),
                cat: "frontier".to_string(),
                ph: "i".to_string(),
                ts: ev.timestamp_us,
                dur: None,
                id: None,
                s: Some("t".to_string()), // thread scope
                pid: 0,
                tid: ev.worker_index as u64,
                args: Some(serde_json::json!({
                    "operator_index": ev.operator_index,
                    "new_frontier": ev.new_frontier,
                })),
            });
        }
        self
    }

    /// Add transfer events as Chrome Trace flow events.
    ///
    /// Each transfer becomes a pair of flow events: a start ("s") on the
    /// source worker track and a finish ("f") on the target worker track.
    pub fn with_transfers(mut self, events: &[TransferEvent]) -> Self {
        for (i, ev) in events.iter().enumerate() {
            let flow_id = i as u64;

            // Flow start on source worker.
            self.trace_events.push(TraceEvent {
                name: format!("transfer[{}]", ev.edge_index),
                cat: "transfer".to_string(),
                ph: "s".to_string(),
                ts: ev.timestamp_us,
                dur: None,
                id: Some(flow_id),
                s: None,
                pid: 0,
                tid: ev.source_worker as u64,
                args: Some(serde_json::json!({
                    "edge_index": ev.edge_index,
                    "items": ev.items,
                    "bytes": ev.bytes,
                })),
            });

            // Flow finish on target worker.
            self.trace_events.push(TraceEvent {
                name: format!("transfer[{}]", ev.edge_index),
                cat: "transfer".to_string(),
                ph: "f".to_string(),
                ts: ev.timestamp_us,
                dur: None,
                id: Some(flow_id),
                s: None,
                pid: 0,
                tid: ev.target_worker as u64,
                args: None,
            });
        }
        self
    }

    /// Add channel metrics as metadata annotations.
    ///
    /// Each channel edge gets an instant event at time 0 summarizing
    /// total transfer volume.
    pub fn with_channels(mut self, channels: &[ChannelMetrics]) -> Self {
        for ch in channels {
            self.trace_events.push(TraceEvent {
                name: format!("channel: {}", ch.label),
                cat: "channel".to_string(),
                ph: "i".to_string(),
                ts: 0,
                dur: None,
                id: None,
                s: Some("g".to_string()),
                pid: 0,
                tid: 0,
                args: Some(serde_json::json!({
                    "edge_index": ch.edge_index,
                    "items_transferred": ch.items_transferred,
                    "bytes_transferred": ch.bytes_transferred,
                })),
            });
        }
        self
    }

    /// Add process/thread metadata events for Perfetto UI labels.
    ///
    /// Call with the number of workers to generate metadata events that
    /// label each thread track as "worker-N" and the process as the
    /// dataflow name.
    pub fn with_metadata(mut self, num_workers: usize) -> Self {
        // Process name.
        self.trace_events.push(TraceEvent {
            name: "process_name".to_string(),
            cat: String::new(),
            ph: "M".to_string(),
            ts: 0,
            dur: None,
            id: None,
                s: None,
            pid: 0,
            tid: 0,
            args: Some(serde_json::json!({
                "name": self.dataflow_name,
            })),
        });

        // Thread names (one per worker).
        for w in 0..num_workers {
            self.trace_events.push(TraceEvent {
                name: "thread_name".to_string(),
                cat: String::new(),
                ph: "M".to_string(),
                ts: 0,
                dur: None,
                id: None,
                s: None,
                pid: 0,
                tid: w as u64,
                args: Some(serde_json::json!({
                    "name": format!("worker-{w}"),
                })),
            });
        }
        self
    }

    /// Serialize to Chrome Trace JSON bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        // Chrome Trace format: {"traceEvents": [...]}
        let wrapper = serde_json::json!({
            "traceEvents": self.trace_events,
        });
        serde_json::to_vec_pretty(&wrapper)
    }

    /// Write Chrome Trace JSON to a file.
    ///
    /// The resulting file can be opened in Perfetto UI (`ui.perfetto.dev`)
    /// via drag-and-drop, or in Chrome's `chrome://tracing`.
    pub fn save(&self, path: impl AsRef<Path>) -> io::Result<()> {
        let bytes = self
            .to_bytes()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        std::fs::write(path, bytes)
    }

    /// Number of trace events that will be written.
    pub fn event_count(&self) -> usize {
        self.trace_events.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::ActivationEvent;

    #[test]
    fn empty_exporter_produces_valid_json() {
        let exporter = ChromeTraceExporter::new("test-df");
        let bytes = exporter.to_bytes().unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert!(json["traceEvents"].is_array());
        assert_eq!(json["traceEvents"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn activations_produce_x_events() {
        let events = vec![
            ActivationEvent {
                operator_index: 0,
                worker_index: 0,
                start_us: 100,
                duration_us: 50,
            },
            ActivationEvent {
                operator_index: 1,
                worker_index: 1,
                start_us: 200,
                duration_us: 30,
            },
        ];
        let names = vec![(0, "source".to_string()), (1, "map".to_string())];

        let exporter = ChromeTraceExporter::new("test")
            .with_activations(&events, &names);

        assert_eq!(exporter.event_count(), 2);

        let bytes = exporter.to_bytes().unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let trace_events = json["traceEvents"].as_array().unwrap();

        assert_eq!(trace_events[0]["name"], "source");
        assert_eq!(trace_events[0]["ph"], "X");
        assert_eq!(trace_events[0]["ts"], 100);
        assert_eq!(trace_events[0]["dur"], 50);
        assert_eq!(trace_events[0]["tid"], 0);

        assert_eq!(trace_events[1]["name"], "map");
        assert_eq!(trace_events[1]["tid"], 1);
    }

    #[test]
    fn channels_produce_instant_events() {
        let channels = vec![ChannelMetrics {
            edge_index: 0,
            label: "exchange[0]".to_string(),
            items_transferred: 1000,
            bytes_transferred: 4000,
        }];

        let exporter = ChromeTraceExporter::new("test").with_channels(&channels);
        assert_eq!(exporter.event_count(), 1);

        let bytes = exporter.to_bytes().unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let ev = &json["traceEvents"][0];
        assert_eq!(ev["ph"], "i");
        assert_eq!(ev["s"], "g"); // global scope required by Chrome Trace spec
        assert_eq!(ev["args"]["items_transferred"], 1000);
    }

    #[test]
    fn metadata_labels_workers() {
        let exporter = ChromeTraceExporter::new("my-dataflow").with_metadata(3);

        // 1 process_name + 3 thread_names = 4 events.
        assert_eq!(exporter.event_count(), 4);

        let bytes = exporter.to_bytes().unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let events = json["traceEvents"].as_array().unwrap();

        // First event should be process name metadata.
        assert_eq!(events[0]["ph"], "M");
        assert_eq!(events[0]["name"], "process_name");
        assert_eq!(events[0]["args"]["name"], "my-dataflow");

        // Thread names.
        assert_eq!(events[1]["args"]["name"], "worker-0");
        assert_eq!(events[2]["args"]["name"], "worker-1");
        assert_eq!(events[3]["args"]["name"], "worker-2");
    }

    #[test]
    fn save_writes_valid_file() {
        let events = vec![ActivationEvent {
            operator_index: 0,
            worker_index: 0,
            start_us: 0,
            duration_us: 100,
        }];
        let names = vec![(0, "src".to_string())];

        let exporter = ChromeTraceExporter::new("file-test")
            .with_activations(&events, &names)
            .with_metadata(1);

        let dir = std::env::temp_dir().join("instancy-test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("trace-test.json");

        exporter.save(&path).unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        let json: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert!(!json["traceEvents"].as_array().unwrap().is_empty());

        // Cleanup.
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn full_pipeline_export() {
        // Simulate a complete export pipeline: activations + channels + metadata.
        let events = vec![
            ActivationEvent {
                operator_index: 0,
                worker_index: 0,
                start_us: 0,
                duration_us: 100,
            },
            ActivationEvent {
                operator_index: 1,
                worker_index: 0,
                start_us: 100,
                duration_us: 200,
            },
            ActivationEvent {
                operator_index: 0,
                worker_index: 1,
                start_us: 10,
                duration_us: 90,
            },
        ];
        let names = vec![
            (0, "source".to_string()),
            (1, "exchange".to_string()),
        ];
        let channels = vec![ChannelMetrics {
            edge_index: 0,
            label: "ex[0]".to_string(),
            items_transferred: 500,
            bytes_transferred: 2000,
        }];

        let exporter = ChromeTraceExporter::new("pipeline-test")
            .with_activations(&events, &names)
            .with_channels(&channels)
            .with_metadata(2);

        // 3 activations + 1 channel + 1 process_name + 2 thread_names = 7
        assert_eq!(exporter.event_count(), 7);

        // Verify it's valid JSON.
        let bytes = exporter.to_bytes().unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["traceEvents"].as_array().unwrap().len(), 7);
    }
}
