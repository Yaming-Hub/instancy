use crate::progress::frontier::Antichain;
use crate::progress::timestamp::Timestamp;

/// Messages carried by exchange channels between StageExecutors.
///
/// In the StageExecutor model, exchange channels carry both data and progress
/// information inline (watermark model). This eliminates the need for a
/// separate progress broadcast channel.
///
/// FIFO ordering is required: a `FrontierUpdate` for time T must arrive
/// after all `DataBatch` messages at time ≤ T on the same channel.
/// This is guaranteed by bounded queues (in-process) and instancy's
/// SequenceCounter + ReorderBuffer (cross-process).
#[derive(Debug, Clone)]
pub enum ExchangeMessage<T: Timestamp, D: Clone + Send + 'static> {
    /// A batch of data records at a specific timestamp.
    DataBatch {
        /// The timestamp of all records in this batch.
        time: T,
        /// The data records.
        data: Vec<D>,
    },
    /// The sender's output frontier has changed.
    /// The receiver uses this to update its FrontierAggregator.
    FrontierUpdate {
        /// The sender's new output frontier (minimal antichain).
        frontier: Antichain<T>,
    },
    /// The sender will produce no more messages on this channel.
    /// After receiving SenderDone from all senders on an exchange input,
    /// the receiver knows no more data can arrive.
    SenderDone,
}

#[cfg(test)]
mod tests {
    use super::ExchangeMessage;
    use crate::progress::frontier::Antichain;

    #[test]
    fn data_batch_construction_and_pattern_matching() {
        let message = ExchangeMessage::DataBatch {
            time: 7u64,
            data: vec![1i32, 2, 3],
        };

        match message {
            ExchangeMessage::DataBatch { time, data } => {
                assert_eq!(time, 7);
                assert_eq!(data, vec![1, 2, 3]);
            }
            other => panic!("expected DataBatch, got {other:?}"),
        }
    }

    #[test]
    fn frontier_update_construction_and_pattern_matching() {
        let message = ExchangeMessage::<u64, i32>::FrontierUpdate {
            frontier: Antichain::from_elem(11),
        };

        match message {
            ExchangeMessage::FrontierUpdate { frontier } => {
                assert_eq!(frontier, Antichain::from_elem(11));
            }
            other => panic!("expected FrontierUpdate, got {other:?}"),
        }
    }

    #[test]
    fn sender_done_construction_and_pattern_matching() {
        let message = ExchangeMessage::<u64, i32>::SenderDone;

        match message {
            ExchangeMessage::SenderDone => {}
            other => panic!("expected SenderDone, got {other:?}"),
        }
    }

    #[test]
    fn clone_preserves_message_contents() {
        let message = ExchangeMessage::DataBatch {
            time: 3u64,
            data: vec![4i32, 5],
        };

        let cloned = message.clone();

        match cloned {
            ExchangeMessage::DataBatch { time, data } => {
                assert_eq!(time, 3);
                assert_eq!(data, vec![4, 5]);
            }
            other => panic!("expected cloned DataBatch, got {other:?}"),
        }
    }

    #[test]
    fn debug_formats_variant_name() {
        let data_batch = ExchangeMessage::DataBatch {
            time: 1u64,
            data: vec![9i32],
        };
        let frontier_update = ExchangeMessage::<u64, i32>::FrontierUpdate {
            frontier: Antichain::from_elem(2),
        };
        let sender_done = ExchangeMessage::<u64, i32>::SenderDone;

        let data_batch_debug = format!("{data_batch:?}");
        let frontier_update_debug = format!("{frontier_update:?}");
        let sender_done_debug = format!("{sender_done:?}");

        assert!(data_batch_debug.contains("DataBatch"));
        assert!(frontier_update_debug.contains("FrontierUpdate"));
        assert!(sender_done_debug.contains("SenderDone"));
    }
}
