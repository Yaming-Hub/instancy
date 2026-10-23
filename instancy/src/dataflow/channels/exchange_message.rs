use crate::progress::timestamp::Timestamp;

/// A batch of records carried by an exchange channel.
#[derive(Debug, Clone)]
pub enum ExchangeMessage<T: Timestamp, D: Clone + Send + 'static> {
    /// A batch of data records at a specific timestamp.
    DataBatch {
        /// The timestamp of all records in this batch.
        time: T,
        /// The data records.
        data: Vec<D>,
    },
}

#[cfg(test)]
mod tests {
    use super::ExchangeMessage;

    #[test]
    fn data_batch_construction_and_pattern_matching() {
        let message = ExchangeMessage::DataBatch {
            time: 7u64,
            data: vec![1i32, 2, 3],
        };

        let ExchangeMessage::DataBatch { time, data } = message;
        assert_eq!(time, 7);
        assert_eq!(data, vec![1, 2, 3]);
    }

    #[test]
    fn clone_preserves_message_contents() {
        let message = ExchangeMessage::DataBatch {
            time: 3u64,
            data: vec![4i32, 5],
        };

        let ExchangeMessage::DataBatch { time, data } = message.clone();
        assert_eq!(time, 3);
        assert_eq!(data, vec![4, 5]);
    }

    #[test]
    fn debug_formats_variant_name() {
        let data_batch = ExchangeMessage::DataBatch {
            time: 1u64,
            data: vec![9i32],
        };
        assert!(format!("{data_batch:?}").contains("DataBatch"));
    }
}
