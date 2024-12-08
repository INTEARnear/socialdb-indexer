use async_trait::async_trait;
use inevents_redis::RedisEventStream;
use inindexer::near_indexer_primitives::types::BlockHeight;
use intear_events::events::socialdb::index::SocialDBIndexEvent;
use redis::aio::ConnectionManager;

use crate::SocialDBEventHandler;

pub struct PushToRedisStream {
    index_stream: RedisEventStream<SocialDBIndexEvent>,
    max_stream_size: usize,
}

impl PushToRedisStream {
    pub async fn new(connection: ConnectionManager, max_stream_size: usize) -> Self {
        Self {
            index_stream: RedisEventStream::new(connection.clone(), SocialDBIndexEvent::ID),
            max_stream_size,
        }
    }
}

#[async_trait]
impl SocialDBEventHandler for PushToRedisStream {
    async fn handle_index(&mut self, event: SocialDBIndexEvent) {
        self.index_stream.add_event(SocialDBIndexEvent {
            block_height: event.block_height,
            block_timestamp_nanosec: event.block_timestamp_nanosec,
            transaction_id: event.transaction_id,
            receipt_id: event.receipt_id,
            account_id: event.account_id,
            index_type: event.index_type,
            index_key: event.index_key,
            index_value: event.index_value,
        });
    }

    async fn flush_events(&mut self, block_height: BlockHeight) {
        self.index_stream
            .flush_events(block_height, self.max_stream_size)
            .await
            .expect("Failed to flush index stream");
    }
}
