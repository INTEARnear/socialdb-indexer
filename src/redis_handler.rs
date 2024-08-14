use async_trait::async_trait;
use inevents_redis::RedisEventStream;
use intear_events::events::socialdb::index::{SocialDBIndexEvent, SocialDBIndexEventData};
use redis::aio::ConnectionManager;

use crate::SocialDBEventHandler;

pub struct PushToRedisStream {
    index_stream: RedisEventStream<SocialDBIndexEventData>,
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
    async fn handle_index(&mut self, event: SocialDBIndexEventData) {
        self.index_stream
            .emit_event(event.block_height, event, self.max_stream_size)
            .await
            .expect("Failed to emit index event");
    }
}
