#[cfg(test)]
mod tests;

use inindexer::neardata_server::NeardataServerProvider;
use inindexer::{
    run_indexer, AutoContinue, BlockIterator, IndexerOptions, PreprocessTransactionsSettings,
};
use redis::aio::ConnectionManager;
use socialdb_indexer::{redis_handler::PushToRedisStream, SocialDBIndexer};

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .with_module_level("inindexer::performance", log::LevelFilter::Debug)
        .init()
        .unwrap();

    let client = redis::Client::open(
        std::env::var("REDIS_URL").expect("No $REDIS_URL environment variable set"),
    )
    .unwrap();
    let connection = ConnectionManager::new(client).await.unwrap();

    let mut indexer = SocialDBIndexer(PushToRedisStream::new(connection, 10_000).await);

    run_indexer(
        &mut indexer,
        NeardataServerProvider::mainnet(),
        IndexerOptions {
            range: if std::env::args().len() > 1 {
                // For debugging
                let msg =
                    "Usage: `socialdb-indexer` or `socialdb-indexer [start-block] [end-block]`";
                BlockIterator::iterator(
                    std::env::args()
                        .nth(1)
                        .expect(msg)
                        .replace(['_', ',', ' ', '.'], "")
                        .parse()
                        .expect(msg)
                        ..=std::env::args()
                            .nth(2)
                            .expect(msg)
                            .replace(['_', ',', ' ', '.'], "")
                            .parse()
                            .expect(msg),
                )
            } else {
                BlockIterator::AutoContinue(AutoContinue::default())
            },
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: if cfg!(debug_assertions) { 0 } else { 100 },
                postfetch_blocks: 0,
            }),
            ..Default::default()
        },
    )
    .await
    .expect("Indexer run failed");
}
