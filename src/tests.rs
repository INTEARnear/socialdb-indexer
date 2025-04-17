use async_trait::async_trait;
use inindexer::{
    near_indexer_primitives::types::{AccountId, BlockHeight},
    neardata::NeardataProvider,
    run_indexer, BlockRange, IndexerOptions, PreprocessTransactionsSettings,
};
use intear_events::events::socialdb::index::SocialDBIndexEvent;
use socialdb_indexer::{SocialDBEventHandler, SocialDBIndexer};

#[derive(Default)]
struct TestIndexer {
    data: Vec<(AccountId, String, serde_json::Value, serde_json::Value)>,
}

#[async_trait]
impl SocialDBEventHandler for TestIndexer {
    async fn handle_index(&mut self, event: SocialDBIndexEvent) {
        self.data.push((
            event.account_id,
            event.index_type,
            event.index_key,
            event.index_value,
        ));
    }

    async fn flush_events(&mut self, _block_height: BlockHeight) {}
}

#[tokio::test]
async fn handles_dao_proposals() {
    let mut indexer = SocialDBIndexer(TestIndexer::default());

    run_indexer(
        &mut indexer,
        NeardataProvider::mainnet(),
        IndexerOptions {
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..IndexerOptions::default_with_range(BlockRange::Range {
                start_inclusive: 122326018,
                end_exclusive: Some(122326020),
            })
        },
    )
    .await
    .unwrap();

    assert_eq!(
        serde_json::to_string(&indexer.0.data).unwrap(),
        r#"[["slimedragon.near","notify","slimedragon.near",{"message":"slimedragon.near created Transfer proposal for intear.sputnik-dao.near","params":{"daoId":"intear.sputnik-dao.near","page":"dao","tab":"proposals"},"type":"custom","widget":"astraplusplus.ndctools.near/widget/home"}],["slimedragon.near","notify","bjirken.near",{"message":"slimedragon.near created Transfer proposal for intear.sputnik-dao.near","params":{"daoId":"intear.sputnik-dao.near","page":"dao","tab":"proposals"},"type":"custom","widget":"astraplusplus.ndctools.near/widget/home"}],["slimedragon.near","notify","mohaa.near",{"message":"slimedragon.near created Transfer proposal for intear.sputnik-dao.near","params":{"daoId":"intear.sputnik-dao.near","page":"dao","tab":"proposals"},"type":"custom","widget":"astraplusplus.ndctools.near/widget/home"}],["slimedragon.near","notify","dyolo.near",{"message":"slimedragon.near created Transfer proposal for intear.sputnik-dao.near","params":{"daoId":"intear.sputnik-dao.near","page":"dao","tab":"proposals"},"type":"custom","widget":"astraplusplus.ndctools.near/widget/home"}]]"#,
    );
}

#[tokio::test]
async fn handles_posts() {
    let mut indexer = SocialDBIndexer(TestIndexer::default());

    run_indexer(
        &mut indexer,
        NeardataProvider::mainnet(),
        IndexerOptions {
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..IndexerOptions::default_with_range(BlockRange::Range {
                start_inclusive: 124058850,
                end_exclusive: Some(124058853),
            })
        },
    )
    .await
    .unwrap();

    assert_eq!(
        serde_json::to_string(&indexer.0.data).unwrap(),
        r#"[["devgovgigs.near","post","main",{"type":"md"}]]"#,
    );
}

#[tokio::test]
async fn handles_like_with_notify() {
    let mut indexer = SocialDBIndexer(TestIndexer::default());

    run_indexer(
        &mut indexer,
        NeardataProvider::mainnet(),
        IndexerOptions {
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..IndexerOptions::default_with_range(BlockRange::Range {
                start_inclusive: 124062482,
                end_exclusive: Some(124062484),
            })
        },
    )
    .await
    .unwrap();

    assert_eq!(
        serde_json::to_string(&indexer.0.data).unwrap(),
        "[[\"nearversedao.near\",\"like\",{\"blockHeight\":124058852,\"path\":\"devgovgigs.near/post/main\",\"type\":\"social\"},{\"type\":\"like\"}],[\"nearversedao.near\",\"notify\",\"devgovgigs.near\",{\"item\":{\"blockHeight\":124058852,\"path\":\"devgovgigs.near/post/main\",\"type\":\"social\"},\"type\":\"like\"}]]"
   );
}
