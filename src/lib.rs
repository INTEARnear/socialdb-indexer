pub mod redis_handler;

use std::collections::HashMap;

use async_trait::async_trait;
use inindexer::near_indexer_primitives::types::{AccountId, BlockHeight};
use inindexer::near_indexer_primitives::views::{ActionView, ReceiptEnumView};
use inindexer::near_indexer_primitives::StreamerMessage;
use inindexer::{IncompleteTransaction, Indexer, TransactionReceipt};
use intear_events::events::socialdb::index::SocialDBIndexEvent;
use serde::Deserialize;

const SOCIALDB_CONTRACT: &str = "social.near";

#[async_trait]
pub trait SocialDBEventHandler: Send + Sync {
    async fn handle_index(&mut self, event: SocialDBIndexEvent);

    /// Called after each block
    async fn flush_events(&mut self, block_height: BlockHeight);
}

pub struct SocialDBIndexer<T: SocialDBEventHandler + Send + Sync + 'static>(pub T);

#[async_trait]
impl<T: SocialDBEventHandler + Send + Sync + 'static> Indexer for SocialDBIndexer<T> {
    type Error = String;

    async fn on_receipt(
        &mut self,
        receipt: &TransactionReceipt,
        transaction: &IncompleteTransaction,
        _block: &StreamerMessage,
    ) -> Result<(), Self::Error> {
        if !receipt.is_successful(false) {
            return Ok(());
        }
        if receipt.receipt.receipt.receiver_id != SOCIALDB_CONTRACT {
            return Ok(());
        }
        let ReceiptEnumView::Action { actions, .. } = &receipt.receipt.receipt.receipt else {
            return Ok(());
        };
        for action in actions {
            let ActionView::FunctionCall {
                method_name, args, ..
            } = action
            else {
                continue;
            };
            if method_name != "set" {
                continue;
            }
            let Ok(ArgsWithIndex { data }) = serde_json::from_slice::<ArgsWithIndex>(args) else {
                continue;
            };
            for (account, ArgsWithIndexUserData { index }) in data {
                if account != receipt.receipt.receipt.predecessor_id {
                    log::warn!(
                        "Someone tried to set someone else's data at receipt {}",
                        receipt.receipt.receipt.receipt_id
                    );
                    continue;
                }
                let mut entries = index.into_iter().collect::<Vec<_>>();
                entries.sort_by_key(|(index_type, _)| index_type.clone()); // preserve strict order
                for (index_type, entries) in entries {
                    let Ok(entries) = serde_json::from_str::<IndexEntries>(&entries) else {
                        log::warn!(
                            "Failed to parse index entry at receipt {}: {}",
                            receipt.receipt.receipt.receipt_id,
                            entries
                        );
                        continue;
                    };
                    for entry in entries {
                        self.0
                            .handle_index(SocialDBIndexEvent {
                                block_height: receipt.block_height,
                                block_timestamp_nanosec: receipt.block_timestamp_nanosec,
                                transaction_id: transaction.transaction.transaction.hash,
                                receipt_id: receipt.receipt.receipt.receipt_id,
                                account_id: account.clone(),
                                index_type: index_type.clone(),
                                index_key: entry.key,
                                index_value: entry.value,
                            })
                            .await;
                    }
                }
            }
        }
        Ok(())
    }

    async fn process_block_end(&mut self, block: &StreamerMessage) -> Result<(), Self::Error> {
        self.0.flush_events(block.block.header.height).await;
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct ArgsWithIndex {
    data: HashMap<AccountId, ArgsWithIndexUserData>,
}

#[derive(Debug, Deserialize)]
struct ArgsWithIndexUserData {
    index: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum IndexEntries {
    OneEntry(IndexEntry),
    ManyEntries(Vec<IndexEntry>),
}

impl IntoIterator for IndexEntries {
    type Item = IndexEntry;
    type IntoIter = Box<dyn Iterator<Item = Self::Item> + Send>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            IndexEntries::OneEntry(entry) => Box::new(std::iter::once(entry)),
            IndexEntries::ManyEntries(entries) => Box::new(entries.into_iter()),
        }
    }
}

#[derive(Debug, Deserialize)]
struct IndexEntry {
    key: serde_json::Value,
    value: serde_json::Value,
}
