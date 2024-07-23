pub mod redis_handler;

use std::collections::HashMap;

use async_trait::async_trait;
use inindexer::near_indexer_primitives::types::AccountId;
use inindexer::near_indexer_primitives::views::{ActionView, ReceiptEnumView};
use inindexer::near_indexer_primitives::StreamerMessage;
use inindexer::{IncompleteTransaction, Indexer, TransactionReceipt};
use intear_events::events::socialdb::index::SocialDBIndexEventData;
use serde::Deserialize;

const SOCIALDB_CONTRACT: &str = "social.near";

#[async_trait]
pub trait SocialDBEventHandler: Send + Sync {
    async fn handle_index(&mut self, mint: SocialDBIndexEventData);
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
                for (index_type, entries) in index {
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
                            .handle_index(SocialDBIndexEventData {
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
