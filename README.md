# SocialDB Indexer

This indexer watches for SocialDB index events (https://github.com/NearSocial/standards/blob/8713aed325226db5cf97ab9744ba78b561cc377b/types/index/Index.md) and sends them to a Redis stream `socialdb_index`.

To run it, set `REDIS_URL` environment variable and `cargo run --release`
