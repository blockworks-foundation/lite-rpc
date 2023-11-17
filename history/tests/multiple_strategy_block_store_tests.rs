use std::borrow::Cow;
use std::ops::Deref;
use solana_lite_rpc_core::{
    structures::produced_block::ProducedBlock,
    traits::block_storage_interface::BlockStorageInterface,
};
use solana_lite_rpc_history::{
    block_stores::multiple_strategy_block_store::MultipleStrategyBlockStorage,
};
use solana_sdk::{commitment_config::CommitmentConfig, hash::Hash};
use std::sync::Arc;
use anyhow::anyhow;
use solana_lite_rpc_core::structures::epoch::EpochCache;
use solana_lite_rpc_history::block_stores::postgres_block_store::PostgresBlockStore;

pub fn create_test_block(slot: u64, commitment_config: CommitmentConfig) -> ProducedBlock {
    ProducedBlock {
        block_height: slot,
        blockhash: Hash::new_unique().to_string(),
        previous_blockhash: Hash::new_unique().to_string(),
        parent_slot: slot - 1,
        transactions: vec![],
        block_time: 0,
        commitment_config,
        leader_id: None,
        slot,
        rewards: None,
    }
}

#[tokio::test]
async fn test_in_multiple_stategy_block_store() {
    tracing_subscriber::fmt::init();

    let epoch_cache = EpochCache::new_for_tests();
    let persistent_store = PostgresBlockStore::new(epoch_cache.clone()).await;
    let multi_store = MultipleStrategyBlockStorage::new(
        persistent_store.clone(),
        None, // not supported
    );

    persistent_store
        .save(&create_test_block(1200, CommitmentConfig::confirmed()))
        .await
        .unwrap();
    // span range of slots between those two
    persistent_store
        .save(&create_test_block(1289, CommitmentConfig::confirmed()))
        .await
        .unwrap();

    assert!(multi_store.query_block(1200).await.ok().is_some());

    assert!(multi_store.query_block(1289).await.ok().is_some());

    // not in range
    assert!(multi_store.query_block(1000).await.is_err());
    // the range check should give "true", yet no block is returned
    assert!(multi_store.query_block(1250).await.is_err());
    // not in range
    assert!(multi_store.query_block(9999).await.is_err());

}
