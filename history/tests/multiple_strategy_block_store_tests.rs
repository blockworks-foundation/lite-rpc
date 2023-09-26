use solana_lite_rpc_core::{
    structures::produced_block::ProducedBlock,
    traits::block_storage_interface::BlockStorageInterface,
};
use solana_lite_rpc_history::{
    block_stores::inmemory_block_store::InmemoryBlockStore,
    block_stores::multiple_strategy_block_store::MultipleStrategyBlockStorage,
};
use solana_rpc_client_api::config::RpcBlockConfig;
use solana_sdk::{commitment_config::CommitmentConfig, hash::Hash};
use std::sync::Arc;

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
    }
}

#[tokio::test]
async fn test_in_multiple_stategy_block_store() {
    let persistent_store: Arc<dyn BlockStorageInterface> = Arc::new(InmemoryBlockStore::new(10));
    let number_of_slots_in_memory = 3;
    let block_storage = MultipleStrategyBlockStorage::new(
        persistent_store.clone(),
        None,
        number_of_slots_in_memory,
    );

    block_storage
        .save(create_test_block(1235, CommitmentConfig::confirmed()))
        .await;
    block_storage
        .save(create_test_block(1236, CommitmentConfig::confirmed()))
        .await;

    assert!(block_storage
        .get(1235, RpcBlockConfig::default())
        .await
        .is_some());
    assert!(block_storage
        .get(1236, RpcBlockConfig::default())
        .await
        .is_some());
    assert!(persistent_store
        .get(1235, RpcBlockConfig::default())
        .await
        .is_none());
    assert!(persistent_store
        .get(1236, RpcBlockConfig::default())
        .await
        .is_none());

    block_storage
        .save(create_test_block(1235, CommitmentConfig::finalized()))
        .await;
    block_storage
        .save(create_test_block(1236, CommitmentConfig::finalized()))
        .await;
    block_storage
        .save(create_test_block(1237, CommitmentConfig::finalized()))
        .await;

    assert!(block_storage
        .get(1235, RpcBlockConfig::default())
        .await
        .is_some());
    assert!(block_storage
        .get(1236, RpcBlockConfig::default())
        .await
        .is_some());
    assert!(block_storage
        .get(1237, RpcBlockConfig::default())
        .await
        .is_some());
    assert!(persistent_store
        .get(1235, RpcBlockConfig::default())
        .await
        .is_some());
    assert!(persistent_store
        .get(1236, RpcBlockConfig::default())
        .await
        .is_some());
    assert!(persistent_store
        .get(1237, RpcBlockConfig::default())
        .await
        .is_some());
    assert!(block_storage.get_in_memory_block(1237).await.is_some());

    // blocks are replaced by finalized blocks
    assert_eq!(
        persistent_store
            .get(1235, RpcBlockConfig::default())
            .await
            .unwrap()
            .blockhash,
        block_storage
            .get_in_memory_block(1235)
            .await
            .unwrap()
            .blockhash
    );
    assert_eq!(
        persistent_store
            .get(1236, RpcBlockConfig::default())
            .await
            .unwrap()
            .blockhash,
        block_storage
            .get_in_memory_block(1236)
            .await
            .unwrap()
            .blockhash
    );
    assert_eq!(
        persistent_store
            .get(1237, RpcBlockConfig::default())
            .await
            .unwrap()
            .blockhash,
        block_storage
            .get_in_memory_block(1237)
            .await
            .unwrap()
            .blockhash
    );

    // no block yet added returns none
    assert!(block_storage
        .get(1238, RpcBlockConfig::default())
        .await
        .is_none());
}
