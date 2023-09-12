use std::sync::Arc;

use solana_lite_rpc_core::block_storage_interface::BlockStorageInterface;
use solana_lite_rpc_storage::{
    block_stores::inmemory_block_store::InmemoryBlockStore,
    multiple_strategy_block_store::MultipleStrategyBlockStorage,
};
use solana_sdk::hash::Hash;
use solana_transaction_status::UiConfirmedBlock;

pub fn create_test_ui_confirmed_block(slot: u64) -> UiConfirmedBlock {
    UiConfirmedBlock {
        block_height: Some(slot),
        block_time: None,
        blockhash: Hash::new_unique().to_string(),
        parent_slot: slot - 1,
        previous_blockhash: Hash::new_unique().to_string(),
        rewards: None,
        signatures: None,
        transactions: Some(vec![]),
    }
}

#[tokio::test]
async fn test_in_multiple_stategy_block_store() {
    let persistent_store: Arc<dyn BlockStorageInterface> = Arc::new(InmemoryBlockStore::new(10));
    let number_of_slots_in_memory = 2;
    let block_storage = MultipleStrategyBlockStorage::new(
        persistent_store.clone(),
        "".to_string(),
        number_of_slots_in_memory,
    );

    block_storage
        .save(
            1235,
            create_test_ui_confirmed_block(1235),
            solana_sdk::commitment_config::CommitmentLevel::Confirmed,
        )
        .await;
    block_storage
        .save(
            1236,
            create_test_ui_confirmed_block(1236),
            solana_sdk::commitment_config::CommitmentLevel::Confirmed,
        )
        .await;

    assert!(block_storage.get(1235).await.is_some());
    assert!(block_storage.get(1236).await.is_some());
    assert!(persistent_store.get(1235).await.is_none());
    assert!(persistent_store.get(1236).await.is_none());

    block_storage
        .save(
            1235,
            create_test_ui_confirmed_block(1235),
            solana_sdk::commitment_config::CommitmentLevel::Finalized,
        )
        .await;
    block_storage
        .save(
            1236,
            create_test_ui_confirmed_block(1236),
            solana_sdk::commitment_config::CommitmentLevel::Finalized,
        )
        .await;
    block_storage
        .save(
            1237,
            create_test_ui_confirmed_block(1237),
            solana_sdk::commitment_config::CommitmentLevel::Finalized,
        )
        .await;

    assert!(block_storage.get(1235).await.is_some());
    assert!(block_storage.get(1236).await.is_some());
    assert!(block_storage.get(1237).await.is_some());
    assert!(persistent_store.get(1235).await.is_some());
    assert!(persistent_store.get(1236).await.is_some());
    assert!(persistent_store.get(1237).await.is_some());
    assert!(block_storage.get_in_memory_block(1237).await.is_some());

    // blocks are replaced by finalized blocks
    assert_eq!(
        persistent_store.get(1235).await.unwrap().blockhash,
        block_storage
            .get_in_memory_block(1235)
            .await
            .unwrap()
            .blockhash
    );
    assert_eq!(
        persistent_store.get(1236).await.unwrap().blockhash,
        block_storage
            .get_in_memory_block(1236)
            .await
            .unwrap()
            .blockhash
    );
    assert_eq!(
        persistent_store.get(1237).await.unwrap().blockhash,
        block_storage
            .get_in_memory_block(1237)
            .await
            .unwrap()
            .blockhash
    );

    // no block yet added returns none
    assert!(block_storage.get(1238).await.is_none());
}
