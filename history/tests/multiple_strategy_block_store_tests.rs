use solana_lite_rpc_core::{
    structures::produced_block::ProducedBlock,
    traits::block_storage_interface::BlockStorageInterface,
};
use solana_lite_rpc_history::{
    block_stores::inmemory_block_store::InmemoryBlockStore,
    block_stores::multiple_strategy_block_store::MultipleStrategyBlockStorage,
};
use solana_sdk::{hash::Hash};
use std::sync::Arc;
use solana_lite_rpc_core::commitment_utils::Commitment;

pub fn create_test_block(slot: u64, commitment_level: Commitment) -> ProducedBlock {
    ProducedBlock {
        block_height: slot,
        blockhash: Hash::new_unique().to_string(),
        previous_blockhash: Hash::new_unique().to_string(),
        parent_slot: slot - 1,
        transactions: vec![],
        block_time: 0,
        commitment_level,
        leader_id: None,
        slot,
        rewards: None,
    }
}

#[tokio::test]
async fn test_in_multiple_stategy_block_store() {
    let persistent_store: Arc<dyn BlockStorageInterface> = Arc::new(InmemoryBlockStore::new(10));
    let number_of_slots_in_memory = 3;
    let block_storage = MultipleStrategyBlockStorage::new(
        persistent_store.clone(),
        None, // not supported
        number_of_slots_in_memory,
    );

    block_storage
        .save(&create_test_block(1235, Commitment::Confirmed))
        .await
        .unwrap();
    block_storage
        .save(&create_test_block(1236, Commitment::Confirmed))
        .await
        .unwrap();

    assert!(block_storage
        .get(1235)
        .await
        .ok()
        .is_some());
    assert!(block_storage
        .get(1236)
        .await
        .ok()
        .is_some());
    assert!(persistent_store
        .get(1235)
        .await
        .ok()
        .is_none());
    assert!(persistent_store
        .get(1236)
        .await
        .ok()
        .is_none());

    block_storage
        .save(&create_test_block(1235, Commitment::Finalized))
        .await
        .unwrap();
    block_storage
        .save(&create_test_block(1236, Commitment::Finalized))
        .await
        .unwrap();
    block_storage
        .save(&create_test_block(1237, Commitment::Finalized))
        .await
        .unwrap();

    assert!(block_storage
        .get(1235)
        .await
        .ok()
        .is_some());
    assert!(block_storage
        .get(1236)
        .await
        .ok()
        .is_some());
    assert!(block_storage
        .get(1237)
        .await
        .ok()
        .is_some());
    assert!(persistent_store
        .get(1235)
        .await
        .ok()
        .is_some());
    assert!(persistent_store
        .get(1236)
        .await
        .ok()
        .is_some());
    assert!(persistent_store
        .get(1237)
        .await
        .ok()
        .is_some());
    assert!(block_storage.get_in_memory_block(1237).await.ok().is_some());

    // blocks are replaced by finalized blocks
    assert_eq!(
        persistent_store
            .get(1235)
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
            .get(1236)
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
            .get(1237)
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
        .get(1238)
        .await
        .ok()
        .is_none());
}
