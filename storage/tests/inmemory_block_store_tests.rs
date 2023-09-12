use std::sync::Arc;

use solana_lite_rpc_core::block_storage_interface::BlockStorageInterface;
use solana_lite_rpc_storage::block_stores::inmemory_block_store::InmemoryBlockStore;
use solana_sdk::{commitment_config::CommitmentLevel, hash::Hash};
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
async fn inmemory_block_store_tests() {
    // will store only 10 blocks
    let store: Arc<dyn BlockStorageInterface> = Arc::new(InmemoryBlockStore::new(10));

    // add 10 blocks
    for i in 1..11 {
        store
            .save(
                i,
                create_test_ui_confirmed_block(i),
                CommitmentLevel::Finalized,
            )
            .await;
    }

    // check if 10 blocks are added
    for i in 1..11 {
        assert!(store.get(i).await.is_some());
    }
    // add 11th block
    store
        .save(
            11,
            create_test_ui_confirmed_block(11),
            CommitmentLevel::Finalized,
        )
        .await;

    // can get 11th block
    assert!(store.get(11).await.is_some());
    // first block is removed
    assert!(store.get(1).await.is_none());

    // cannot add old blocks
    store
        .save(
            1,
            create_test_ui_confirmed_block(1),
            CommitmentLevel::Finalized,
        )
        .await;
    assert!(store.get(1).await.is_none());
}
