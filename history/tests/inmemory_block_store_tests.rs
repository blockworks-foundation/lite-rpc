use solana_lite_rpc_core::{
    structures::produced_block::ProducedBlock,
    traits::block_storage_interface::BlockStorageInterface,
};
use solana_lite_rpc_history::block_stores::inmemory_block_store::InmemoryBlockStore;
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
        rewards: None,
    }
}

#[tokio::test]
async fn inmemory_block_store_tests() {
    // will store only 10 blocks
    let store: Arc<dyn BlockStorageInterface> = Arc::new(InmemoryBlockStore::new(10));

    // add 10 blocks
    for i in 1..11 {
        store
            .save(&create_test_block(i, CommitmentConfig::finalized()))
            .await
            .unwrap();
    }

    // check if 10 blocks are added
    for i in 1..11 {
        assert!(store.get(i).await.ok().is_some());
    }
    // add 11th block
    store
        .save(&create_test_block(11, CommitmentConfig::finalized()))
        .await
        .unwrap();

    // can get 11th block
    assert!(store.get(11).await.ok().is_some());
    // first block is removed
    assert!(store.get(1).await.ok().is_none());

    // cannot add old blocks
    store
        .save(&create_test_block(1, CommitmentConfig::finalized()))
        .await
        .unwrap();
    assert!(store.get(1).await.ok().is_none());
}
