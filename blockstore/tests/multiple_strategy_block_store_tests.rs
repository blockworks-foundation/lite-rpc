use solana_lite_rpc_blockstore::block_stores::multiple_strategy_block_store::BlockStorageData;
use solana_lite_rpc_blockstore::block_stores::multiple_strategy_block_store::MultipleStrategyBlockStorage;
use solana_lite_rpc_blockstore::block_stores::postgres::postgres_block_store_query::PostgresQueryBlockStore;
use solana_lite_rpc_blockstore::block_stores::postgres::postgres_block_store_writer::PostgresBlockStore;
use solana_lite_rpc_blockstore::block_stores::postgres::BlockstorePostgresSessionConfig;
use solana_lite_rpc_core::structures::epoch::EpochCache;
use solana_lite_rpc_core::structures::produced_block::{ProducedBlock, ProducedBlockInner};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::reward_type::RewardType;
use solana_sdk::{commitment_config::CommitmentConfig, hash::Hash};
use solana_transaction_status::Reward;

pub fn create_test_block(slot: u64, commitment_config: CommitmentConfig) -> ProducedBlock {
    let inner = ProducedBlockInner {
        block_height: slot,
        blockhash: Hash::new_unique(),
        previous_blockhash: Hash::new_unique(),
        parent_slot: slot - 1,
        transactions: vec![],
        block_time: 0,
        leader_id: None,
        slot,
        rewards: Some(vec![Reward {
            pubkey: Pubkey::new_unique().to_string(),
            lamports: 5000,
            post_balance: 1000000,
            reward_type: Some(RewardType::Voting),
            commission: None,
        }]),
    };
    ProducedBlock::new(inner, commitment_config)
}

#[ignore = "need postgres database"]
#[tokio::test]
async fn test_in_multiple_stategy_block_store() {
    tracing_subscriber::fmt::init();

    let pg_session_config = BlockstorePostgresSessionConfig::new_from_env("BLOCKSTOREDB").unwrap();
    let epoch_cache = EpochCache::new_for_tests();
    let persistent_store =
        PostgresBlockStore::new(epoch_cache.clone(), pg_session_config.clone()).await;
    let block_storage_query = PostgresQueryBlockStore::new(epoch_cache, pg_session_config).await;
    let multi_store = MultipleStrategyBlockStorage::new(
        block_storage_query, // None, // not supported
    );

    persistent_store.prepare_epoch_schema(1200).await.unwrap();

    persistent_store
        .save_confirmed_block(&create_test_block(1200, CommitmentConfig::confirmed()))
        .await
        .unwrap();
    // span range of slots between those two
    persistent_store
        .save_confirmed_block(&create_test_block(1289, CommitmentConfig::confirmed()))
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

    let block_1200: BlockStorageData = multi_store.query_block(1200).await.unwrap();
    assert_eq!(1, block_1200.rewards.as_ref().unwrap().len());
    assert_eq!(
        5000,
        block_1200
            .rewards
            .as_ref()
            .unwrap()
            .first()
            .unwrap()
            .lamports
    );
}
