use solana_lite_rpc_core::stores::block_information_store::BlockInformation;
use solana_lite_rpc_core::stores::data_cache::DataCache;
use solana_lite_rpc_core::structures::epoch::Epoch as LiteRpcEpoch;
use solana_sdk::commitment_config::CommitmentConfig;

pub async fn get_current_confirmed_slot(data_cache: &DataCache) -> u64 {
    let commitment = CommitmentConfig::confirmed();
    let BlockInformation { slot, .. } = data_cache
        .block_information_store
        .get_latest_block(commitment)
        .await;
    slot
}

pub async fn get_current_epoch(data_cache: &DataCache) -> LiteRpcEpoch {
    let commitment = CommitmentConfig::confirmed();
    data_cache.get_current_epoch(commitment).await
}
