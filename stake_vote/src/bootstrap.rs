use crate::epoch::ScheduleEpochData;
use solana_lite_rpc_core::stores::data_cache::DataCache;
use solana_lite_rpc_core::structures::leaderschedule::CalculatedSchedule;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use std::sync::Arc;

pub async fn bootstrap_process_data(
    data_cache: &DataCache,
    rpc_client: Arc<RpcClient>,
) -> (ScheduleEpochData, BootstrapData) {
    let new_rate_activation_epoch = solana_sdk::feature_set::FeatureSet::default()
        .new_warmup_cooldown_rate_epoch(data_cache.epoch_data.get_epoch_schedule());

    let bootstrap_epoch = crate::utils::get_current_epoch(data_cache).await;
    let current_schedule_epoch = ScheduleEpochData {
        current_epoch: bootstrap_epoch.epoch,
        slots_in_epoch: bootstrap_epoch.slots_in_epoch,
        last_slot_in_epoch: data_cache
            .epoch_data
            .get_last_slot_in_epoch(bootstrap_epoch.epoch),
        current_confirmed_slot: bootstrap_epoch.absolute_slot,
        new_rate_activation_epoch,
    };

    //Init bootstrap process
    let bootstrap_data = crate::bootstrap::BootstrapData {
        done: false,
        sleep_time: 1,
        rpc_client,
    };
    (current_schedule_epoch, bootstrap_data)
}

pub fn bootstrap_leader_schedule(
    current_file_patch: &str,
    next_file_patch: &str,
    slots_in_epoch: u64,
) -> anyhow::Result<CalculatedSchedule> {
    todo!();
}

pub struct BootstrapData {
    pub done: bool,
    pub sleep_time: u64,
    pub rpc_client: Arc<RpcClient>,
}
