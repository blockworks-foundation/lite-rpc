use solana_lite_rpc_blocks_processing::block_information_store::BlockInformation;
use solana_lite_rpc_blocks_processing::block_information_store::BlockInformationStore;
use solana_rpc_client_api::config::RpcGetVoteAccountsConfig;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::ParsePubkeyError;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::slot_history::Slot;
use solana_sdk::sysvar::epoch_schedule::EpochSchedule;
use std::collections::HashMap;
use std::str::FromStr;

use crate::epoch::EpochCache;

#[derive(Clone, Default)]
pub struct GetVoteAccountsConfig {
    pub vote_pubkey: Option<Pubkey>,
    pub commitment: Option<CommitmentConfig>,
    pub keep_unstaked_delinquents: Option<bool>,
    pub delinquent_slot_distance: Option<u64>,
}

impl TryFrom<RpcGetVoteAccountsConfig> for GetVoteAccountsConfig {
    type Error = ParsePubkeyError;

    fn try_from(config: RpcGetVoteAccountsConfig) -> Result<Self, Self::Error> {
        let vote_pubkey = config
            .vote_pubkey
            .as_ref()
            .map(|pk| Pubkey::from_str(pk))
            .transpose()?;
        Ok(GetVoteAccountsConfig {
            vote_pubkey,
            commitment: config.commitment,
            keep_unstaked_delinquents: config.keep_unstaked_delinquents,
            delinquent_slot_distance: config.delinquent_slot_distance,
        })
    }
}

#[derive(Clone, Default, Debug)]
pub struct CalculatedSchedule {
    pub current: Option<LeaderScheduleData>,
    pub next: Option<LeaderScheduleData>,
}

impl CalculatedSchedule {
    pub async fn get_leader_schedule_for_slot(
        &self,
        slot: Option<u64>,
        commitment: Option<CommitmentConfig>,
        block_information_store: &BlockInformationStore,
        epoch_cache: &EpochCache,
    ) -> Option<HashMap<String, Vec<usize>>> {
        log::debug!(
            "get_leader_schedule_for_slot current:{:?} next:{:?} ",
            self.current.clone().unwrap_or_default(),
            self.next.clone().unwrap_or_default()
        );

        let commitment = commitment.unwrap_or_default();
        let slot = match slot {
            Some(slot) => slot,
            None => {
                let BlockInformation { slot, .. } = block_information_store
                    .get_latest_block(commitment)
                    .await;
                slot
            }
        };
        let epoch = epoch_cache.get_epoch_at_slot(slot);

        let get_schedule = |schedule_data: Option<&LeaderScheduleData>| {
            schedule_data.and_then(|current| {
                (current.epoch == epoch.epoch).then_some(current.schedule_by_node.clone())
            })
        };
        get_schedule(self.current.as_ref()).or_else(|| get_schedule(self.next.as_ref()))
    }

    pub async fn get_slot_leaders(
        &self,
        start_slot: Slot,
        limit: u64,
        epock_schedule: &EpochSchedule,
    ) -> Result<Vec<Pubkey>, String> {
        log::debug!(
            "get_slot_leaders rpc request received (start: {} limit: {})",
            start_slot,
            limit
        );
        pub const MAX_GET_SLOT_LEADERS: usize =
            solana_rpc_client_api::request::MAX_GET_SLOT_LEADERS;

        let mut limit = limit as usize;
        if limit > MAX_GET_SLOT_LEADERS {
            return Err(format!(
                "Invalid Params: Invalid limit; max {MAX_GET_SLOT_LEADERS}"
            ));
        }

        let (epoch, slot_index) = epock_schedule.get_epoch_and_slot_index(start_slot);
        let mut slot_leaders = Vec::with_capacity(limit);

        let mut extend_slot_from_epoch = |leader_schedule: &[Pubkey], slot_index: usize| {
            let take = limit.saturating_sub(slot_leaders.len());
            slot_leaders.extend(leader_schedule.iter().skip(slot_index).take(take));
            limit -= slot_leaders.len();
        };

        // log::info!(
        //     "get_slot_leaders  epoch:{epoch} current:{:?} next:{:?} ",
        //     self.current.clone().unwrap_or_default(),
        //     self.next.clone().unwrap_or_default()
        // );

        //TODO manage more leader schedule data in storage.
        //Here  only search  on current and next epoch
        let res = [
            (&self.current, slot_index as usize, epoch),
            (&self.next, slot_index as usize, epoch),
            (&self.next, 0, epoch + 1),
        ]
        .into_iter()
        .filter_map(|(epoch_data, slot_index, epoch)| {
            epoch_data.as_ref().and_then(|epoch_data| {
                (epoch_data.epoch == epoch).then_some((epoch_data, slot_index))
            })
        })
        .map(|(epoch_data, slot_index)| {
            extend_slot_from_epoch(&epoch_data.schedule_by_slot, slot_index);
        })
        .collect::<Vec<()>>();
        match res.is_empty()  {
            true => Err(format!(
                "Invalid Params: Invalid slot range: leader schedule for epoch {epoch} is unavailable"
            )),
            false => Ok(slot_leaders),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct LeaderScheduleData {
    pub schedule_by_node: HashMap<String, Vec<usize>>,
    pub schedule_by_slot: Vec<Pubkey>,
    pub epoch: u64,
}
