use crate::stores::block_information_store::BlockInformation;
use crate::stores::data_cache::DataCache;
use solana_rpc_client_api::config::RpcGetVoteAccountsConfig;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::ParsePubkeyError;
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use std::str::FromStr;

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
        data_cache: &DataCache,
    ) -> Option<HashMap<String, Vec<usize>>> {
        let commitment = commitment.unwrap_or_else(CommitmentConfig::default);
        let slot = match slot {
            Some(slot) => slot,
            None => {
                let BlockInformation { slot, .. } = data_cache
                    .block_information_store
                    .get_latest_block(commitment)
                    .await;
                slot
            }
        };
        let epoch = data_cache.epoch_data.get_epoch_at_slot(slot);

        let get_schedule = |schedule_data: Option<&LeaderScheduleData>| {
            schedule_data.and_then(|current| {
                (current.epoch == epoch.epoch).then_some(current.schedule_by_node.clone())
            })
        };
        get_schedule(self.current.as_ref()).or_else(|| get_schedule(self.next.as_ref()))
    }
}

#[derive(Clone, Debug)]
pub struct LeaderScheduleData {
    pub schedule_by_node: HashMap<String, Vec<usize>>,
    pub schedule_by_slot: Vec<Pubkey>,
    pub epoch: u64,
}
