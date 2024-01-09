use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use solana_lite_rpc_core::stores::data_cache::DataCache;
use solana_lite_rpc_core::structures::epoch::Epoch as LiteRpcEpoch;
use solana_lite_rpc_core::structures::stored_vote::StoredVote;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use std::default::Default;
use std::fs::File;
use std::io::Write;
use std::str::FromStr;

pub async fn get_current_epoch(data_cache: &DataCache) -> LiteRpcEpoch {
    let commitment = CommitmentConfig::confirmed();
    data_cache.get_current_epoch(commitment).await
}

//Read save epoch vote stake to bootstrap current leader shedule and get_vote_account.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct StringSavedStake {
    epoch: u64,
    stake_vote_map: HashMap<String, (u64, StoredVote)>,
}

#[allow(clippy::type_complexity)]
pub fn read_schedule_vote_stakes(
    file_path: &str,
) -> anyhow::Result<(u64, DashMap<Pubkey, (u64, StoredVote)>)> {
    let content = std::fs::read_to_string(file_path)?;
    let stakes_str: StringSavedStake = serde_json::from_str(&content)?;
    //convert to EpochStake because json hashmap parser can only have String key.
    let ret_stakes = stakes_str
        .stake_vote_map
        .into_iter()
        .map(|(pk, st)| (Pubkey::from_str(&pk).unwrap(), (st.0, st.1)))
        .collect();
    Ok((stakes_str.epoch, ret_stakes))
}

pub fn save_schedule_vote_stakes(
    base_file_path: &str,
    stake_vote_map: &DashMap<Pubkey, (u64, StoredVote)>,
    epoch: u64,
) -> anyhow::Result<()> {
    //save new schedule for restart.
    //need to convert hahsmap key to String because json aloow only string
    //key for dictionnary.
    //it's better to use json because the file can use to very some stake by hand.
    //in the end it will be removed with the bootstrap process.
    let save_stakes = StringSavedStake {
        epoch,
        stake_vote_map: stake_vote_map
            .iter()
            .map(|iter| (iter.key().to_string(), (iter.0, iter.1.clone())))
            .collect::<HashMap<_, _>>(),
    };
    let serialized_stakes = serde_json::to_string(&save_stakes).unwrap();
    let mut file = File::create(base_file_path).unwrap();
    file.write_all(serialized_stakes.as_bytes()).unwrap();
    file.flush().unwrap();
    Ok(())
}
