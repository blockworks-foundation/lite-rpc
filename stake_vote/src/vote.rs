use crate::utils::TakableContent;
use crate::utils::TakableMap;
use crate::utils::UpdateAction;
use crate::AccountPretty;
use crate::Slot;
use anyhow::bail;
use serde::{Deserialize, Serialize};
use solana_lite_rpc_core::structures::leaderschedule::GetVoteAccountsConfig;
use solana_rpc_client_api::request::MAX_RPC_VOTE_ACCOUNT_INFO_EPOCH_CREDITS_HISTORY;
use solana_rpc_client_api::response::RpcVoteAccountInfo;
use solana_rpc_client_api::response::RpcVoteAccountStatus;
use solana_sdk::account::Account;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::vote::state::VoteState;
use std::collections::HashMap;
use std::sync::Arc;

pub type VoteMap = HashMap<Pubkey, Arc<StoredVote>>;
pub type VoteContent = (VoteMap, EpochVoteStakesCache);

#[derive(Debug, Clone)]
pub struct EpochVoteStakes {
    pub vote_stakes: HashMap<Pubkey, (u64, Arc<StoredVote>)>,
    pub epoch: u64,
}

//TODO define the cache invalidation.
#[derive(Default)]
pub struct EpochVoteStakesCache {
    pub cache: HashMap<u64, EpochVoteStakes>,
}

impl EpochVoteStakesCache {
    pub fn vote_stakes_for_epoch(&self, epoch: u64) -> Option<&EpochVoteStakes> {
        self.cache.get(&epoch)
    }

    pub fn add_stakes_for_epoch(&mut self, vote_stakes: EpochVoteStakes) {
        log::debug!("add_stakes_for_epoch :{}", vote_stakes.epoch);
        self.cache.insert(vote_stakes.epoch, vote_stakes);
    }
}

impl TakableContent<StoredVote> for VoteContent {
    fn add_value(&mut self, val: UpdateAction<StoredVote>) {
        VoteStore::process_vote_action(&mut self.0, val);
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct StoredVote {
    pub pubkey: Pubkey,
    pub vote_data: VoteState,
    pub last_update_slot: Slot,
    pub write_version: u64,
}

impl StoredVote {
    pub fn convert_to_rpc_vote_account_info(
        &self,
        activated_stake: u64,
        epoch_vote_account: bool,
    ) -> RpcVoteAccountInfo {
        let last_vote = self
            .vote_data
            .votes
            .iter()
            .last()
            .map(|vote| vote.slot())
            .unwrap_or_default();

        RpcVoteAccountInfo {
            vote_pubkey: self.pubkey.to_string(),
            node_pubkey: self.vote_data.node_pubkey.to_string(),
            activated_stake,
            commission: self.vote_data.commission,
            epoch_vote_account,
            epoch_credits: self.vote_data.epoch_credits.clone(),
            last_vote,
            root_slot: self.vote_data.root_slot.unwrap_or_default(),
        }
    }
}

#[derive(Default)]
pub struct VoteStore {
    pub votes: TakableMap<StoredVote, VoteContent>,
}

impl VoteStore {
    pub fn new(capacity: usize) -> Self {
        VoteStore {
            votes: TakableMap::new((
                HashMap::with_capacity(capacity),
                EpochVoteStakesCache::default(),
            )),
        }
    }

    pub fn notify_vote_change(
        &mut self,
        new_account: AccountPretty,
        current_end_epoch_slot: Slot,
    ) -> anyhow::Result<()> {
        if new_account.lamports == 0 {
            //self.remove_from_store(&new_account.pubkey, new_account.slot);
            self.votes.add_value(
                UpdateAction::Remove(new_account.pubkey, new_account.slot),
                new_account.slot > current_end_epoch_slot,
            );
        } else {
            let Ok(mut vote_data) = new_account.read_vote() else {
                bail!("Can't read Vote from account data");
            };

            //remove unnecessary entry. See Solana code rpc::rpc::get_vote_accounts
            let epoch_credits = vote_data.epoch_credits();
            vote_data.epoch_credits =
                if epoch_credits.len() > MAX_RPC_VOTE_ACCOUNT_INFO_EPOCH_CREDITS_HISTORY {
                    epoch_credits
                        .iter()
                        .skip(epoch_credits.len().saturating_sub(MAX_RPC_VOTE_ACCOUNT_INFO_EPOCH_CREDITS_HISTORY))
                        .cloned()
                        .collect()
                } else {
                    epoch_credits.clone()
                };

            //log::info!("add_vote {} :{vote_data:?}", new_account.pubkey);

            let new_voteacc = StoredVote {
                pubkey: new_account.pubkey,
                vote_data,
                last_update_slot: new_account.slot,
                write_version: new_account.write_version,
            };

            let action_update_slot = new_voteacc.last_update_slot;
            self.votes.add_value(
                UpdateAction::Notify(action_update_slot, new_voteacc),
                action_update_slot > current_end_epoch_slot,
            );
        }

        Ok(())
    }

    fn process_vote_action(votes: &mut VoteMap, action: UpdateAction<StoredVote>) {
        match action {
            UpdateAction::Notify(_, vote) => {
                Self::vote_map_insert_vote(votes, vote);
            }
            UpdateAction::Remove(account_pk, slot) => {
                Self::remove_from_store(votes, &account_pk, slot)
            }
        }
    }

    fn remove_from_store(votes: &mut VoteMap, account_pk: &Pubkey, update_slot: Slot) {
        //TODO use action.
        if votes
            .get(account_pk)
            .map(|vote| vote.last_update_slot <= update_slot)
            .unwrap_or(true)
        {
            log::info!("Vote remove_from_store for {}", account_pk.to_string());
            votes.remove(account_pk);
        }
    }

    fn vote_map_insert_vote(map: &mut VoteMap, vote_data: StoredVote) {
        let vote_account_pk = vote_data.pubkey;
        match map.entry(vote_account_pk) {
            std::collections::hash_map::Entry::Occupied(occupied) => {
                let voteacc = occupied.into_mut(); // <-- get mut reference to existing value
                if voteacc.last_update_slot <= vote_data.last_update_slot {
                    // generate a lot of trace log::trace!(
                    //     "Vote updated for: {vote_account_pk} node_id:{} root_slot:{:?}",
                    //     vote_data.vote_data.node_pubkey,
                    //     vote_data.vote_data.root_slot,
                    // );
                    // if vote_data.vote_data.root_slot.is_none() {
                    //     log::info!("Update vote account:{vote_account_pk} with None root slot.");
                    // }

                    // if voteacc.vote_data.root_slot.is_none() {
                    //     log::info!(
                    //         "Update vote account:{vote_account_pk} that were having None root slot."
                    //     );
                    // }

                    *voteacc = Arc::new(vote_data);
                }
            }
            // If value doesn't exist yet, then insert a new value of 1
            std::collections::hash_map::Entry::Vacant(vacant) => {
                log::trace!(
                    "New Vote added for: {vote_account_pk} node_id:{}, root slot:{:?}",
                    vote_data.vote_data.node_pubkey,
                    vote_data.vote_data.root_slot,
                );
                vacant.insert(Arc::new(vote_data));
            }
        };
    }
}

pub fn merge_program_account_in_vote_map(
    vote_map: &mut VoteMap,
    pa_list: Vec<(Pubkey, Account)>,
    last_update_slot: Slot,
) {
    pa_list
        .into_iter()
        .filter_map(
            |(pk, account)| match VoteState::deserialize(&account.data) {
                Ok(vote) => Some((pk, vote)),
                Err(err) => {
                    log::warn!("Error during vote account data deserialisation:{err}");
                    None
                }
            },
        )
        .for_each(|(pk, vote)| {
            //log::info!("Vote init {pk} :{vote:?}");
            let vote = StoredVote {
                pubkey: pk,
                vote_data: vote,
                last_update_slot,
                write_version: 0,
            };
            VoteStore::vote_map_insert_vote(vote_map, vote);
        });
}

// Validators that are this number of slots behind are considered delinquent
pub fn get_rpc_vote_accounts_info(
    current_slot: Slot,
    votes: &VoteMap,
    vote_accounts: &HashMap<Pubkey, (u64, Arc<StoredVote>)>,
    config: GetVoteAccountsConfig,
) -> RpcVoteAccountStatus {
    pub const DELINQUENT_VALIDATOR_SLOT_DISTANCE: u64 =
        solana_rpc_client_api::request::DELINQUENT_VALIDATOR_SLOT_DISTANCE;
    let delinquent_validator_slot_distance = config
        .delinquent_slot_distance
        .unwrap_or(DELINQUENT_VALIDATOR_SLOT_DISTANCE);
    //From Solana rpc::rpc::metaz::get_vote_accounts() code.
    let (current_vote_accounts, delinquent_vote_accounts): (
        Vec<RpcVoteAccountInfo>,
        Vec<RpcVoteAccountInfo>,
    ) = votes
        .values()
        .map(|vote| {
            let (stake, epoch_vote_account) = vote_accounts
                .get(&vote.pubkey)
                .map(|(stake, _)| (*stake, true))
                .unwrap_or((0, false));
            vote.convert_to_rpc_vote_account_info(stake, epoch_vote_account)
        })
        .partition(|vote_account_info| {
            if current_slot >= delinquent_validator_slot_distance {
                vote_account_info.last_vote > current_slot.saturating_sub(delinquent_validator_slot_distance)
            } else {
                vote_account_info.last_vote > 0
            }
        });
    let keep_unstaked_delinquents = config.keep_unstaked_delinquents.unwrap_or_default();
    let delinquent_vote_accounts = if !keep_unstaked_delinquents {
        delinquent_vote_accounts
            .into_iter()
            .filter(|vote_account_info| vote_account_info.activated_stake > 0)
            .collect::<Vec<_>>()
    } else {
        delinquent_vote_accounts
    };

    RpcVoteAccountStatus {
        current: current_vote_accounts,
        delinquent: delinquent_vote_accounts,
    }
}

pub fn get_rpc_vote_account_info_from_current_epoch_stakes(
    current_epoch_stakes: &EpochVoteStakes,
) -> RpcVoteAccountStatus {
    let current_vote_accounts: Vec<RpcVoteAccountInfo> = current_epoch_stakes
        .vote_stakes
        .values()
        .map(|(stake, vote)| vote.convert_to_rpc_vote_account_info(*stake, true))
        .collect();
    RpcVoteAccountStatus {
        current: current_vote_accounts,
        delinquent: vec![], //no info about delinquent at startup.
    }
}
