use super::takable_map::{TakableContent, TakableMap, UpdateAction};
use crate::structures::{account_pretty::AccountPretty, stored_vote::StoredVote};
use anyhow::bail;
use dashmap::DashMap;
use solana_rpc_client_api::request::MAX_RPC_VOTE_ACCOUNT_INFO_EPOCH_CREDITS_HISTORY;
use solana_sdk::{pubkey::Pubkey, slot_history::Slot};
use std::sync::Arc;

pub type VoteMap = Arc<DashMap<Pubkey, StoredVote>>;
pub type VoteContent = (VoteMap, EpochVoteStakesCache);
pub const VOTESTORE_INITIAL_CAPACITY: usize = 600000;

#[derive(Debug, Clone)]
pub struct EpochVoteStakes {
    pub vote_stakes: DashMap<Pubkey, (u64, StoredVote)>,
    pub epoch: u64,
}

#[derive(Default, Clone)]
pub struct EpochVoteStakesCache {
    pub cache: Arc<DashMap<u64, Arc<EpochVoteStakes>>>,
}

impl EpochVoteStakesCache {
    pub fn vote_stakes_for_epoch(&self, epoch: u64) -> Option<Arc<EpochVoteStakes>> {
        self.cache.get(&epoch).map(|iter| iter.value().clone())
    }

    pub fn add_stakes_for_epoch(&mut self, vote_stakes: EpochVoteStakes) {
        log::debug!("add_stakes_for_epoch :{}", vote_stakes.epoch);
        self.cache.insert(vote_stakes.epoch, Arc::new(vote_stakes));
    }
}

impl TakableContent<StoredVote> for VoteContent {
    fn add_value(&mut self, val: UpdateAction<StoredVote>) {
        VoteStore::process_vote_action(&mut self.0, val);
    }
}

#[derive(Default, Clone)]
pub struct VoteStore {
    pub votes: TakableMap<StoredVote, VoteContent>,
}

impl VoteStore {
    pub fn new(capacity: usize) -> Self {
        VoteStore {
            votes: TakableMap::new((
                Arc::new(DashMap::with_capacity(capacity)),
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
                        .skip(epoch_credits.len() - MAX_RPC_VOTE_ACCOUNT_INFO_EPOCH_CREDITS_HISTORY)
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

    pub fn vote_map_insert_vote(map: &mut VoteMap, vote_data: StoredVote) {
        let vote_account_pk = vote_data.pubkey;
        match map.entry(vote_account_pk) {
            dashmap::mapref::entry::Entry::Occupied(mut occupied) => {
                let voteacc = occupied.get_mut(); // <-- get mut reference to existing value
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

                    *voteacc = vote_data;
                }
            }
            // If value doesn't exist yet, then insert a new value of 1
            dashmap::mapref::entry::Entry::Vacant(vacant) => {
                log::trace!(
                    "New Vote added for: {vote_account_pk} node_id:{}, root slot:{:?}",
                    vote_data.vote_data.node_pubkey,
                    vote_data.vote_data.root_slot,
                );
                vacant.insert(vote_data);
            }
        };
    }
}
