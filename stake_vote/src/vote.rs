use crate::utils::TakableContent;
use crate::utils::TakableMap;
use crate::AccountPretty;
use crate::Slot;
use anyhow::bail;
use serde::{Deserialize, Serialize};
use solana_sdk::account::Account;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::vote::state::VoteState;
use std::collections::HashMap;
use std::sync::Arc;

pub type VoteMap = HashMap<Pubkey, Arc<StoredVote>>;

impl TakableContent<StoredVote> for VoteMap {
    fn add_value(&mut self, val: StoredVote) {
        VoteStore::vote_map_insert_vote(self, val.pubkey, val);
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct StoredVote {
    pub pubkey: Pubkey,
    pub vote_data: VoteState,
    pub last_update_slot: Slot,
    pub write_version: u64,
}

#[derive(Default)]
pub struct VoteStore {
    votes: TakableMap<StoredVote, VoteMap>,
}

impl VoteStore {
    pub fn new(capacity: usize) -> Self {
        VoteStore {
            votes: TakableMap::new(HashMap::with_capacity(capacity)),
        }
    }
    pub fn add_vote(
        &mut self,
        new_account: AccountPretty,
        current_end_epoch_slot: Slot,
    ) -> anyhow::Result<()> {
        if new_account.lamports == 0 {
            self.remove_from_store(&new_account.pubkey, new_account.slot);
        } else {
            let Ok(vote_data) = new_account.read_vote() else {
                bail!("Can't read Vote from account data");
            };

            //log::info!("add_vote {} :{vote_data:?}", new_account.pubkey);

            let new_voteacc = StoredVote {
                pubkey: new_account.pubkey,
                vote_data,
                last_update_slot: new_account.slot,
                write_version: new_account.write_version,
            };

            let action_update_slot = new_voteacc.last_update_slot;
            self.votes
                .add_value(new_voteacc, action_update_slot <= current_end_epoch_slot);
        }

        Ok(())
    }
    //helper method to extract and merge stakes.
    pub fn take_votestore(votestore: &mut VoteStore) -> anyhow::Result<VoteMap> {
        crate::utils::take(&mut votestore.votes)
    }

    pub fn merge_votestore(votestore: &mut VoteStore, vote_map: VoteMap) -> anyhow::Result<()> {
        crate::utils::merge(&mut votestore.votes, vote_map)
    }

    fn remove_from_store(&mut self, account_pk: &Pubkey, update_slot: Slot) {
        if self
            .votes
            .content
            .get(account_pk)
            .map(|vote| vote.last_update_slot <= update_slot)
            .unwrap_or(true)
        {
            log::info!("Vote remove_from_store for {}", account_pk.to_string());
            self.votes.content.remove(account_pk);
        }
    }

    fn vote_map_insert_vote(map: &mut VoteMap, vote_account_pk: Pubkey, vote_data: StoredVote) {
        match map.entry(vote_account_pk) {
            std::collections::hash_map::Entry::Occupied(occupied) => {
                let voteacc = occupied.into_mut(); // <-- get mut reference to existing value
                if voteacc.last_update_slot <= vote_data.last_update_slot {
                    // generate a lot of trace log::trace!(
                    //     "Vote updated for: {vote_account_pk} node_id:{} root_slot:{:?}",
                    //     vote_data.vote_data.node_pubkey,
                    //     vote_data.vote_data.root_slot,
                    // );
                    if vote_data.vote_data.root_slot.is_none() {
                        log::info!("Update vote account:{vote_account_pk} with None root slot.");
                    }

                    if voteacc.vote_data.root_slot.is_none() {
                        log::info!(
                            "Update vote account:{vote_account_pk} that were having None root slot."
                        );
                    }

                    *voteacc = Arc::new(vote_data);
                }
            }
            // If value doesn't exist yet, then insert a new value of 1
            std::collections::hash_map::Entry::Vacant(vacant) => {
                log::info!(
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
            VoteStore::vote_map_insert_vote(vote_map, pk, vote);
        });
}
