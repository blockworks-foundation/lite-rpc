use crate::AccountPretty;
use crate::Slot;
use anyhow::bail;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::vote::state::VoteState;
use std::collections::HashMap;
use std::sync::Arc;

pub type VoteMap = HashMap<Pubkey, Arc<StoredVote>>;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct StoredVote {
    pub pubkey: Pubkey,
    pub vote_data: VoteState,
    pub last_update_slot: Slot,
    pub write_version: u64,
}

#[derive(Default)]
pub struct VoteStore {
    votes: VoteMap,
    updates: Vec<(Pubkey, StoredVote)>,
    extracted: bool,
}

impl VoteStore {
    pub fn new(capacity: usize) -> Self {
        VoteStore {
            votes: HashMap::with_capacity(capacity),
            updates: vec![],
            extracted: false,
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

            //during extract push the new update or
            //don't insertnow account change that has been done in next epoch.
            //put in update pool to be merged next epoch change.
            let insert_stake =
                !self.extracted || new_voteacc.last_update_slot > current_end_epoch_slot;
            match insert_stake {
                false => self.updates.push((new_account.pubkey, new_voteacc)),
                true => self.insert_vote(new_account.pubkey, new_voteacc),
            }
        }

        Ok(())
    }
    fn insert_vote(&mut self, vote_account: Pubkey, vote_data: StoredVote) {
        todo!();
    }

    fn remove_from_store(&mut self, account_pk: &Pubkey, update_slot: Slot) {
        if self
            .votes
            .get(account_pk)
            .map(|vote| vote.last_update_slot <= update_slot)
            .unwrap_or(true)
        {
            log::info!("Vote remove_from_store for {}", account_pk.to_string());
            self.votes.remove(account_pk);
        }
    }
}
