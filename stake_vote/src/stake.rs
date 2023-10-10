use crate::AccountPretty;
use crate::Slot;
use anyhow::bail;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::stake::state::Delegation;
use solana_sdk::stake_history::StakeHistory;
use std::collections::HashMap;

pub type StakeMap = HashMap<Pubkey, StoredStake>;

#[derive(Debug, Default)]
pub enum StakeAction {
    Notify {
        stake: StoredStake,
    },
    Remove(Pubkey, Slot),
    // Merge {
    //     source_account: Pubkey,
    //     destination_account: Pubkey,
    //     update_slot: Slot,
    // },
    #[default]
    None,
}

impl StakeAction {
    fn get_update_slot(&self) -> u64 {
        match self {
            StakeAction::Notify { stake } => stake.last_update_slot,
            StakeAction::Remove(_, slot) => *slot,
            StakeAction::None => 0,
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct StoredStake {
    pub pubkey: Pubkey,
    pub lamports: u64,
    pub stake: Delegation,
    pub last_update_slot: Slot,
    pub write_version: u64,
}

#[derive(Debug, Default)]
pub struct StakeStore {
    stakes: StakeMap,
    stake_history: Option<StakeHistory>,
    pub updates: Vec<StakeAction>,
    pub extracted: bool,
}

impl StakeStore {
    pub fn new(capacity: usize) -> Self {
        StakeStore {
            stakes: HashMap::with_capacity(capacity),
            stake_history: None,
            updates: vec![],
            extracted: false,
        }
    }

    pub fn get_stake_history(&self) -> Option<StakeHistory> {
        self.stake_history.clone()
    }

    pub fn notify_stake_change(
        &mut self,
        account: AccountPretty,
        current_end_epoch_slot: Slot,
    ) -> anyhow::Result<()> {
        //if lamport == 0 the account has been removed.
        if account.lamports == 0 {
            self.notify_stake_action(
                StakeAction::Remove(account.pubkey, account.slot),
                current_end_epoch_slot,
            );
        } else {
            let Ok(delegated_stake_opt) = account.read_stake() else {
                bail!("Can't read stake from account data");
            };

            if let Some(delegated_stake) = delegated_stake_opt {
                let stake = StoredStake {
                    pubkey: account.pubkey,
                    lamports: account.lamports,
                    stake: delegated_stake,
                    last_update_slot: account.slot,
                    write_version: account.write_version,
                };

                self.notify_stake_action(StakeAction::Notify { stake }, current_end_epoch_slot);
            }
        }

        Ok(())
    }
    pub fn notify_stake_action(&mut self, action: StakeAction, current_end_epoch_slot: Slot) {
        //during extract push the new update or
        //don't insertnow account change that has been done in next epoch.
        //put in update pool to be merged next epoch change.
        let insert_stake = !self.extracted || action.get_update_slot() > current_end_epoch_slot;
        match insert_stake {
            false => self.updates.push(action),
            true => self.process_stake_action(action),
        }
    }

    fn process_stake_action(&mut self, action: StakeAction) {
        match action {
            StakeAction::Notify { stake } => {
                todo!();
            }
            StakeAction::Remove(account_pk, slot) => self.remove_from_store(&account_pk, slot),
            StakeAction::None => (),
        }
    }

    fn remove_from_store(&mut self, account_pk: &Pubkey, update_slot: Slot) {
        if self
            .stakes
            .get(account_pk)
            .map(|stake| stake.last_update_slot <= update_slot)
            .unwrap_or(false)
        {
            log::info!("Stake remove_from_store for {}", account_pk.to_string());
            self.stakes.remove(account_pk);
        }
    }
}
