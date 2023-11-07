use crate::utils::TakableContent;
use crate::utils::TakableMap;
use crate::utils::UpdateAction;
use crate::AccountPretty;
use crate::Slot;
use anyhow::bail;
use serde::{Deserialize, Serialize};
use solana_sdk::account::Account;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::stake::state::Delegation;
use std::collections::HashMap;

pub type StakeMap = HashMap<Pubkey, StoredStake>;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct StoredStake {
    pub pubkey: Pubkey,
    pub lamports: u64,
    pub stake: Delegation,
    pub last_update_slot: Slot,
    pub write_version: u64,
}

impl TakableContent<StoredStake> for StakeMap {
    fn add_value(&mut self, val: UpdateAction<StoredStake>) {
        StakeStore::process_stake_action(self, val);
    }
}

#[derive(Debug, Default)]
pub struct StakeStore {
    pub stakes: TakableMap<StoredStake, StakeMap>,
}

impl StakeStore {
    pub fn new(capacity: usize) -> Self {
        StakeStore {
            stakes: TakableMap::new(HashMap::with_capacity(capacity)),
        }
    }

    pub fn notify_stake_change(
        &mut self,
        account: AccountPretty,
        current_end_epoch_slot: Slot,
    ) -> anyhow::Result<()> {
        //if lamport == 0 the account has been removed.
        if account.lamports == 0 {
            self.stakes.add_value(
                UpdateAction::Remove(account.pubkey, account.slot),
                account.slot <= current_end_epoch_slot,
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

                let action_update_slot = stake.last_update_slot;
                self.stakes.add_value(
                    UpdateAction::Notify(action_update_slot, stake),
                    action_update_slot <= current_end_epoch_slot,
                );
            }
        }

        Ok(())
    }

    fn process_stake_action(stakes: &mut StakeMap, action: UpdateAction<StoredStake>) {
        match action {
            UpdateAction::Notify(_, stake) => {
                Self::notify_stake(stakes, stake);
            }
            UpdateAction::Remove(account_pk, slot) => Self::remove_stake(stakes, &account_pk, slot),
        }
    }
    fn notify_stake(map: &mut StakeMap, stake: StoredStake) {
        //log::info!("stake_map_notify_stake stake:{stake:?}");
        match map.entry(stake.pubkey) {
            // If value already exists, then increment it by one
            std::collections::hash_map::Entry::Occupied(occupied) => {
                let strstake = occupied.into_mut(); // <-- get mut reference to existing value
                                                    //doesn't erase new state with an old one. Can arrive during bootstrapping.
                                                    //several instructions can be done in the same slot.
                if strstake.last_update_slot <= stake.last_update_slot {
                    log::info!("stake_map_notify_stake Stake store updated stake: {} old_stake:{strstake:?} stake:{stake:?}", stake.pubkey);
                    *strstake = stake;
                }
            }
            // If value doesn't exist yet, then insert a new value of 1
            std::collections::hash_map::Entry::Vacant(vacant) => {
                log::info!(
                    "stake_map_notify_stake Stake store insert stake: {} stake:{stake:?}",
                    stake.pubkey
                );
                vacant.insert(stake);
            }
        };
    }

    fn remove_stake(stakes: &mut StakeMap, account_pk: &Pubkey, update_slot: Slot) {
        if stakes
            .get(account_pk)
            .map(|stake| stake.last_update_slot <= update_slot)
            .unwrap_or(false)
        {
            log::info!("Stake remove_from_store for {}", account_pk.to_string());
            stakes.remove(account_pk);
        }
    }
}

pub fn merge_program_account_in_strake_map(
    stake_map: &mut StakeMap,
    stakes_list: Vec<(Pubkey, Account)>,
    last_update_slot: Slot,
) {
    stakes_list
        .into_iter()
        .filter_map(|(pk, account)| {
            match crate::account::read_stake_from_account_data(&account.data) {
                Ok(opt_stake) => opt_stake.map(|stake| (pk, stake, account.lamports)),
                Err(err) => {
                    log::warn!("Error during pa account data deserialisation:{err}");
                    None
                }
            }
        })
        .for_each(|(pk, delegated_stake, lamports)| {
            let stake = StoredStake {
                pubkey: pk,
                lamports,
                stake: delegated_stake,
                last_update_slot,
                write_version: 0,
            };

            StakeStore::notify_stake(stake_map, stake);
        });
}
