use std::{collections::HashSet, sync::Arc};

use crate::account_store_interface::{AccountLoadingError, AccountStorageInterface};
use async_trait::async_trait;
use dashmap::DashMap;
use prometheus::{opts, register_int_gauge, IntGauge};
use solana_lite_rpc_core::{commitment_utils::Commitment, structures::account_data::AccountData};
use solana_rpc_client_api::filter::RpcFilterType;
use solana_sdk::{pubkey::Pubkey, slot_history::Slot};
use std::collections::BTreeMap;
use tokio::sync::Mutex;

lazy_static::lazy_static! {
    static ref ACCOUNT_STORED_IN_MEMORY: IntGauge =
       register_int_gauge!(opts!("literpc_accounts_in_memory", "Account InMemory")).unwrap();

    static ref TOTAL_PROCESSED_ACCOUNTS: IntGauge =
        register_int_gauge!(opts!("literpc_total_processed_accounts_in_memory", "Account processed accounts InMemory")).unwrap();

    static ref SLOT_FOR_LATEST_ACCOUNT_UPDATE: IntGauge =
        register_int_gauge!(opts!("literpc_slot_for_latest_account_update", "Slot of latest account update")).unwrap();
}

#[derive(Default)]
pub struct AccountDataByCommitment {
    pub pubkey: Pubkey,
    // should have maximum 32 entries, all processed slots which are not yet finalized
    pub processed_accounts: BTreeMap<Slot, AccountData>,
    pub confirmed_account: Option<AccountData>,
    pub finalized_account: Option<AccountData>,
}

impl AccountDataByCommitment {
    #[allow(deprecated)]
    pub fn get_account_data(&self, commitment: Commitment) -> Option<AccountData> {
        match commitment {
            Commitment::Processed => self
                .processed_accounts
                .last_key_value()
                .map(|(_, v)| v.clone())
                .or(self.confirmed_account.clone()),
            Commitment::Confirmed => self.confirmed_account.clone(),
            Commitment::Finalized => self.finalized_account.clone(),
        }
    }

    // Should be used when accounts is created by geyser notification
    pub fn new(data: AccountData, commitment: Commitment) -> Self {
        let mut processed_accounts = BTreeMap::new();
        processed_accounts.insert(data.updated_slot, data.clone());
        AccountDataByCommitment {
            pubkey: data.pubkey,
            processed_accounts,
            confirmed_account: if commitment == Commitment::Confirmed
                || commitment == Commitment::Finalized
            {
                Some(data.clone())
            } else {
                None
            },
            finalized_account: if commitment == Commitment::Finalized {
                Some(data)
            } else {
                None
            },
        }
    }

    // should be called with finalized accounts data
    // when accounts storage is being warmed up
    pub fn initialize(data: AccountData) -> Self {
        let mut processed_accounts = BTreeMap::new();
        processed_accounts.insert(data.updated_slot, data.clone());
        AccountDataByCommitment {
            pubkey: data.pubkey,
            processed_accounts,
            confirmed_account: Some(data.clone()),
            finalized_account: Some(data),
        }
    }

    pub fn update(&mut self, data: AccountData, commitment: Commitment) -> bool {
        // if commitmentment is processed check and update processed
        // if commitmentment is confirmed check and update processed and confirmed
        // if commitmentment is finalized check and update all
        let update_confirmed = self
            .confirmed_account
            .as_ref()
            .map(|x| x.updated_slot < data.updated_slot)
            .unwrap_or(true);
        let update_finalized = self
            .finalized_account
            .as_ref()
            .map(|x| x.updated_slot < data.updated_slot)
            .unwrap_or(true);

        let mut updated = false;
        // processed not present for the slot
        // grpc can send multiple inter transaction changed account states for same slot
        // we have to update till we get the last
        if commitment == Commitment::Processed
            || !self.processed_accounts.contains_key(&data.updated_slot)
        {
            self.processed_accounts
                .insert(data.updated_slot, data.clone());
            updated = true;
        }

        match commitment {
            Commitment::Confirmed => {
                if update_confirmed {
                    self.confirmed_account = Some(data);
                    updated = true;
                }
            }
            Commitment::Finalized => {
                if update_confirmed {
                    self.confirmed_account = Some(data.clone());
                    updated = true;
                }
                if update_finalized {
                    self.finalized_account = Some(data);
                    updated = true;
                }
            }
            Commitment::Processed => {
                // processed already treated
            }
        }
        updated
    }
    // this method will promote processed account to confirmed account to finalized account
    // returns promoted account
    pub fn promote_slot_commitment(
        &mut self,
        _pubkey: Pubkey,
        slot: Slot,
        commitment: Commitment,
    ) -> Option<(AccountData, Option<AccountData>)> {
        if let Some(account_data) = self.processed_accounts.get(&slot).cloned() {
            match commitment {
                Commitment::Processed => {
                    // do nothing
                    None
                }
                Commitment::Confirmed => {
                    if self
                        .confirmed_account
                        .as_ref()
                        .map(|acc| acc.updated_slot)
                        .unwrap_or_default()
                        < slot
                    {
                        let prev_data = self.confirmed_account.clone();
                        self.confirmed_account = Some(account_data.clone());
                        Some((account_data, prev_data))
                    } else {
                        None
                    }
                }
                Commitment::Finalized => {
                    // slot finalized remove data from processed
                    while self.processed_accounts.len() > 1
                        && self
                            .processed_accounts
                            .first_key_value()
                            .map(|(s, _)| *s)
                            .unwrap_or(u64::MAX)
                            <= slot
                    {
                        self.processed_accounts.pop_first();
                    }

                    if self
                        .finalized_account
                        .as_ref()
                        .map(|acc| acc.updated_slot)
                        .unwrap_or_default()
                        < slot
                    {
                        let prev_data = self.finalized_account.clone();
                        self.finalized_account = Some(account_data.clone());
                        Some((account_data, prev_data))
                    } else {
                        None
                    }
                }
            }
        } else if commitment == Commitment::Finalized {
            // remove processed slot data
            while self.processed_accounts.len() > 1
                && self
                    .processed_accounts
                    .first_key_value()
                    .map(|(s, _)| *s)
                    .unwrap_or(u64::MAX)
                    <= slot
            {
                self.processed_accounts.pop_first();
            }

            None
        } else {
            //log::warn!("Expected to have processed account update for slot {} data and pk {}", slot, pubkey);
            None
        }
    }
}

struct SlotStatus {
    pub commitment: Commitment,
    pub accounts_updated: HashSet<Pubkey>,
}

pub struct InmemoryAccountStore {
    account_store: Arc<DashMap<Pubkey, AccountDataByCommitment>>,
    accounts_by_owner: Arc<DashMap<Pubkey, HashSet<Pubkey>>>,
    slots_status: Arc<Mutex<BTreeMap<Slot, SlotStatus>>>,
}

impl InmemoryAccountStore {
    pub fn new() -> Self {
        Self {
            account_store: Arc::new(DashMap::new()),
            accounts_by_owner: Arc::new(DashMap::new()),
            slots_status: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    fn add_account_owner(&self, account: Pubkey, owner: Pubkey) {
        match self.accounts_by_owner.entry(owner) {
            dashmap::mapref::entry::Entry::Occupied(mut occ) => {
                occ.get_mut().insert(account);
            }
            dashmap::mapref::entry::Entry::Vacant(vc) => {
                let mut set = HashSet::new();
                set.insert(account);
                vc.insert(set);
            }
        }
    }

    // here if the commitment is processed and the account has changed owner from A->B we keep the key for both A and B
    // then we remove the key from A for finalized commitment
    fn update_owner(
        &self,
        prev_account_data: &AccountData,
        new_account_data: &AccountData,
        commitment: Commitment,
    ) {
        assert_eq!(prev_account_data.pubkey, new_account_data.pubkey);
        if prev_account_data.account.owner != new_account_data.account.owner {
            if commitment == Commitment::Finalized {
                match self
                    .accounts_by_owner
                    .entry(prev_account_data.account.owner)
                {
                    dashmap::mapref::entry::Entry::Occupied(mut occ) => {
                        occ.get_mut().remove(&prev_account_data.pubkey);
                    }
                    dashmap::mapref::entry::Entry::Vacant(_) => {
                        // do nothing
                    }
                }
            }
            self.add_account_owner(new_account_data.pubkey, new_account_data.account.owner);
        }
    }

    async fn maybe_update_slot_status(
        &self,
        account_data: &AccountData,
        commitment: Commitment,
    ) -> Commitment {
        let slot = account_data.updated_slot;
        let mut lk = self.slots_status.lock().await;
        let slot_status = match lk.get_mut(&slot) {
            Some(x) => x,
            None => {
                lk.insert(
                    slot,
                    SlotStatus {
                        commitment,
                        accounts_updated: HashSet::new(),
                    },
                );
                lk.get_mut(&slot).unwrap()
            }
        };
        match commitment {
            Commitment::Processed => {
                // insert account into slot status
                slot_status.accounts_updated.insert(account_data.pubkey);
                slot_status.commitment
            }
            Commitment::Confirmed => slot_status.commitment,
            Commitment::Finalized => commitment,
        }
    }
}

#[async_trait]
impl AccountStorageInterface for InmemoryAccountStore {
    async fn update_account(&self, account_data: AccountData, commitment: Commitment) -> bool {
        SLOT_FOR_LATEST_ACCOUNT_UPDATE.set(account_data.updated_slot as i64);
        // check if the blockhash and slot is already confirmed
        let commitment = self
            .maybe_update_slot_status(&account_data, commitment)
            .await;

        match self.account_store.entry(account_data.pubkey) {
            dashmap::mapref::entry::Entry::Occupied(mut occ) => {
                let prev_account = occ.get().get_account_data(commitment);

                // if account has been updated
                if occ.get_mut().update(account_data.clone(), commitment) {
                    if let Some(prev_account) = prev_account {
                        self.update_owner(&prev_account, &account_data, commitment);
                    }
                    true
                } else {
                    false
                }
            }
            dashmap::mapref::entry::Entry::Vacant(vac) => {
                ACCOUNT_STORED_IN_MEMORY.inc();
                self.add_account_owner(account_data.pubkey, account_data.account.owner);
                vac.insert(AccountDataByCommitment::new(
                    account_data.clone(),
                    commitment,
                ));
                true
            }
        }
    }

    async fn initilize_or_update_account(&self, account_data: AccountData) {
        self.maybe_update_slot_status(&account_data, Commitment::Finalized)
            .await;
        match self.account_store.contains_key(&account_data.pubkey) {
            true => {
                // account has already been filled by an event
                self.update_account(account_data, Commitment::Finalized)
                    .await;
            }
            false => {
                ACCOUNT_STORED_IN_MEMORY.inc();
                self.add_account_owner(account_data.pubkey, account_data.account.owner);
                self.account_store.insert(
                    account_data.pubkey,
                    AccountDataByCommitment::initialize(account_data),
                );
            }
        }
    }

    async fn get_account(
        &self,
        account_pk: Pubkey,
        commitment: Commitment,
    ) -> Result<Option<AccountData>, AccountLoadingError> {
        if let Some(account_by_commitment) = self.account_store.get(&account_pk) {
            Ok(account_by_commitment.get_account_data(commitment).clone())
        } else {
            Ok(None)
        }
    }

    async fn get_program_accounts(
        &self,
        program_pubkey: Pubkey,
        account_filters: Option<Vec<RpcFilterType>>,
        commitment: Commitment,
    ) -> Option<Vec<AccountData>> {
        if let Some(program_accounts) = self.accounts_by_owner.get(&program_pubkey) {
            let mut return_vec = vec![];
            for program_account in program_accounts.iter() {
                let account_data = self.get_account(*program_account, commitment).await;
                if let Ok(Some(account_data)) = account_data {
                    // recheck program owner and filters
                    if account_data.account.owner.eq(&program_pubkey) {
                        match &account_filters {
                            Some(filters) => {
                                if filters.iter().all(|filter| account_data.allows(filter)) {
                                    return_vec.push(account_data.clone());
                                }
                            }
                            None => {
                                return_vec.push(account_data.clone());
                            }
                        }
                    }
                }
            }
            Some(return_vec)
        } else {
            None
        }
    }

    async fn process_slot_data(&self, slot: Slot, commitment: Commitment) -> Vec<AccountData> {
        let writable_accounts = {
            let mut lk = self.slots_status.lock().await;
            // remove old slot status if finalized
            if commitment == Commitment::Finalized {
                while let Some(entry) = lk.first_entry() {
                    if *entry.key() < slot {
                        entry.remove();
                    } else {
                        break;
                    }
                }
            }
            match lk.get_mut(&slot) {
                Some(status) => {
                    status.commitment = commitment;
                    status.accounts_updated.clone()
                }
                None => {
                    if commitment == Commitment::Confirmed {
                        log::debug!(
                            "slot status not found for {} and commitment {}, confirmed lagging",
                            slot,
                            commitment.into_commitment_level()
                        );
                    } else if commitment == Commitment::Finalized {
                        log::error!("slot status not found for {} and commitment {}, should be normal during startup", slot, commitment.into_commitment_level());
                    }
                    let status = SlotStatus {
                        commitment,
                        accounts_updated: HashSet::new(),
                    };
                    lk.insert(slot, status);
                    HashSet::new()
                }
            }
        };

        let mut updated_accounts = vec![];
        for writable_account in writable_accounts {
            if let Some(mut account) = self.account_store.get_mut(&writable_account) {
                if let Some((account_data, prev_account_data)) =
                    account.promote_slot_commitment(writable_account, slot, commitment)
                {
                    if let Some(prev_account_data) = prev_account_data {
                        // check if owner has changed
                        if prev_account_data.account.owner != account_data.account.owner {
                            self.update_owner(&prev_account_data, &account_data, commitment);
                        }

                        //check if account data has changed
                        if prev_account_data != account_data {
                            updated_accounts.push(account_data);
                        }
                    } else {
                        // account has been confirmed first time
                        updated_accounts.push(account_data);
                    }
                }
            }
        }

        // update number of processed accounts in memory
        let number_of_processed_accounts_in_memory: i64 = self
            .account_store
            .iter()
            .map(|x| x.processed_accounts.len() as i64)
            .sum();
        TOTAL_PROCESSED_ACCOUNTS.set(number_of_processed_accounts_in_memory);

        updated_accounts
    }
}

impl Default for InmemoryAccountStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Arc};

    use itertools::Itertools;
    use rand::{rngs::ThreadRng, Rng};
    use solana_lite_rpc_core::{
        commitment_utils::Commitment,
        structures::account_data::{Account, AccountData},
    };
    use solana_sdk::{account::Account as SolanaAccount, pubkey::Pubkey, slot_history::Slot};

    use crate::{
        account_store_interface::AccountStorageInterface,
        inmemory_account_store::InmemoryAccountStore,
    };

    fn create_random_account(
        rng: &mut ThreadRng,
        updated_slot: Slot,
        pubkey: Pubkey,
        program: Pubkey,
    ) -> AccountData {
        let length: usize = rng.gen_range(100..1000);
        let sol_account = SolanaAccount {
            lamports: rng.gen(),
            data: (0..length).map(|_| rng.gen::<u8>()).collect_vec(),
            owner: program,
            executable: false,
            rent_epoch: 0,
        };
        AccountData {
            pubkey,
            account: Arc::new(Account::from_solana_account(
                sol_account,
                solana_lite_rpc_core::structures::account_data::CompressionMethod::None,
            )),
            updated_slot,
        }
    }

    #[tokio::test]
    pub async fn test_account_store() {
        let store = InmemoryAccountStore::default();
        let mut rng = rand::thread_rng();
        let program = Pubkey::new_unique();
        let pk1 = Pubkey::new_unique();
        let pk2 = Pubkey::new_unique();

        let account_data_0 = create_random_account(&mut rng, 0, pk1, program);
        store
            .initilize_or_update_account(account_data_0.clone())
            .await;

        let account_data_1 = create_random_account(&mut rng, 0, pk2, program);

        let mut pubkeys = HashSet::new();
        pubkeys.insert(pk1);
        pubkeys.insert(pk2);

        store
            .initilize_or_update_account(account_data_1.clone())
            .await;

        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await,
            Ok(Some(account_data_0.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await,
            Ok(Some(account_data_0.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await,
            Ok(Some(account_data_0.clone()))
        );

        assert_eq!(
            store.get_account(pk2, Commitment::Processed).await,
            Ok(Some(account_data_1.clone()))
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Confirmed).await,
            Ok(Some(account_data_1.clone()))
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Finalized).await,
            Ok(Some(account_data_1.clone()))
        );

        let account_data_2 = create_random_account(&mut rng, 1, pk1, program);
        let account_data_3 = create_random_account(&mut rng, 2, pk1, program);
        let account_data_4 = create_random_account(&mut rng, 3, pk1, program);
        let account_data_5 = create_random_account(&mut rng, 4, pk1, program);

        store
            .update_account(account_data_2.clone(), Commitment::Processed)
            .await;
        store
            .update_account(account_data_3.clone(), Commitment::Processed)
            .await;
        store
            .update_account(account_data_4.clone(), Commitment::Processed)
            .await;
        store
            .update_account(account_data_5.clone(), Commitment::Processed)
            .await;

        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await,
            Ok(Some(account_data_5.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await,
            Ok(Some(account_data_0.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await,
            Ok(Some(account_data_0.clone()))
        );

        store.process_slot_data(1, Commitment::Confirmed).await;

        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await,
            Ok(Some(account_data_5.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await,
            Ok(Some(account_data_2.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await,
            Ok(Some(account_data_0.clone()))
        );

        store.process_slot_data(2, Commitment::Confirmed).await;

        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await,
            Ok(Some(account_data_5.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await,
            Ok(Some(account_data_3.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await,
            Ok(Some(account_data_0.clone()))
        );

        store.process_slot_data(1, Commitment::Finalized).await;

        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await,
            Ok(Some(account_data_5.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await,
            Ok(Some(account_data_3.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await,
            Ok(Some(account_data_2.clone()))
        );
    }

    #[tokio::test]
    pub async fn test_account_store_if_finalized_clears_old_processed_slots() {
        let store = InmemoryAccountStore::default();

        let program = Pubkey::new_unique();
        let pk1 = Pubkey::new_unique();

        let mut pubkeys = HashSet::new();
        pubkeys.insert(pk1);

        let mut rng = rand::thread_rng();

        store
            .initilize_or_update_account(create_random_account(&mut rng, 0, pk1, program))
            .await;

        store
            .update_account(
                create_random_account(&mut rng, 1, pk1, program),
                Commitment::Processed,
            )
            .await;
        store
            .update_account(
                create_random_account(&mut rng, 1, pk1, program),
                Commitment::Processed,
            )
            .await;
        store
            .update_account(
                create_random_account(&mut rng, 2, pk1, program),
                Commitment::Processed,
            )
            .await;
        store
            .update_account(
                create_random_account(&mut rng, 3, pk1, program),
                Commitment::Processed,
            )
            .await;
        store
            .update_account(
                create_random_account(&mut rng, 4, pk1, program),
                Commitment::Processed,
            )
            .await;
        store
            .update_account(
                create_random_account(&mut rng, 5, pk1, program),
                Commitment::Processed,
            )
            .await;
        store
            .update_account(
                create_random_account(&mut rng, 6, pk1, program),
                Commitment::Processed,
            )
            .await;
        store
            .update_account(
                create_random_account(&mut rng, 7, pk1, program),
                Commitment::Processed,
            )
            .await;

        let account_8 = create_random_account(&mut rng, 8, pk1, program);
        store
            .update_account(account_8.clone(), Commitment::Processed)
            .await;
        store
            .update_account(
                create_random_account(&mut rng, 9, pk1, program),
                Commitment::Processed,
            )
            .await;
        store
            .update_account(
                create_random_account(&mut rng, 10, pk1, program),
                Commitment::Processed,
            )
            .await;

        let last_account = create_random_account(&mut rng, 11, pk1, program);
        store
            .update_account(last_account.clone(), Commitment::Processed)
            .await;

        assert_eq!(
            store
                .account_store
                .get(&pk1)
                .unwrap()
                .processed_accounts
                .len(),
            12
        );
        store.process_slot_data(11, Commitment::Finalized).await;
        assert_eq!(
            store
                .account_store
                .get(&pk1)
                .unwrap()
                .processed_accounts
                .len(),
            1
        );

        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await,
            Ok(Some(last_account.clone())),
        );

        // check finalizing previous commitment does not affect
        store.process_slot_data(8, Commitment::Finalized).await;

        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await,
            Ok(Some(last_account)),
        );
    }

    #[tokio::test]
    pub async fn test_get_program_account() {
        let store = InmemoryAccountStore::default();

        let prog_1 = Pubkey::new_unique();
        let prog_2 = Pubkey::new_unique();

        let mut rng = rand::thread_rng();

        let pks = (0..5).map(|_| Pubkey::new_unique()).collect_vec();

        store
            .update_account(
                create_random_account(&mut rng, 1, pks[0], prog_1),
                Commitment::Confirmed,
            )
            .await;
        store
            .update_account(
                create_random_account(&mut rng, 1, pks[1], prog_1),
                Commitment::Confirmed,
            )
            .await;
        store
            .update_account(
                create_random_account(&mut rng, 1, pks[2], prog_1),
                Commitment::Confirmed,
            )
            .await;
        store
            .update_account(
                create_random_account(&mut rng, 1, pks[3], prog_1),
                Commitment::Confirmed,
            )
            .await;

        store
            .update_account(
                create_random_account(&mut rng, 1, pks[4], prog_2),
                Commitment::Confirmed,
            )
            .await;

        let acc_prgram_1 = store
            .get_program_accounts(prog_1, None, Commitment::Processed)
            .await;
        assert!(acc_prgram_1.is_some());
        assert_eq!(acc_prgram_1.unwrap().len(), 4);
        let acc_prgram_1 = store
            .get_program_accounts(prog_1, None, Commitment::Confirmed)
            .await;
        assert!(acc_prgram_1.is_some());
        assert_eq!(acc_prgram_1.unwrap().len(), 4);
        let acc_prgram_1 = store
            .get_program_accounts(prog_1, None, Commitment::Finalized)
            .await;
        assert!(acc_prgram_1.is_some());
        assert!(acc_prgram_1.unwrap().is_empty());

        let acc_prgram_2 = store
            .get_program_accounts(prog_2, None, Commitment::Processed)
            .await;
        assert!(acc_prgram_2.is_some());
        assert_eq!(acc_prgram_2.unwrap().len(), 1);
        let acc_prgram_2 = store
            .get_program_accounts(prog_2, None, Commitment::Confirmed)
            .await;
        assert!(acc_prgram_2.is_some());
        assert_eq!(acc_prgram_2.unwrap().len(), 1);
        let acc_prgram_2 = store
            .get_program_accounts(prog_2, None, Commitment::Finalized)
            .await;
        assert!(acc_prgram_2.is_some());
        assert!(acc_prgram_2.unwrap().is_empty());

        let acc_prgram_3 = store
            .get_program_accounts(Pubkey::new_unique(), None, Commitment::Processed)
            .await;
        assert!(acc_prgram_3.is_none());
        let acc_prgram_3 = store
            .get_program_accounts(Pubkey::new_unique(), None, Commitment::Confirmed)
            .await;
        assert!(acc_prgram_3.is_none());
        let acc_prgram_3 = store
            .get_program_accounts(Pubkey::new_unique(), None, Commitment::Finalized)
            .await;
        assert!(acc_prgram_3.is_none());

        store.process_slot_data(1, Commitment::Finalized).await;

        let acc_prgram_1 = store
            .get_program_accounts(prog_1, None, Commitment::Finalized)
            .await;
        assert!(acc_prgram_1.is_some());
        assert_eq!(acc_prgram_1.unwrap().len(), 4);
        let acc_prgram_2 = store
            .get_program_accounts(prog_2, None, Commitment::Finalized)
            .await;
        assert!(acc_prgram_2.is_some());
        assert_eq!(acc_prgram_2.unwrap().len(), 1);

        let pk = Pubkey::new_unique();
        let prog_3 = Pubkey::new_unique();

        let account_finalized = create_random_account(&mut rng, 2, pk, prog_3);
        store
            .update_account(account_finalized.clone(), Commitment::Finalized)
            .await;
        store.process_slot_data(2, Commitment::Finalized).await;

        let account_confirmed = create_random_account(&mut rng, 3, pk, prog_3);
        store
            .update_account(account_confirmed.clone(), Commitment::Confirmed)
            .await;

        let prog_4 = Pubkey::new_unique();
        let account_processed = create_random_account(&mut rng, 4, pk, prog_4);
        store
            .update_account(account_processed.clone(), Commitment::Processed)
            .await;

        let f = store
            .get_program_accounts(prog_3, None, Commitment::Finalized)
            .await;

        let c = store
            .get_program_accounts(prog_3, None, Commitment::Confirmed)
            .await;

        let p_3 = store
            .get_program_accounts(prog_3, None, Commitment::Processed)
            .await;

        let p_4 = store
            .get_program_accounts(prog_4, None, Commitment::Processed)
            .await;

        assert_eq!(c, Some(vec![account_confirmed.clone()]));
        assert_eq!(p_3, Some(vec![]));
        assert_eq!(p_4, Some(vec![account_processed.clone()]));

        assert_eq!(f, Some(vec![account_finalized.clone()]));

        store.process_slot_data(3, Commitment::Finalized).await;
        store.process_slot_data(4, Commitment::Confirmed).await;

        let f = store
            .get_program_accounts(prog_3, None, Commitment::Finalized)
            .await;

        let p_3 = store
            .get_program_accounts(prog_3, None, Commitment::Confirmed)
            .await;

        let p_4 = store
            .get_program_accounts(prog_4, None, Commitment::Confirmed)
            .await;

        assert_eq!(f, Some(vec![account_confirmed.clone()]));
        assert_eq!(p_3, Some(vec![]));
        assert_eq!(p_4, Some(vec![account_processed.clone()]));

        store.process_slot_data(4, Commitment::Finalized).await;
        let p_3 = store
            .get_program_accounts(prog_3, None, Commitment::Finalized)
            .await;

        let p_4 = store
            .get_program_accounts(prog_4, None, Commitment::Finalized)
            .await;

        assert_eq!(p_3, Some(vec![]));
        assert_eq!(p_4, Some(vec![account_processed.clone()]));
    }
}
