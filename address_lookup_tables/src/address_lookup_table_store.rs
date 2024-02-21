use async_trait::async_trait;
use dashmap::DashMap;
use itertools::Itertools;
use prometheus::{opts, register_int_gauge, IntGauge};
use serde::{Deserialize, Serialize};
use solana_address_lookup_table_program::state::AddressLookupTable;
use solana_lite_rpc_core::traits::address_lookup_table_interface::AddressLookupTableInterface;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey};
use std::{sync::Arc, time::Duration};

lazy_static::lazy_static! {
    static ref LRPC_ALTS_IN_STORE: IntGauge =
       register_int_gauge!(opts!("literpc_alts_stored", "Alts stored in literpc")).unwrap();
}

#[derive(Clone)]
pub struct AddressLookupTableStore {
    rpc_client: Arc<RpcClient>,
    pub map: Arc<DashMap<Pubkey, Vec<Pubkey>>>,
}

impl AddressLookupTableStore {
    pub fn new(rpc_client: Arc<RpcClient>) -> Self {
        Self {
            rpc_client,
            map: Arc::new(DashMap::new()),
        }
    }

    async fn load_alts_list(&self, alts_list: &[Pubkey]) {
        log::trace!("Preloading {} ALTs", alts_list.len());
        for batches in alts_list.chunks(1000).map(|x| x.to_vec()) {
            let tasks = batches.chunks(100).map(|batch| {
                let batch = batch.to_vec();
                let rpc_client = self.rpc_client.clone();
                let this = self.clone();
                tokio::spawn(async move {
                    let data = rpc_client
                        .get_multiple_accounts_with_commitment(
                            &batch,
                            CommitmentConfig::processed(),
                        )
                        .await;

                    match data {
                        Ok(multiple_accounts) => {
                            for (index, acc) in multiple_accounts.value.iter().enumerate() {
                                if let Some(acc) = acc {
                                    this.save_account(&batch[index], &acc.data);
                                }
                            }
                        }
                        Err(e) => {
                            log::error!(
                                "error loading {} alts with error {}, skipping lookup for this batch and continue",
                                batch.len(),
                                e.to_string()
                            );
                        }
                    };
                })
            });
            if tokio::time::timeout(Duration::from_secs(60), futures::future::join_all(tasks))
                .await
                .is_err()
            {
                log::error!(
                    "timeout loading {} alts, skipping lookup for this batch and continue",
                    alts_list.len()
                );
            }
        }
        LRPC_ALTS_IN_STORE.set(self.map.len() as i64);
    }

    pub fn save_account(&self, address: &Pubkey, data: &[u8]) {
        let lookup_table = AddressLookupTable::deserialize(data).unwrap();
        if self
            .map
            .insert(*address, lookup_table.addresses.to_vec())
            .is_none()
        {
            LRPC_ALTS_IN_STORE.inc();
        }
        drop(lookup_table);
    }

    pub async fn reload_if_necessary(
        &self,
        alt_messages: &[&solana_sdk::message::v0::MessageAddressTableLookup],
    ) {
        let accounts_to_load = alt_messages
            .iter()
            .filter_map(|alt| match self.map.get(&alt.account_key) {
                Some(alt_data) => {
                    let size = alt_data.len() as u8;
                    if alt.readonly_indexes.iter().any(|x| *x >= size)
                        || alt.writable_indexes.iter().any(|x| *x >= size)
                    {
                        Some(alt.account_key)
                    } else {
                        None
                    }
                }
                None => Some(alt.account_key),
            })
            .collect_vec();
        self.load_alts_list(&accounts_to_load).await;
    }

    pub async fn reload_alt_account(&self, address: &Pubkey) {
        log::info!("Reloading {address:?}");

        let account = match self
            .rpc_client
            .get_account_with_commitment(address, CommitmentConfig::processed())
            .await
        {
            Ok(acc) => acc.value,
            Err(e) => {
                log::error!(
                    "Error for fetching address lookup table {} error :{}",
                    address.to_string(),
                    e.to_string()
                );
                None
            }
        };
        match account {
            Some(account) => {
                self.save_account(address, &account.data);
            }
            None => {
                log::error!("Cannot find address lookup table {}", address.to_string());
            }
        }
    }

    async fn get_accounts_in_address_lookup_table(
        &self,
        alt: &Pubkey,
        accounts: &[u8],
    ) -> Option<Vec<Pubkey>> {
        let alt_account = self.map.get(alt);
        match alt_account {
            Some(alt_account) => {
                if accounts
                    .iter()
                    .any(|index| *index as usize >= alt_account.len())
                {
                    log::error!("address lookup table {} should have been reloaded", alt);
                    None
                } else {
                    Some(
                        accounts
                            .iter()
                            .map(|i| alt_account[*i as usize])
                            .collect_vec(),
                    )
                }
            }
            None => {
                log::error!("address lookup table {} was not found", alt);
                None
            }
        }
    }

    pub async fn get_accounts(&self, alt: &Pubkey, accounts: &[u8]) -> Vec<Pubkey> {
        match self
            .get_accounts_in_address_lookup_table(alt, accounts)
            .await
        {
            Some(x) => x,
            None => {
                // forget alt for now, start loading it for next blocks
                // loading should be on its way
                vec![]
            }
        }
    }

    pub fn serialize_binary(&self) -> Vec<u8> {
        bincode::serialize::<BinaryALTData>(&BinaryALTData::new(&self.map)).unwrap()
    }

    /// To load binary ALT file at the startup
    pub fn load_binary(&self, binary_data: Vec<u8>) {
        let binary_alt_data = bincode::deserialize::<BinaryALTData>(&binary_data).unwrap();
        for (alt, accounts) in binary_alt_data.data.iter() {
            self.map.insert(*alt, accounts.clone());
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct BinaryALTData {
    data: Vec<(Pubkey, Vec<Pubkey>)>,
}

impl BinaryALTData {
    pub fn new(map: &Arc<DashMap<Pubkey, Vec<Pubkey>>>) -> Self {
        let data = map
            .iter()
            .map(|x| (*x.key(), x.value().clone()))
            .collect_vec();
        Self { data }
    }
}

#[async_trait]
impl AddressLookupTableInterface for AddressLookupTableStore {
    async fn resolve_addresses_from_lookup_table(
        &self,
        message_address_table_lookup: &solana_sdk::message::v0::MessageAddressTableLookup,
    ) -> (Vec<Pubkey>, Vec<Pubkey>) {
        (
            self.get_accounts(
                &message_address_table_lookup.account_key,
                &message_address_table_lookup.writable_indexes,
            )
            .await,
            self.get_accounts(
                &message_address_table_lookup.account_key,
                &message_address_table_lookup.readonly_indexes,
            )
            .await,
        )
    }

    async fn reload_if_necessary(
        &self,
        message_address_table_lookups: &[&solana_sdk::message::v0::MessageAddressTableLookup],
    ) {
        self.reload_if_necessary(message_address_table_lookups)
            .await;
    }
}
