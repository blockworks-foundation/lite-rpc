use std::sync::Arc;

use solana_lite_rpc_core::{
    traits::address_lookup_table_interface::AddressLookupTableInterface, types::BlockStream,
};
use solana_sdk::{pubkey::Pubkey, slot_history::Slot};
use tokio::{sync::broadcast::Sender, task::JoinHandle};

use crate::{
    account_priofees::AccountPrioStore,
    rpc_data::{AccountPrioFeesStats, AccountPrioFeesUpdateMessage},
};
use tokio::sync::broadcast::error::RecvError::{Closed, Lagged};

#[derive(Clone)]
pub struct AccountPrioService {
    account_store: AccountPrioStore,
    pub priofees_update_sender: Sender<AccountPrioFeesUpdateMessage>,
}

impl AccountPrioService {
    pub fn start_account_priofees_task(
        mut block_stream: BlockStream,
        slots_to_retain: usize,
        address_lookup_tables_impl: Option<Arc<dyn AddressLookupTableInterface>>,
    ) -> (JoinHandle<()>, AccountPrioService) {
        let account_store = AccountPrioStore::new(slots_to_retain, address_lookup_tables_impl);
        let (priofees_update_sender, _priofees_update_receiver) =
            tokio::sync::broadcast::channel(64);

        let jh = {
            let account_store = account_store.clone();
            let priofees_update_sender = priofees_update_sender.clone();
            tokio::spawn(async move {
                'recv_loop: loop {
                    let block = block_stream.recv().await;
                    match block {
                        Ok(block) => {
                            if !block.commitment_config.is_processed() {
                                continue;
                            }

                            let account_fee_message = account_store.update(&block).await;
                            let _ = priofees_update_sender.send(account_fee_message);
                        }
                        Err(Lagged(_lagged)) => {
                            log::warn!(
                                "channel error receiving block for priofees calculation - continue"
                            );
                            continue 'recv_loop;
                        }
                        Err(Closed) => {
                            log::error!("failed to receive block, sender closed - aborting");
                            break 'recv_loop;
                        }
                    }
                }
                log::info!("priofees task shutting down");
            })
        };
        (
            jh,
            AccountPrioService {
                account_store,
                priofees_update_sender,
            },
        )
    }

    pub fn get_latest_stats(&self, account: &Pubkey) -> (Slot, AccountPrioFeesStats) {
        self.account_store.get_latest_stats(account)
    }

    pub fn get_n_last_stats(&self, account: &Pubkey, nb: usize) -> (Slot, AccountPrioFeesStats) {
        self.account_store.get_n_last_stats(account, nb)
    }
}
