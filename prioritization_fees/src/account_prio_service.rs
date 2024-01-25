use solana_lite_rpc_core::types::BlockStream;
use solana_sdk::pubkey::Pubkey;
use tokio::task::JoinHandle;

use crate::{account_priofees::AccountPrioStore, rpc_data::AccountPrioFeesStats};
use tokio::sync::broadcast::error::RecvError::{Closed, Lagged};

#[derive(Clone)]
pub struct AccountPrioService {
    account_store: AccountPrioStore,
}

impl AccountPrioService {
    pub fn new(slots_to_retain: usize) -> Self {
        Self {
            account_store: AccountPrioStore::new(slots_to_retain),
        }
    }

    pub fn start_account_priofees_task(
        mut block_stream: BlockStream,
        slots_to_retain: usize,
    ) -> (JoinHandle<()>, AccountPrioService) {
        let service_instance = Self::new(slots_to_retain);
        let service = service_instance.clone();
        let jh = tokio::spawn(async move {
            'recv_loop: loop {
                let block = block_stream.recv().await;
                match block {
                    Ok(block) => {
                        if !block.commitment_config.is_processed() {
                            continue;
                        }

                        service.account_store.update(&block);
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
        });
        (jh, service_instance)
    }

    pub fn get_latest_stats(&self, account: &Pubkey) -> (u64, AccountPrioFeesStats) {
        self.account_store.get_latest_stats(account)
    }

    pub fn get_n_last_stats(&self, account: &Pubkey, nb: usize) -> (u64, AccountPrioFeesStats) {
        self.account_store.get_n_last_stats(account, nb)
    }
}
