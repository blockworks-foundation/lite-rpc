use solana_lite_rpc_core::types::BlockStream;
use solana_sdk::pubkey::Pubkey;
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
    ) -> (JoinHandle<()>, AccountPrioService) {
        let account_store = AccountPrioStore::new(slots_to_retain);
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

                            let account_fee_message = account_store.update(&block);
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

    pub fn get_latest_stats(&self, account: &Pubkey) -> (u64, AccountPrioFeesStats) {
        self.account_store.get_latest_stats(account)
    }

    pub fn get_n_last_stats(&self, account: &Pubkey, nb: usize) -> (u64, AccountPrioFeesStats) {
        self.account_store.get_n_last_stats(account, nb)
    }

    // pub async fn notify_account_prios(&self, slot: u64, write_map: HashMap<Pubkey, BlockPrioData>, all_map: HashMap<Pubkey, BlockPrioData>) {
    //     self.subscriptions.retain(|_, subscriptions| {
    //         // remove close subscriptions
    //         for index in (0..subscriptions.len()).rev() {
    //             if subscriptions[index].is_closed() {
    //                 subscriptions.remove(index);
    //             }
    //         }
    //         !subscriptions.is_empty()
    //     });
    //     for account_iter in self.subscriptions.iter() {
    //         let account = account_iter.key();
    //         let subscriptions = account_iter.value();
    //         if let Some(all_prio_data) = all_map.get(account) {
    //             let write_stats = write_map.get(account).map(|x| x.calculate_stats()).unwrap_or_default();
    //             let all_stats = all_prio_data.calculate_stats();
    //             let account_prio_stats = AccountPrioFeesStats {
    //                 write_stats,
    //                 all_stats,
    //             };
    //             let message = serde_json::json!(account_prio_stats);
    //             for subscription in subscriptions {
    //                 subscription.send(slot, message.clone()).await;
    //             }
    //         }
    //     }
    // }
}
