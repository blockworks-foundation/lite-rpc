use anyhow::Context;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_lite_rpc_core::AnyhowJoinHandle;
use solana_rpc_client_api::response::{RpcContactInfo, RpcVoteAccountStatus};
use std::{sync::Arc, time::Duration};
use log::warn;
use tokio::sync::broadcast::Sender;

pub fn poll_vote_accounts_and_cluster_info(
    rpc_client: Arc<RpcClient>,
    contact_info_sender: Sender<Vec<RpcContactInfo>>,
    vote_account_sender: Sender<RpcVoteAccountStatus>,
) -> AnyhowJoinHandle {
    // task MUST not terminate but might be aborted from outside
    tokio::spawn(async move {
        loop {
            match rpc_client.get_cluster_nodes().await {
                Ok(cluster_nodes) => {
                    if let Err(e) = contact_info_sender
                        .send(cluster_nodes) {
                        warn!("rpc_cluster_info channel has no receivers {e:?}");
                    }
                }
                Err(error) => {
                    warn!("rpc_cluster_info failed {error:?} - retrying");
                    // throttle
                    tokio::time::sleep(Duration::from_secs(2500)).await;
                }
            }
            match rpc_client.get_vote_accounts().await {
                Ok(vote_accounts) => {
                    if let Err(e) = vote_account_sender
                        .send(vote_accounts) {
                        warn!("rpc_vote_accounts channel has no receivers {e:?}");
                    }
                }
                Err(error) => {
                    warn!("rpc_vote_accounts failed {error:?} - retrying");
                    // throttle
                    tokio::time::sleep(Duration::from_secs(2500)).await;
                }
            }
            tokio::time::sleep(Duration::from_secs(600)).await;
        }
        unreachable!("Task is not allowed to terminate");
    })
}
