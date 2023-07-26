use crate::structures::identity_stakes::IdentityStakes;
use anyhow::Context;
use log::info;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{pubkey::Pubkey, slot_history::Slot};
use solana_streamer::nonblocking::quic::ConnectionPeerType;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{
    broadcast,
    mpsc::{UnboundedReceiver, UnboundedSender},
};

const AVERAGE_SLOT_CHANGE_TIME_IN_MILLIS: u64 = 400;

pub struct SolanaUtils;

impl SolanaUtils {
    pub async fn get_stakes_for_identity(
        rpc_client: Arc<RpcClient>,
        identity: Pubkey,
    ) -> anyhow::Result<IdentityStakes> {
        let vote_accounts = rpc_client.get_vote_accounts().await?;
        let map_of_stakes: HashMap<String, u64> = vote_accounts
            .current
            .iter()
            .map(|x| (x.node_pubkey.clone(), x.activated_stake))
            .collect();

        if let Some(stakes) = map_of_stakes.get(&identity.to_string()) {
            let all_stakes: Vec<u64> = vote_accounts
                .current
                .iter()
                .map(|x| x.activated_stake)
                .collect();

            let identity_stakes = IdentityStakes {
                peer_type: ConnectionPeerType::Staked,
                stakes: *stakes,
                min_stakes: all_stakes.iter().min().map_or(0, |x| *x),
                max_stakes: all_stakes.iter().max().map_or(0, |x| *x),
                total_stakes: all_stakes.iter().sum(),
            };

            info!(
                "Idenity stakes {}, {}, {}, {}",
                identity_stakes.total_stakes,
                identity_stakes.min_stakes,
                identity_stakes.max_stakes,
                identity_stakes.stakes
            );
            Ok(identity_stakes)
        } else {
            Ok(IdentityStakes::default())
        }
    }

}
