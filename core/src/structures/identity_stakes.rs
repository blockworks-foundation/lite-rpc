use std::{collections::HashMap, sync::Arc};

use itertools::Itertools;
use solana_rpc_client_api::response::RpcVoteAccountStatus;
use solana_sdk::pubkey::Pubkey;
use solana_streamer::nonblocking::quic::ConnectionPeerType;
use tokio::sync::RwLock;

#[derive(Debug, Copy, Clone)]
pub struct IdentityStakesData {
    pub peer_type: ConnectionPeerType,
    pub stakes: u64,
    pub total_stakes: u64,
    pub min_stakes: u64,
    pub max_stakes: u64,
}

impl Default for IdentityStakesData {
    fn default() -> Self {
        IdentityStakesData {
            peer_type: ConnectionPeerType::Unstaked,
            stakes: 0,
            total_stakes: 0,
            max_stakes: 0,
            min_stakes: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct IdentityStakes {
    identity: Pubkey,
    stakes_data: Arc<RwLock<IdentityStakesData>>,
}

impl IdentityStakes {
    pub fn new(identity: Pubkey) -> Self {
        Self {
            identity,
            stakes_data: Arc::new(RwLock::new(IdentityStakesData::default())),
        }
    }

    pub async fn get_stakes(&self) -> IdentityStakesData {
        *self.stakes_data.read().await
    }

    pub async fn update_stakes_for_identity(&self, vote_accounts: RpcVoteAccountStatus) {
        let map_of_stakes: HashMap<String, u64> = vote_accounts
            .current
            .iter()
            .chain(vote_accounts.delinquent.iter())
            .map(|x| (x.node_pubkey.clone(), x.activated_stake))
            .collect();

        if let Some(stakes) = map_of_stakes.get(&self.identity.to_string()) {
            let only_stakes = map_of_stakes.iter().map(|x| *x.1).collect_vec();
            let identity_stakes = IdentityStakesData {
                peer_type: ConnectionPeerType::Staked,
                stakes: *stakes,
                min_stakes: only_stakes.iter().min().map_or(0, |x| *x),
                max_stakes: only_stakes.iter().max().map_or(0, |x| *x),
                total_stakes: only_stakes.iter().sum(),
            };

            log::info!(
                "Idenity stakes {}, {}, {}, {}",
                identity_stakes.total_stakes,
                identity_stakes.min_stakes,
                identity_stakes.max_stakes,
                identity_stakes.stakes
            );
            *self.stakes_data.write().await = identity_stakes;
        }
    }
}
