use crate::structures::identity_stakes::IdentityStakes;
use anyhow::Context;
use log::info;
use serde::Serialize;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::hash::Hash;
use solana_sdk::pubkey::Pubkey;
use solana_streamer::nonblocking::quic::ConnectionPeerType;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use solana_sdk::signature::Signature;
use solana_sdk::transaction::{uses_durable_nonce, Transaction, VersionedTransaction};
use tokio::sync::mpsc::UnboundedReceiver;

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

    pub async fn poll_slots(
        rpc_client: &RpcClient,
        update_slot: impl Fn(u64),
    ) -> anyhow::Result<()> {
        let mut poll_frequency = tokio::time::interval(Duration::from_millis(50));
        loop {
            let slot = rpc_client
                .get_slot_with_commitment(solana_sdk::commitment_config::CommitmentConfig {
                    commitment: solana_sdk::commitment_config::CommitmentLevel::Processed,
                })
                .await
                .context("Error getting slot")?;
            update_slot(slot);
            poll_frequency.tick().await;
        }
    }

    // Estimates the slots, either from polled slot or by forcefully updating after every 400ms
    // returns if the estimated slot was updated or not
    pub async fn slot_estimator(
        slot_update_notifier: &mut UnboundedReceiver<u64>,
        current_slot: Arc<AtomicU64>,
        estimated_slot: Arc<AtomicU64>,
    ) -> bool {
        match tokio::time::timeout(
            Duration::from_millis(AVERAGE_SLOT_CHANGE_TIME_IN_MILLIS),
            slot_update_notifier.recv(),
        )
        .await
        {
            Ok(recv) => {
                if let Some(slot) = recv {
                    if slot > estimated_slot.load(Ordering::Relaxed) {
                        // incase of multilple slot update events / take the current slot
                        let current_slot = current_slot.load(Ordering::Relaxed);
                        estimated_slot.store(current_slot, Ordering::Relaxed);
                        true
                    } else {
                        // queue is late estimate slot is already ahead
                        false
                    }
                } else {
                    false
                }
            }
            Err(_) => {
                // force update the slot
                let es = estimated_slot.load(Ordering::Relaxed);
                let cs = current_slot.load(Ordering::Relaxed);
                // estimated slot should not go ahead more than 32 slots
                // this is because it may be a slot block
                if es < cs + 32 {
                    estimated_slot.fetch_add(1, Ordering::Relaxed);
                    true
                } else {
                    false
                }
            }
        }
    }
}

/// Trait used to add support for versioned transactions to RPC APIs while
/// retaining backwards compatibility
pub trait SerializableTransaction: Serialize {
    fn get_signature(&self) -> &Signature;
    fn get_recent_blockhash(&self) -> &Hash;
    fn uses_durable_nonce(&self) -> bool;
}
impl SerializableTransaction for Transaction {
    fn get_signature(&self) -> &Signature {
        &self.signatures[0]
    }
    fn get_recent_blockhash(&self) -> &Hash {
        &self.message.recent_blockhash
    }
    fn uses_durable_nonce(&self) -> bool {
        uses_durable_nonce(self).is_some()
    }
}
impl SerializableTransaction for VersionedTransaction {
    fn get_signature(&self) -> &Signature {
        &self.signatures[0]
    }
    fn get_recent_blockhash(&self) -> &Hash {
        self.message.recent_blockhash()
    }
    fn uses_durable_nonce(&self) -> bool {
        self.uses_durable_nonce()
    }
}
