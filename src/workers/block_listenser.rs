use std::sync::Arc;

use anyhow::{bail, Context};
use dashmap::DashMap;
use futures::StreamExt;
use log::info;
use solana_client::nonblocking::pubsub_client::PubsubClient;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::{RpcBlockSubscribeConfig, RpcBlockSubscribeFilter};
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_sdk::signature::Signature;
use solana_sdk::transaction;
use tokio::task::JoinHandle;

/// Background worker which listen's to new blocks
/// and keeps a track of confirmed txs
#[derive(Clone)]
pub struct BlockListener {
    pub_sub_client: Arc<PubsubClient>,
    pub blocks: Arc<DashMap<String, CommitmentLevel>>,
    rpc_client: Arc<RpcClient>,
}

impl BlockListener {
    pub async fn new(rpc_client: Arc<RpcClient>, ws_url: &str) -> anyhow::Result<Self> {
        let pub_sub_client = Arc::new(PubsubClient::new(ws_url).await?);
        Ok(Self {
            pub_sub_client,
            rpc_client,
            blocks: Default::default(),
        })
    }

    /// check if tx is in the confirmed cache
    ///
    /// ## Return
    ///
    /// None if transaction is un-confirmed
    /// Some(Err) in case of transaction failure
    /// Some(Ok(())) if tx is confirmed without failure
    pub async fn confirm_tx(
        &self,
        sig: Signature,
        commitment_config: CommitmentConfig,
    ) -> Option<transaction::Result<()>> {
        let sig_string = sig.to_string();
        if self.blocks.contains_key(&sig_string) {
            info!("Confirmed {sig} from cache");
            Some(Ok(()))
        } else {
            let res = self
                .rpc_client
                .get_signature_status_with_commitment(&sig, commitment_config)
                .await
                .unwrap();
            if res.is_some() {
                self.blocks.insert(sig_string, commitment_config.commitment);
            }
            res
        }
    }

    pub fn listen(self, commitment_config: CommitmentConfig) -> JoinHandle<anyhow::Result<()>> {
        tokio::spawn(async move {
            info!("Subscribing to blocks");

            let (mut recv, _) = self
                .pub_sub_client
                .block_subscribe(
                    RpcBlockSubscribeFilter::All,
                    Some(RpcBlockSubscribeConfig {
                        commitment: Some(commitment_config),
                        encoding: None,
                        transaction_details: Some(
                            solana_transaction_status::TransactionDetails::Signatures,
                        ),
                        show_rewards: None,
                        max_supported_transaction_version: None,
                    }),
                )
                .await
                .context("Error calling block_subscribe")?;

            let commitment = commitment_config.commitment;

            info!("Listening to {commitment:?} blocks");

            while let Some(block) = recv.as_mut().next().await {
                let Some(block) = block.value.block else {
                    continue;
                };

                let Some(signatures) = block.signatures else {
                    continue;
                };

                for sig in signatures {
                    info!("{commitment:?} {sig}");
                    self.blocks.insert(sig, commitment_config.commitment);
                }
            }

            bail!("Stopped Listening to {commitment:?} blocks")
        })
    }
}
