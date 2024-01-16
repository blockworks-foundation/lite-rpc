use anyhow::Context;
use futures::future::join_all;
use indicatif::MultiProgress;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::hash::Hash;
use solana_sdk::signature::Keypair;
use solana_sdk::slot_history::Slot;
use solana_sdk::transaction::Transaction;
use solana_transaction_status::TransactionConfirmationStatus;

use crate::helpers::{BenchHelper, Rng8};

use super::Strategy;
use crate::cli::{ExtraRpcArgs, LiteRpcArgs, RpcArgs};

#[derive(Debug, serde::Serialize)]
pub struct Tc1Result {
    rpc: String,
    slot: Slot,
}

/// send 2 txs (one via LiteRPC, one via Solana RPC) and compare confirmation slot (=slot distance)
#[derive(clap::Args, Debug)]
pub struct Tc1 {
    #[command(flatten)]
    rpc_args: RpcArgs,

    #[command(flatten)]
    lite_rpc_args: LiteRpcArgs,

    #[command(flatten)]
    other_rpcs: ExtraRpcArgs,
}

impl Tc1 {
    async fn create_tx(
        &self,
        hash: Hash,
        payer: &Keypair,
        rng: &mut Rng8,
    ) -> anyhow::Result<Transaction> {
        Ok(BenchHelper::create_memo_tx(
            payer,
            hash,
            rng,
            self.rpc_args.tx_size,
        ))
    }

    async fn send_transaction_and_get_slot(
        &self,
        client: &RpcClient,
        tx: Transaction,
        multi_progress_bar: &MultiProgress,
    ) -> anyhow::Result<u64> {
        let status = BenchHelper::send_and_confirm_transactions(
            client,
            &[tx],
            TransactionConfirmationStatus::Confirmed,
            self.rpc_args.confirmation_retries,
            multi_progress_bar,
        )
        .await?
        .into_iter()
        .next()
        .unwrap()?
        .context("unable to confirm tx")?;

        Ok(status.slot)
    }
}

#[async_trait::async_trait]
impl Strategy for Tc1 {
    type Output = Vec<Tc1Result>;

    async fn execute(&self) -> anyhow::Result<Self::Output> {
        let mut rng = BenchHelper::create_rng(None);
        let payer = BenchHelper::get_payer(&self.rpc_args.payer).await?;

        let multi_progress_bar = MultiProgress::new();

        let endpoints = {
            let mut endpoints = vec![
                self.rpc_args.rpc_addr.clone(),
                self.lite_rpc_args.lite_rpc_addr.clone(),
            ];

            if let Some(extra_endpoints) = &self.other_rpcs.other_rpcs {
                endpoints.extend(extra_endpoints.clone());
            }

            endpoints
        };

        let rpcs = endpoints
            .iter()
            .map(|rpc_addr| RpcClient::new(rpc_addr.to_string()))
            .collect::<Vec<_>>();

        let txs = {
            let hash = rpcs[0].get_latest_blockhash().await?;
            let mut txs = Vec::with_capacity(rpcs.len());
            for _ in rpcs.iter() {
                txs.push(self.create_tx(hash, &payer, &mut rng).await?);
            }
            txs
        };

        let slots = join_all(rpcs.iter().zip(txs.iter()).map(|(rpc, tx)| {
            self.send_transaction_and_get_slot(rpc, tx.clone(), &multi_progress_bar)
        }))
        .await;

        // filter out errors and log them
        let res = slots
            .into_iter()
            .zip(endpoints.iter())
            .filter_map(|(slot, endpoint)| match slot {
                Ok(slot) => Some(Tc1Result {
                    rpc: endpoint.to_owned(),
                    slot,
                }),
                Err(err) => {
                    log::error!("error with endpoint {} : {}", endpoint, err);
                    None
                }
            })
            .collect::<Vec<_>>();

        Ok(res)
    }
}
