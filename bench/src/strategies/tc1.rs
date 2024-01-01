use anyhow::Context;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::transaction::Transaction;
use solana_sdk::{commitment_config::CommitmentConfig, signature::Keypair};

use crate::helpers::{BenchHelper, Rng8};

use super::Strategy;
use crate::cli::{LiteRpcArgs, RpcArgs};

#[derive(Debug, serde::Serialize)]
pub struct Tc1Result {
    rpc_slot: u64,
    lite_rpc_slot: u64,
}

/// send 2 txs (one via LiteRPC, one via Solana RPC) and compare confirmation slot (=slot distance)
#[derive(clap::Args, Debug)]
pub struct Tc1 {
    #[command(flatten)]
    rpc_args: RpcArgs,

    #[command(flatten)]
    lite_rpc_args: LiteRpcArgs,
}

impl Tc1 {
    async fn create_tx(
        &self,
        rpc: &RpcClient,
        payer: &Keypair,
        rng: &mut Rng8,
    ) -> anyhow::Result<Transaction> {
        let hash = rpc.get_latest_blockhash().await?;

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
    ) -> anyhow::Result<u64> {
        let status = BenchHelper::send_and_confirm_transactions(
            client,
            &[tx],
            CommitmentConfig::confirmed(),
            None,
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
    type Output = Tc1Result;

    async fn execute(&self) -> anyhow::Result<Self::Output> {
        let lite_rpc = RpcClient::new(self.lite_rpc_args.lite_rpc_addr.clone());
        let rpc = RpcClient::new(self.rpc_args.rpc_addr.clone());

        let mut rng = BenchHelper::create_rng(None);
        let payer = BenchHelper::get_payer(&self.rpc_args.payer).await?;

        let tx = self.create_tx(&rpc, &payer, &mut rng).await?;

        let (rpc_slot, lite_rpc_slot) = tokio::join!(
            self.send_transaction_and_get_slot(&rpc, tx.clone()),
            self.send_transaction_and_get_slot(&lite_rpc, tx)
        );

        Ok(Tc1Result {
            rpc_slot: rpc_slot?,
            lite_rpc_slot: lite_rpc_slot?,
        })
    }
}