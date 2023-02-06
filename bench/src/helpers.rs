use std::{ops::Deref, sync::Arc};

use anyhow::Context;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    hash::Hash,
    message::Message,
    pubkey::Pubkey,
    signature::{Keypair, Signature},
    signer::Signer,
    system_instruction,
    transaction::Transaction,
};

#[derive(Clone)]
pub struct BenchHelper {
    pub rpc_client: Arc<RpcClient>,
}

impl Deref for BenchHelper {
    type Target = RpcClient;

    fn deref(&self) -> &Self::Target {
        &self.rpc_client
    }
}

impl BenchHelper {
    pub fn new(rpc_client: Arc<RpcClient>) -> Self {
        Self { rpc_client }
    }

    pub async fn get_payer(&self) -> anyhow::Result<Keypair> {
        let mut config_dir = dirs::config_dir().context("Unable to get path to user config dir")?;

        config_dir.push("solana");
        config_dir.push("id.json");

        let payer = tokio::fs::read_to_string(config_dir.to_str().unwrap())
            .await
            .context("Error reading payer file")?;
        let payer: Vec<u8> = serde_json::from_str(&payer)?;
        let payer = Keypair::from_bytes(&payer)?;

        Ok(payer)
    }

    pub async fn wait_till_signature_status(
        &self,
        sig: &Signature,
        commitment_config: CommitmentConfig,
    ) -> anyhow::Result<()> {
        loop {
            if let Some(err) = self
                .rpc_client
                .get_signature_status_with_commitment(sig, commitment_config)
                .await?
            {
                err?;
                return Ok(());
            }
        }
    }

    pub fn create_transaction(&self, funded_payer: &Keypair, blockhash: Hash) -> Transaction {
        let to_pubkey = Pubkey::new_unique();

        // transfer instruction
        let instruction =
            system_instruction::transfer(&funded_payer.pubkey(), &to_pubkey, 1_000_000);

        let message = Message::new(&[instruction], Some(&funded_payer.pubkey()));

        Transaction::new(&[funded_payer], message, blockhash)
    }

    pub async fn generate_txs(
        &self,
        num_of_txs: usize,
        funded_payer: &Keypair,
    ) -> anyhow::Result<Vec<Transaction>> {
        let mut txs = Vec::with_capacity(num_of_txs);

        let blockhash = self.rpc_client.get_latest_blockhash().await?;

        for _ in 0..num_of_txs {
            txs.push(self.create_transaction(funded_payer, blockhash));
        }

        Ok(txs)
    }
}
