use std::{ops::Deref, sync::Arc};

use anyhow::Context;
use lite_client::LiteClient;
use log::info;
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
    pub lite_client: Arc<LiteClient>,
    pub use_signature_status_to_confirm: bool,
}

impl Deref for BenchHelper {
    type Target = LiteClient;

    fn deref(&self) -> &Self::Target {
        &self.lite_client
    }
}

impl BenchHelper {
    pub fn new(lite_client: Arc<LiteClient>) -> Self {
        Self {
            lite_client,
            use_signature_status_to_confirm: false,
        }
    }

    pub async fn new_funded_payer(&self, amount: u64) -> anyhow::Result<Keypair> {
        let payer = Keypair::new();
        let payer_pubkey = payer.pubkey().to_string();

        // request airdrop to payer
        let airdrop_sig = self
            .lite_client
            .request_airdrop(&payer.pubkey(), amount)
            .await
            .context("requesting air drop")?;

        info!("Air Dropping {payer_pubkey} with {amount}L");

        self.wait_till_commitment(&airdrop_sig, CommitmentConfig::finalized())
            .await;

        info!("Air Drop Successful: {airdrop_sig}");

        Ok(payer)
    }

    pub async fn wait_till_block_signature(
        &self,
        sig: &Signature,
        commitment_config: CommitmentConfig,
    ) {
        while self
            .lite_client
            .confirm_transaction_with_commitment(sig.to_string(), commitment_config)
            .await
        {}
    }

    pub async fn wait_till_signature_status(
        &self,
        sig: &Signature,
        commitment_config: CommitmentConfig,
    ) -> anyhow::Result<()> {
        loop {
            if let Some(err) = self
                .lite_client
                .get_signature_status_with_commitment(sig, commitment_config)
                .await?
            {
                err?;
                return Ok(());
            }
        }
    }

    pub async fn wait_till_commitment(&self, sig: &Signature, commitment_config: CommitmentConfig) {
        if self.use_signature_status_to_confirm {
            self.wait_till_signature_status(sig, commitment_config)
                .await
                .unwrap();
        } else {
            self.wait_till_block_signature(sig, commitment_config).await;
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

        let blockhash = self.lite_client.get_latest_blockhash().await?;

        for _ in 0..num_of_txs {
            txs.push(self.create_transaction(funded_payer, blockhash));
        }

        Ok(txs)
    }
}
