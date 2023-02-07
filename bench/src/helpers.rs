use std::str::FromStr;

use anyhow::Context;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    hash::Hash,
    instruction::Instruction,
    message::Message,
    pubkey::Pubkey,
    signature::{Keypair, Signature},
    signer::Signer,
    system_instruction,
    transaction::Transaction,
};

const MEMO_PROGRAM_ID: &str = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";

pub struct BenchHelper;

impl BenchHelper {
    pub async fn get_payer() -> anyhow::Result<Keypair> {
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
        rpc_client: &RpcClient,
        sig: &Signature,
        commitment_config: CommitmentConfig,
    ) -> anyhow::Result<()> {
        loop {
            if let Some(err) = rpc_client
                .get_signature_status_with_commitment(sig, commitment_config)
                .await?
            {
                err?;
                return Ok(());
            }
        }
    }

    pub fn create_transaction(funded_payer: &Keypair, blockhash: Hash) -> Transaction {
        let to_pubkey = Pubkey::new_unique();

        // transfer instruction
        let instruction =
            system_instruction::transfer(&funded_payer.pubkey(), &to_pubkey, 1_000_000);

        let message = Message::new(&[instruction], Some(&funded_payer.pubkey()));

        Transaction::new(&[funded_payer], message, blockhash)
    }

    pub async fn generate_txs(
        num_of_txs: usize,
        funded_payer: &Keypair,
        blockhash: Hash,
    ) -> anyhow::Result<Vec<Transaction>> {
        let mut txs = Vec::with_capacity(num_of_txs);

        for _ in 0..num_of_txs {
            txs.push(Self::create_transaction(funded_payer, blockhash));
        }

        Ok(txs)
    }

    pub async fn create_memo_tx(
        msg: &[u8],
        payer: &Keypair,
        blockhash: Hash,
    ) -> anyhow::Result<Transaction> {
        let memo = Pubkey::from_str(MEMO_PROGRAM_ID)?;

        let instruction = Instruction::new_with_bytes(memo, msg, vec![]);
        let message = Message::new(&[instruction], Some(&payer.pubkey()));
        let tx = Transaction::new(&[payer], message, blockhash);

        Ok(tx)
    }
}
