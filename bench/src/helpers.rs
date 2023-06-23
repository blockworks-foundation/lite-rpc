use anyhow::Context;
use rand::{distributions::Alphanumeric, prelude::Distribution, SeedableRng};
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
use std::{str::FromStr, time::Duration};
use std::path::PathBuf;
use lazy_static::lazy_static;
use tokio::time::Instant;

const MEMO_PROGRAM_ID: &str = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";
const WAIT_LIMIT_IN_SECONDS: u64 = 60;

lazy_static! {
    static ref USER_KEYPAIR: PathBuf = {
        dirs::home_dir().unwrap()
            .join(".config")
            .join("solana")
            .join("id.json")
    };
}


pub struct BenchHelper;

impl BenchHelper {

    pub async fn get_payer() -> anyhow::Result<Keypair> {

        let payer = tokio::fs::read_to_string(USER_KEYPAIR.as_path())
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
        let instant = Instant::now();
        loop {
            if instant.elapsed() > Duration::from_secs(WAIT_LIMIT_IN_SECONDS) {
                return Err(anyhow::Error::msg("Timedout waiting"));
            }
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

    pub fn generate_random_strings(num_of_txs: usize, random_seed: Option<u64>) -> Vec<Vec<u8>> {
        let seed = random_seed.map_or(0, |x| x);
        let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(seed);
        (0..num_of_txs)
            .map(|_| Alphanumeric.sample_iter(&mut rng).take(10).collect())
            .collect()
    }

    #[inline]
    pub fn generate_txs(
        num_of_txs: usize,
        funded_payer: &Keypair,
        blockhash: Hash,
        random_seed: Option<u64>,
    ) -> Vec<Transaction> {
        let seed = random_seed.map_or(0, |x| x);
        let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(seed);
        (0..num_of_txs)
            .map(|_| {
                let random_bytes: Vec<u8> = Alphanumeric.sample_iter(&mut rng).take(10).collect();

                Self::create_memo_tx(&random_bytes, funded_payer, blockhash)
            })
            .collect()
    }

    pub fn create_memo_tx(msg: &[u8], payer: &Keypair, blockhash: Hash) -> Transaction {
        let memo = Pubkey::from_str(MEMO_PROGRAM_ID).unwrap();

        let instruction = Instruction::new_with_bytes(memo, msg, vec![]);
        let message = Message::new(&[instruction], Some(&payer.pubkey()));
        Transaction::new(&[payer], message, blockhash)
    }
}
