use anyhow::{bail, Context};
use futures::future::join_all;
use itertools::Itertools;
use lazy_static::lazy_static;
use rand::{distributions::Alphanumeric, prelude::Distribution, SeedableRng};
use solana_rpc_client::{nonblocking::rpc_client::RpcClient, rpc_client::SerializableTransaction};
use solana_sdk::{
    commitment_config::CommitmentConfig,
    compute_budget::ComputeBudgetInstruction,
    hash::Hash,
    instruction::{AccountMeta, Instruction},
    message::Message,
    pubkey::Pubkey,
    signature::{Keypair, Signature},
    signer::Signer,
    signers::Signers,
    system_instruction,
    transaction::Transaction,
};

use solana_transaction_status::{TransactionConfirmationStatus, TransactionStatus};
use std::path::Path;
use std::{str::FromStr, time::Duration};
use tokio::time::Instant;

use crate::tx_size::TxSize;

const MEMO_PROGRAM_ID: &str = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";
const WAIT_LIMIT_IN_SECONDS: u64 = 60;

lazy_static! {
    pub static ref USER_KEYPAIR_PATH: String = dirs::home_dir()
        .unwrap()
        .join(".config")
        .join("solana")
        .join("id.json")
        .to_str()
        .unwrap_or_default()
        .to_string();
}

pub type Rng8 = rand_chacha::ChaCha8Rng;

pub struct BenchHelper;

impl BenchHelper {
    pub async fn get_payer(path: impl AsRef<Path>) -> anyhow::Result<Keypair> {
        let payer = tokio::fs::read_to_string(path)
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

    pub async fn send_and_confirm_transactions(
        rpc_client: &RpcClient,
        txs: &[impl SerializableTransaction],
        commitment_config: TransactionConfirmationStatus,
        tries: Option<usize>,
    ) -> anyhow::Result<Vec<anyhow::Result<Option<TransactionStatus>>>> {
        let url = rpc_client.url();

        let sigs = join_all(txs.iter().map(|tx| {
            log::info!("{url} Sending transaction {}", tx.get_signature());

            rpc_client.send_transaction(tx)
        }))
        .await;

        let mut results: Vec<anyhow::Result<Option<TransactionStatus>>> = sigs
            .iter()
            .map(|sig| {
                let Err(err) = sig else {
                    return Ok(None);
                };

                bail!("Error sending transaction: {:?}", err)
            })
            .collect_vec();

        // 5 tries
        for _ in 0..tries.unwrap_or(100) {
            let sigs = results
                .iter()
                .enumerate()
                .filter_map(|(index, result)| match result {
                    Ok(status) => {
                        if let Some(status) = status {
                            if status.confirmation_status() == commitment_config {
                                return None;
                            }
                        }

                        let sig = sigs[index].as_ref().unwrap().to_owned();
                        log::info!("{url} Waiting for {commitment_config:?} of {}", sig);

                        Some(sig)
                    }
                    _ => None,
                })
                .collect_vec();

            let mut statuses = rpc_client
                .get_signature_statuses(&sigs)
                .await?
                .value
                .into_iter();

            results.iter_mut().enumerate().for_each(|(index, result)| {
                if let Ok(None) = result {
                    *result = Ok(statuses.next().unwrap());
                    let sig = &sigs[index];
                    match result {
                        Ok(Some(status)) => {
                            log::info!(
                                "{url} Transaction {:?} {:?} in slot {:?}",
                                sig,
                                status.confirmation_status(),
                                status.slot,
                            );
                        }
                        Ok(None) => {
                            log::warn!("{url} No status found for {}", sig);
                        }
                        _ => unreachable!(),
                    }
                }
            });

            if results.iter().all(|result| match result {
                Err(_) => true,
                Ok(None) => false,
                Ok(Some(status)) => status.confirmation_status() == commitment_config,
            }) {
                break;
            }

            log::info!("{url} Waiting for {commitment_config:?} (500ms)...");
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        Ok(results)
    }

    pub fn create_transaction(funded_payer: &Keypair, blockhash: Hash) -> Transaction {
        let to_pubkey = Pubkey::new_unique();

        // transfer instruction
        let instruction =
            system_instruction::transfer(&funded_payer.pubkey(), &to_pubkey, 1_000_000);

        let message = Message::new(&[instruction], Some(&funded_payer.pubkey()));

        Transaction::new(&[funded_payer], message, blockhash)
    }

    #[inline]
    pub fn create_rng(seed: Option<u64>) -> Rng8 {
        let seed = seed.map_or(0, |x| x);
        Rng8::seed_from_u64(seed)
    }

    #[inline]
    pub fn generate_random_string(rng: &mut Rng8, n_chars: usize) -> Vec<u8> {
        Alphanumeric.sample_iter(rng).take(n_chars).collect()
    }

    #[inline]
    pub fn generate_random_strings(rng: &mut Rng8, amount: usize, n_chars: usize) -> Vec<Vec<u8>> {
        (0..amount)
            .map(|_| Self::generate_random_string(rng, n_chars))
            .collect()
    }

    #[inline]
    pub fn generate_txs(
        num_of_txs: usize,
        payer: &Keypair,
        blockhash: Hash,
        rng: &mut Rng8,
        size: TxSize,
    ) -> Vec<Transaction> {
        (0..num_of_txs)
            .map(|_| Self::create_memo_tx(payer, blockhash, rng, size))
            .collect()
    }

    pub fn create_memo_tx(
        payer: &Keypair,
        blockhash: Hash,
        rng: &mut Rng8,
        size: TxSize,
    ) -> Transaction {
        let rand_str = Self::generate_random_string(rng, size.memo_size());

        match size {
            TxSize::Small => Self::create_memo_tx_small(&rand_str, payer, blockhash),
            TxSize::Large => Self::create_memo_tx_large(&rand_str, payer, blockhash),
        }
    }

    /// first signer is payer
    pub fn create_tx_with_cu<T: Signers + ?Sized>(
        signers: &T,
        blockhash: Hash,
        mut instructions: Vec<Instruction>,
        priority_fee: Option<u64>,
        cu_budget: Option<u32>,
    ) -> Transaction {
        let cu_budget = cu_budget.or_else(|| {
            std::env::var("CU_BUDGET")
                .ok()
                .and_then(|budget_str| budget_str.parse::<u32>().ok())
        });

        let priority_fee = priority_fee.or_else(|| {
            std::env::var("PRIORITY_FEE_MICRO_LAMPORTS")
                .ok()
                .and_then(|fee_str| fee_str.parse::<u64>().ok())
        });

        if let Some(cu_budget) = cu_budget {
            let cu_limit = ComputeBudgetInstruction::set_compute_unit_limit(cu_budget);
            instructions.push(cu_limit);
        }

        if let Some(priority_fee) = priority_fee {
            let cu_price = ComputeBudgetInstruction::set_compute_unit_price(priority_fee);
            instructions.push(cu_price);
        }

        let message = Message::new(&instructions, Some(signers.pubkeys().first().unwrap()));

        Transaction::new(signers, message, blockhash)
    }

    pub fn create_memo_tx_small(msg: &[u8], payer: &Keypair, blockhash: Hash) -> Transaction {
        let memo = Pubkey::from_str(MEMO_PROGRAM_ID).unwrap();

        let instruction = Instruction::new_with_bytes(memo, msg, vec![]);

        Self::create_tx_with_cu(&[payer], blockhash, vec![instruction], None, None)
    }

    pub fn create_memo_tx_large(msg: &[u8], payer: &Keypair, blockhash: Hash) -> Transaction {
        let accounts = (0..8).map(|_| Keypair::new()).collect_vec();

        let memo = Pubkey::from_str(MEMO_PROGRAM_ID).unwrap();

        let instruction = Instruction::new_with_bytes(
            memo,
            msg,
            accounts
                .iter()
                .map(|keypair| AccountMeta::new_readonly(keypair.pubkey(), true))
                .collect_vec(),
        );

        let mut signers = vec![payer];
        signers.extend(accounts.iter());

        Self::create_tx_with_cu(&signers, blockhash, vec![instruction], None, None)
    }
}

#[test]
fn transaction_size_small() {
    let blockhash = Hash::default();
    let payer_keypair = Keypair::from_base58_string(
        "rKiJ7H5UUp3JR18kNyTF1XPuwPKHEM7gMLWHZPWP5djrW1vSjfwjhvJrevxF9MPmUmN9gJMLHZdLMgc9ao78eKr",
    );

    let mut rng = BenchHelper::create_rng(Some(42));
    let rand_string = BenchHelper::generate_random_string(&mut rng, 10);

    let tx = BenchHelper::create_memo_tx_small(&rand_string, &payer_keypair, blockhash);

    assert_eq!(bincode::serialized_size(&tx).unwrap(), 179);
}

#[test]
fn transaction_size_large() {
    let blockhash = Hash::default();
    let payer_keypair = Keypair::from_base58_string(
        "rKiJ7H5UUp3JR18kNyTF1XPuwPKHEM7gMLWHZPWP5djrW1vSjfwjhvJrevxF9MPmUmN9gJMLHZdLMgc9ao78eKr",
    );

    let mut rng = BenchHelper::create_rng(Some(42));
    let rand_string = BenchHelper::generate_random_string(&mut rng, 240);

    let tx = BenchHelper::create_memo_tx_large(&rand_string, &payer_keypair, blockhash);

    assert_eq!(bincode::serialized_size(&tx).unwrap(), 1186);
}
