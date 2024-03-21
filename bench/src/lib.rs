use anyhow::bail;
use clap::Parser;
use futures::future::join_all;
use itertools::Itertools;
use log::{debug, warn};
use rand::{distributions::Alphanumeric, prelude::Distribution, SeedableRng};
use solana_rpc_client::{nonblocking::rpc_client::RpcClient, rpc_client::SerializableTransaction};
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    hash::Hash,
    instruction::{AccountMeta, Instruction},
    message::Message,
    pubkey::Pubkey,
    signature::{Keypair, Signature},
    signer::Signer,
    system_instruction,
    transaction::Transaction,
};
use solana_transaction_status::TransactionStatus;
use std::{str::FromStr, time::Duration};
use tokio::time::Instant;
use tx_size::TxSize;

pub mod benches;
pub mod helpers;
pub mod metrics;
pub mod tx_size;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Number of tx(s) sent in each run
    #[arg(short = 'n', long, default_value_t = 5_000)]
    pub tx_count: usize,
    /// Number of bench runs
    #[arg(short = 'r', long, default_value_t = 1)]
    pub runs: usize,
    /// Interval between each bench run (ms)
    #[arg(short = 'i', long, default_value_t = 1000)]
    pub run_interval_ms: u64,
    /// Metrics output file name
    #[arg(short = 'm', long, default_value_t = String::from("metrics.csv"))]
    pub metrics_file_name: String,
    /// Lite Rpc Address
    #[arg(short = 'l', long, default_value_t = String::from("http://127.0.0.1:8890"))]
    pub lite_rpc_addr: String,
    #[arg(short = 't', long, default_value_t = String::from("transactions.csv"))]
    pub transaction_save_file: String,
    // choose between small (179 bytes) and large (1186 bytes) transactions
    #[arg(short = 'L', long, default_value_t = false)]
    pub large_transactions: bool,
}

pub struct BenchmarkTransactionParams {
    pub tx_size: TxSize,
    pub cu_price_micro_lamports: u64,
}

const MEMO_PROGRAM_ID: &str = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";
const WAIT_LIMIT_IN_SECONDS: u64 = 60;

pub type Rng8 = rand_chacha::ChaCha8Rng;

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
    commitment_config: CommitmentConfig,
    tries: Option<usize>,
) -> anyhow::Result<Vec<anyhow::Result<Option<TransactionStatus>>>> {
    let started_at = Instant::now();
    let sigs_or_fails = join_all(txs.iter().map(|tx| rpc_client.send_transaction(tx))).await;

    debug!(
        "sent {} transactions in {:.02}ms",
        txs.len(),
        started_at.elapsed().as_secs_f32() * 1000.0
    );

    let num_sent_ok = sigs_or_fails
        .iter()
        .filter(|sig_or_fail| sig_or_fail.is_ok())
        .count();
    let num_sent_failed = sigs_or_fails
        .iter()
        .filter(|sig_or_fail| sig_or_fail.is_err())
        .count();

    debug!("{} transactions sent successfully", num_sent_ok);
    debug!("{} transactions failed to send", num_sent_failed);

    if num_sent_failed > 0 {
        warn!(
            "Some transactions failed to send: {} out of {}",
            num_sent_failed,
            txs.len()
        );
        bail!("Failed to send all transactions");
    }

    let mut results = sigs_or_fails
        .iter()
        .map(|sig| {
            let Err(err) = sig else {
                return Ok(None);
            };

            bail!("Error sending transaction: {:?}", err)
        })
        .collect_vec();

    // 5 tries
    for _ in 0..tries.unwrap_or(5) {
        let sigs = results
            .iter()
            .enumerate()
            .filter_map(|(index, result)| match result {
                Ok(None) => Some(sigs_or_fails[index].as_ref().unwrap().to_owned()),
                _ => None,
            })
            .collect_vec();

        let mut statuses = rpc_client
            .get_signature_statuses(&sigs)
            .await?
            .value
            .into_iter();

        results.iter_mut().for_each(|result| {
            if let Ok(None) = result {
                *result = Ok(statuses.next().unwrap());
            }
        });

        if results.iter().all(|result| {
            let Ok(result) = result else { return true };
            if let Some(result) = result {
                result.satisfies_commitment(commitment_config)
            } else {
                false
            }
        }) {
            break;
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    Ok(results)
}

pub fn create_transaction(funded_payer: &Keypair, blockhash: Hash) -> Transaction {
    let to_pubkey = funded_payer.pubkey();
    let instruction = system_instruction::transfer(&funded_payer.pubkey(), &to_pubkey, 5000);
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
pub fn generate_txs(
    num_of_txs: usize,
    payer: &Keypair,
    blockhash: Hash,
    rng: &mut Rng8,
    tx_params: &BenchmarkTransactionParams,
) -> Vec<Transaction> {
    (0..num_of_txs)
        .map(|_| create_memo_tx(payer, blockhash, rng, tx_params))
        .collect()
}

pub fn create_memo_tx(
    payer: &Keypair,
    blockhash: Hash,
    rng: &mut Rng8,
    tx_params: &BenchmarkTransactionParams,
) -> Transaction {
    let rand_str = generate_random_string(rng, tx_params.tx_size.memo_size());

    match tx_params.tx_size {
        tx_size::TxSize::Small => create_memo_tx_small(
            &rand_str,
            payer,
            blockhash,
            tx_params.cu_price_micro_lamports,
        ),
        tx_size::TxSize::Large => create_memo_tx_large(
            &rand_str,
            payer,
            blockhash,
            tx_params.cu_price_micro_lamports,
        ),
    }
}

pub fn create_memo_tx_small(
    msg: &[u8],
    payer: &Keypair,
    blockhash: Hash,
    cu_price_micro_lamports: u64,
) -> Transaction {
    let memo = Pubkey::from_str(MEMO_PROGRAM_ID).unwrap();

    let cu_budget_ix: Instruction =
        ComputeBudgetInstruction::set_compute_unit_price(cu_price_micro_lamports);
    // Program consumed: 12775 of 13700 compute units
    let cu_limit_ix: Instruction = ComputeBudgetInstruction::set_compute_unit_limit(14000);
    let instruction = Instruction::new_with_bytes(memo, msg, vec![]);
    let message = Message::new(
        &[cu_budget_ix, cu_limit_ix, instruction],
        Some(&payer.pubkey()),
    );
    Transaction::new(&[payer], message, blockhash)
}

pub fn create_memo_tx_large(
    msg: &[u8],
    payer: &Keypair,
    blockhash: Hash,
    cu_price_micro_lamports: u64,
) -> Transaction {
    let accounts = (0..8).map(|_| Keypair::new()).collect_vec();

    let memo = Pubkey::from_str(MEMO_PROGRAM_ID).unwrap();
    let cu_budget_ix: Instruction =
        ComputeBudgetInstruction::set_compute_unit_price(cu_price_micro_lamports);
    let cu_limit_ix: Instruction = ComputeBudgetInstruction::set_compute_unit_limit(14000);

    let instruction = Instruction::new_with_bytes(
        memo,
        msg,
        accounts
            .iter()
            .map(|keypair| AccountMeta::new_readonly(keypair.pubkey(), true))
            .collect_vec(),
    );
    let message = Message::new(
        &[cu_budget_ix, cu_limit_ix, instruction],
        Some(&payer.pubkey()),
    );

    let mut signers = vec![payer];
    signers.extend(accounts.iter());

    Transaction::new(&signers, message, blockhash)
}

#[test]
fn transaction_size_small() {
    let blockhash = Hash::default();
    let payer_keypair = Keypair::from_base58_string(
        "rKiJ7H5UUp3JR18kNyTF1XPuwPKHEM7gMLWHZPWP5djrW1vSjfwjhvJrevxF9MPmUmN9gJMLHZdLMgc9ao78eKr",
    );
    let mut rng = create_rng(Some(42));
    let rand_string = generate_random_string(&mut rng, 10);
    let priority_fee = 100;

    let tx = create_memo_tx_small(&rand_string, &payer_keypair, blockhash, priority_fee);
    assert_eq!(bincode::serialized_size(&tx).unwrap(), 231);
}

#[test]
fn transaction_size_large() {
    let blockhash = Hash::default();
    let payer_keypair = Keypair::from_base58_string(
        "rKiJ7H5UUp3JR18kNyTF1XPuwPKHEM7gMLWHZPWP5djrW1vSjfwjhvJrevxF9MPmUmN9gJMLHZdLMgc9ao78eKr",
    );
    let mut rng = create_rng(Some(42));
    let rand_string = generate_random_string(&mut rng, 240);
    let priority_fee = 100;

    let tx = create_memo_tx_large(&rand_string, &payer_keypair, blockhash, priority_fee);
    assert_eq!(bincode::serialized_size(&tx).unwrap(), 1230);
}
