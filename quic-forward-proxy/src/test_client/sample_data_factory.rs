use std::path::Path;
use std::str::FromStr;
use log::warn;
use solana_sdk::hash::Hash;
use solana_sdk::instruction::Instruction;
use solana_sdk::message::Message;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, keypair, Signature, Signer};
use solana_sdk::transaction::{Transaction, VersionedTransaction};
const MEMO_PROGRAM_ID: &str = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";



pub fn build_raw_sample_tx() -> Vec<u8> {

    warn!("Use synthetic keypair for payer!");
    let payer_keypair = Keypair::from_base58_string("rKiJ7H5UUp3JR18kNyTF1XPuwPKHEM7gMLWHZPWP5djrW1vSjfwjhvJrevxF9MPmUmN9gJMLHZdLMgc9ao78eKr");

    let tx = build_sample_tx(&payer_keypair);

    let raw_tx = bincode::serialize::<VersionedTransaction>(&tx).expect("failed to serialize tx");

    raw_tx
}

fn build_sample_tx(payer_keypair: &Keypair) -> VersionedTransaction {
    let blockhash = Hash::default();
    create_memo_tx(b"hi", payer_keypair, blockhash).into()
}

// from bench helpers
fn create_memo_tx(msg: &[u8], payer: &Keypair, blockhash: Hash) -> Transaction {
    let memo = Pubkey::from_str(MEMO_PROGRAM_ID).unwrap();

    let instruction = Instruction::new_with_bytes(memo, msg, vec![]);
    let message = Message::new(&[instruction], Some(&payer.pubkey()));
    Transaction::new(&[payer], message, blockhash)
}


