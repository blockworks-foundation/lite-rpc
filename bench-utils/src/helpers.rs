use anyhow::bail;
use lite_client::LiteClient;
use log::info;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;

use solana_sdk::hash::Hash;
use solana_sdk::signature::Signature;
use solana_sdk::{
    message::Message, pubkey::Pubkey, signature::Keypair, signer::Signer, system_instruction,
    transaction::Transaction,
};

pub async fn new_funded_payer(rpc_client: &RpcClient, amount: u64) -> anyhow::Result<Keypair> {
    let payer = Keypair::new();
    let payer_pubkey = payer.pubkey().to_string();

    // request airdrop to payer
    let airdrop_sig = rpc_client.request_airdrop(&payer.pubkey(), amount).await?;

    info!("Air Dropping {payer_pubkey} with {amount}L");

    loop {
        if let Some(res) = rpc_client
            .get_signature_status_with_commitment(&airdrop_sig, CommitmentConfig::finalized())
            .await?
        {
            match res {
                Ok(_) => break,
                Err(_) => bail!("Error air dropping {payer_pubkey}"),
            }
        }
    }

    info!("Air Drop Successful: {airdrop_sig}");

    Ok(payer)
}

pub async fn wait_till_confirmed(lite_client: &LiteClient, sig: &Signature) {
    while lite_client.confirm_transaction(sig.to_string()).await {}
}

pub fn create_transaction(funded_payer: &Keypair, blockhash: Hash) -> Transaction {
    let to_pubkey = Pubkey::new_unique();

    // transfer instruction
    let instruction = system_instruction::transfer(&funded_payer.pubkey(), &to_pubkey, 1_000_000);

    let message = Message::new(&[instruction], Some(&funded_payer.pubkey()));

    Transaction::new(&[funded_payer], message, blockhash)
}

pub async fn generate_txs(
    num_of_txs: usize,
    rpc_client: &RpcClient,
    funded_payer: &Keypair,
) -> anyhow::Result<Vec<Transaction>> {
    let mut txs = Vec::with_capacity(num_of_txs);

    let blockhash = rpc_client.get_latest_blockhash().await?;

    for _ in 0..num_of_txs {
        txs.push(create_transaction(funded_payer, blockhash));
    }

    Ok(txs)
}
