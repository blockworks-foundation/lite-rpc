use jsonrpsee::types::error::INVALID_PARAMS_CODE;
use log::info;
use solana_rpc_client::rpc_client::RpcClient;
use solana_rpc_client_api::client_error::{Error, ErrorKind};
use solana_rpc_client_api::client_error::ErrorKind::RpcError;
use solana_rpc_client_api::config::RpcBlockConfig;
use solana_rpc_client_api::request::RpcError::{RpcRequestError, RpcResponseError};
use solana_rpc_client_api::request::RpcResponseErrorData;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::message::{MessageHeader, v0, VersionedMessage};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_sdk::transaction::VersionedTransaction;
use solana_transaction_status::{ConfirmedBlock, EncodedConfirmedBlock, EncodedTransactionWithStatusMeta, Rewards, TransactionDetails, TransactionStatusMeta, TransactionWithStatusMeta, UiTransactionEncoding, VersionedTransactionWithStatusMeta};
use solana_lite_rpc_core::structures::produced_block::TransactionInfo;
use crate::errors::JsonRpcError;

// "lite-rpc grpc blockstore -testnet"

#[cfg(test)]
mod tests {
    use assert_json_diff::{assert_json_eq, assert_json_include};
    use solana_sdk::clock::Slot;
    use solana_sdk::transaction::VersionedTransaction;
    use solana_transaction_status::UiConfirmedBlock;
    use super::*;

    #[test]
    fn reject_request_for_processed() {
        let rpc_client = create_local_rpc_client();
        let slot = get_recent_slot();
        let config = RpcBlockConfig {
            encoding: Some(UiTransactionEncoding::Base58),
            transaction_details: Some(TransactionDetails::None),
            rewards: Some(true),
            commitment: Some(CommitmentConfig::processed()),
            max_supported_transaction_version: Some(0),
        };
        let result = rpc_client.get_block_with_config(slot, config);
        assert_eq!(result.unwrap_err().to_string(), "RPC response error -32602: Method does not support commitment below `confirmed` ");
    }


    #[test]
    fn transactions_none_base58() {
        let slot = get_recent_slot();
        let config = RpcBlockConfig {
            encoding: Some(UiTransactionEncoding::Base58),
            transaction_details: Some(TransactionDetails::None),
            rewards: Some(true),
            commitment: None,
            max_supported_transaction_version: Some(0),
        };

        assert_same_response_success(slot, config);
    }

    #[test]
    fn transactions_signatures_base58() {
        let slot = get_recent_slot();
        let config = RpcBlockConfig {
            encoding: Some(UiTransactionEncoding::Base58),
            transaction_details: Some(TransactionDetails::Signatures),
            rewards: Some(true),
            commitment: None,
            max_supported_transaction_version: Some(0),
        };

        assert_same_response_success(slot, config);
    }

    #[test]
    fn transactions_full() {
        let slot = get_recent_slot();
        println!("asking slot slot: {slot}");
        let config = RpcBlockConfig {
            encoding: Some(UiTransactionEncoding::Base58),
            transaction_details: Some(TransactionDetails::Full),
            rewards: Some(true),
            commitment: None,
            max_supported_transaction_version: Some(0),
        };

        assert_same_response_success(slot, config);
    }

    #[test]
    fn transaction_order_full_vs_signatures() {
        let rpc_client = create_local_rpc_client();
        let slot = 256903604;
        let config1 = RpcBlockConfig {
            encoding: Some(UiTransactionEncoding::Base58),
            transaction_details: Some(TransactionDetails::Full),
            rewards: Some(true),
            commitment: None,
            max_supported_transaction_version: Some(0),
        };
        let config2 = RpcBlockConfig {
            encoding: Some(UiTransactionEncoding::Base58),
            transaction_details: Some(TransactionDetails::Signatures),
            rewards: Some(true),
            commitment: None,
            max_supported_transaction_version: Some(0),
        };

        let block1 = rpc_client.get_block_with_config(slot, config1).unwrap();
        let block2 = rpc_client.get_block_with_config(slot, config2).unwrap();

        // for tx in &block1.transactions.unwrap() {
        //     let zzz: VersionedTransaction = tx.transaction.decode().unwrap();
        //     println!("tx: {}", zzz.signatures);
        // }
    }


    fn assert_same_response_success(slot: u64, config: RpcBlockConfig) {
        let mainnet = "http://api.mainnet-beta.solana.com/";
        let testnet = format!("https://api.testnet.rpcpool.com/{testnet_api_token}", testnet_api_token = std::env::var("TESTNET_API_TOKEN").unwrap());
        let local = "http://localhost:8890";
        let rpc_client1 = solana_rpc_client::rpc_client::RpcClient::new(local.to_string());
        let rpc_client2 = solana_rpc_client::rpc_client::RpcClient::new(testnet.to_string());

        let result1 = rpc_client1.get_block_with_config(slot, config);
        let result1 = result1.expect("call to local must succeed");
        let result1 = truncate(result1);

        let result2 = rpc_client2.get_block_with_config(slot, config);
        let result2 = result2.expect("call to testnet must succeed");
        let result2 = truncate(result2);

        println!("comparing responses for block {}", slot);
        println!("-------result1-------\n");
        println!("{}", serde_json::to_string(&result1).unwrap());
        println!("=======result1=======\n");
        println!("\n");
        println!("-------result2-------\n");
        println!("{}", serde_json::to_string(&result2).unwrap());
        println!("=======result2=======\n");
        assert_json_eq!(result2, result1);
    }

    fn truncate(mut result1: UiConfirmedBlock) -> UiConfirmedBlock {
        const LIMIT: usize = 5;
        result1.transactions = Some(result1.transactions.unwrap().into_iter().take(LIMIT).collect());
        result1
    }

    fn assert_same_response_fail(slot: u64, config: RpcBlockConfig) {
        let rpc_client1 = create_local_rpc_client();
        let rpc_client2 = create_testnet_rpc_client();

        let result1 = rpc_client1.get_block_with_config(slot, config);
        let result2 = rpc_client2.get_block_with_config(slot, config);

        assert!(result1.is_err(), "call to local must fail");
        assert!(result2.is_err(), "call to testnet must fail");

        println!("comparing responses for block {}", slot);
        assert_eq!(result2.unwrap_err().to_string(), result1.unwrap_err().to_string());
    }

    fn create_testnet_rpc_client() -> RpcClient {
        let testnet = format!("https://api.testnet.rpcpool.com/{testnet_api_token}", testnet_api_token = std::env::var("TESTNET_API_TOKEN").unwrap());
        let rpc_client2 = solana_rpc_client::rpc_client::RpcClient::new(testnet.to_string());
        rpc_client2
    }

    fn create_local_rpc_client() -> RpcClient {
        let local = "http://localhost:8890";
        let rpc_client1 = solana_rpc_client::rpc_client::RpcClient::new(local.to_string());
        rpc_client1
    }

    fn get_recent_slot() -> Slot {
        let slot = create_testnet_rpc_client().get_slot().unwrap();
        let slot = slot - 40;
        slot
    }

    fn extract_error(result: Result<EncodedConfirmedBlock, Error>) {
        if let Err(error) = result {
            if let ErrorKind::RpcError(RpcResponseError {
                                           code,
                                           message,
                                           data,
                                       }) = error.kind {
                println!("Error: {}", code);
            }
        }
    }
}

