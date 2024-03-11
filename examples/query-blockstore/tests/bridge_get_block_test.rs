use log::info;
use solana_rpc_client::rpc_client::RpcClient;
use solana_rpc_client_api::client_error::{Error, ErrorKind};
use solana_rpc_client_api::config::RpcBlockConfig;
use solana_rpc_client_api::request::RpcError::{RpcResponseError};
use solana_sdk::commitment_config::CommitmentConfig;
use solana_transaction_status::{EncodedConfirmedBlock, TransactionDetails, UiTransactionEncoding};

// "lite-rpc grpc blockstore -testnet"

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use assert_json_diff::{assert_json_eq, assert_json_include};
    use serde_json::Value;
    use solana_account_decoder::parse_token::UiTokenAmount;
    use solana_sdk::blake3::Hash;
    use solana_sdk::clock::{Slot, UnixTimestamp};
    use solana_sdk::hash::Hasher;
    use solana_sdk::instruction::CompiledInstruction;
    use solana_sdk::message::{legacy, LegacyMessage};
    use solana_sdk::reward_type::RewardType;
    use solana_sdk::system_instruction::SystemInstruction;
    use solana_sdk::transaction::{TransactionVersion, VersionedTransaction};
    use solana_sdk::vote;
    use solana_sdk::vote::instruction::VoteInstruction;
    use solana_transaction_status::{BlockEncodingOptions, EncodedTransaction, Reward, TransactionBinaryEncoding, TransactionTokenBalance, UiConfirmedBlock, UiTransactionTokenBalance};
    use solana_transaction_status::TransactionDetails::Signatures;
    use tracing::warn;
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
        let slot = 257671036;
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
    fn test_batch_compare() {
        tracing_subscriber::fmt::init();
        let slot = 253506193;
        let rpc_local = create_local_rpc_client();
        let rpc_reference = create_reference_rpc_client();


        // None: all MATCH
        // Full/Json: all MATCH
        // Full/Base58: all MATCH
        // Full/Base64: all MATCH
        // Full/Binary: all MATCH
        //
        // rewards: true


        for slot in slot-60..slot-40 {
            info!("check slot {}", slot);
            let check_block = rpc_reference.get_block_with_config(slot, RpcBlockConfig {
                encoding: Some(UiTransactionEncoding::Base58),
                transaction_details: Some(TransactionDetails::Full),
                rewards: Some(true),
                commitment: None,
                max_supported_transaction_version: Some(0),
            }).is_ok();
            if !check_block {
                warn!("Skip block {} (not on reference system)", slot);
                continue;
            }

            let check_block = rpc_local.get_block_with_config(slot, RpcBlockConfig {
                encoding: None,
                transaction_details: Some(TransactionDetails::None),
                rewards: Some(false),
                commitment: None,
                max_supported_transaction_version: Some(0),
            }).is_ok();
            if !check_block {
                warn!("Skip block {} (not on local)", slot);
                continue;
            }

            let config = RpcBlockConfig {
                encoding: Some(UiTransactionEncoding::Base58),
                transaction_details: Some(TransactionDetails::Full),
                rewards: Some(true),
                commitment: None,
                max_supported_transaction_version: Some(0),
            };

            assert_same_response_success(slot, config);
            info!("MATCH for block {}", slot);
        }
    }


    #[test]
    fn transactions_signatures() {
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
        let slot = 253508366;
        let config = RpcBlockConfig {
            encoding: Some(UiTransactionEncoding::Json),
            transaction_details: Some(TransactionDetails::Full),
            rewards: Some(true),
            commitment: None,
            max_supported_transaction_version: Some(0),
        };

        assert_same_response_success(slot, config);
    }

    #[test]
    fn transactions_full_base58() {
        let slot = 257671036;
        let config = RpcBlockConfig {
            encoding: Some(UiTransactionEncoding::Base58),
            transaction_details: Some(TransactionDetails::Full),
            rewards: Some(true),
            commitment: None,
            max_supported_transaction_version: Some(0),
        };

        assert_same_response_success(slot, config);
    }

    fn assert_same_response_success(slot: u64, config: RpcBlockConfig) {
        let mainnet = format!("https://mango.rpcpool.com/{mainnet_api_token}", mainnet_api_token = std::env::var("MAINNET_API_TOKEN").unwrap());
        let testnet = format!("https://api.testnet.rpcpool.com/{testnet_api_token}", testnet_api_token = std::env::var("TESTNET_API_TOKEN").unwrap());
        let reference_rpc = mainnet;
        let local = "http://localhost:8890";
        let rpc_client1 = solana_rpc_client::rpc_client::RpcClient::new(local.to_string());
        let rpc_client2 = solana_rpc_client::rpc_client::RpcClient::new(reference_rpc.to_string());

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
        let rpc_client2 = create_reference_rpc_client();

        let result1 = rpc_client1.get_block_with_config(slot, config);
        let result2 = rpc_client2.get_block_with_config(slot, config);

        assert!(result1.is_err(), "call to local must fail");
        assert!(result2.is_err(), "call to testnet must fail");

        println!("comparing responses for block {}", slot);
        assert_eq!(result2.unwrap_err().to_string(), result1.unwrap_err().to_string());
    }

    // RPC to compare with
    fn create_reference_rpc_client() -> RpcClient {
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
        let slot = create_reference_rpc_client().get_slot().unwrap();
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

