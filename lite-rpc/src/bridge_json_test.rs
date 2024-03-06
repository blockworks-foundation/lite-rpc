use jsonrpsee::types::error::INVALID_PARAMS_CODE;
use log::info;
use solana_rpc_client::rpc_client::RpcClient;
use solana_rpc_client_api::client_error::{Error, ErrorKind};
use solana_rpc_client_api::client_error::ErrorKind::RpcError;
use solana_rpc_client_api::config::RpcBlockConfig;
use solana_rpc_client_api::request::RpcError::{RpcRequestError, RpcResponseError};
use solana_rpc_client_api::request::RpcResponseErrorData;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_transaction_status::{EncodedConfirmedBlock, EncodedTransactionWithStatusMeta, TransactionDetails, UiTransactionEncoding};
use crate::errors::JsonRpcError;


#[cfg(test)]
mod tests {
    use assert_json_diff::{assert_json_eq, assert_json_include};
    use super::*;

    #[test]
    fn error_json() {
        let msg = format!("Method does not support commitment below `confirmed`");
        let err = jsonrpsee::types::error::ErrorObject::owned(INVALID_PARAMS_CODE, msg, None::<()>);
        assert_eq!(serde_json::to_string(&err).unwrap(), r#"{"code":-32602,"message":"Method does not support commitment below `confirmed`"}"#);
    }

    #[test]
    fn reject_request_for_processed() {
        let rpc_client = create_rpc_client();

        // error
        let slot = 256715762;
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

    fn assert_same_response(slot: u64, config: RpcBlockConfig) {
        let mainnet = "http://api.mainnet-beta.solana.com/";
        let testnet = format!("https://api.testnet.rpcpool.com/{testnet_api_token}", testnet_api_token = std::env::var("TESTNET_API_TOKEN").unwrap());
        let local = "http://localhost:8890";
        let rpc_client1 = solana_rpc_client::rpc_client::RpcClient::new(local.to_string());
        let rpc_client2 = solana_rpc_client::rpc_client::RpcClient::new(testnet.to_string());

        let result1 = rpc_client1.get_block_with_config(slot, config);
        let result2 = rpc_client2.get_block_with_config(slot, config);

        assert_json_eq!(result1.unwrap(), result2.unwrap());
        // assert!(format!("{:?}", result1) == format!("{:?}", result2), "must return the same result");
    }

    #[test]
    fn transactions_none_base58() {
        // error
        let slot = 256718715;
        let config = RpcBlockConfig {
            encoding: Some(UiTransactionEncoding::Base58),
            transaction_details: Some(TransactionDetails::None),
            rewards: Some(true),
            commitment: None,
            max_supported_transaction_version: Some(0),
        };

        assert_same_response(slot, config);
    }


    fn create_rpc_client() -> RpcClient {
        let mainnet = "http://api.mainnet-beta.solana.com/";
        let testnet = format!("https://api.testnet.rpcpool.com/{testnet_api_token}", testnet_api_token = std::env::var("TESTNET_API_TOKEN").unwrap());
        let local = "http://localhost:8890";
        let rpc_client = solana_rpc_client::rpc_client::RpcClient::new(testnet.to_string());

        rpc_client
    }


    #[test]
    fn connect() {
        let mainnet = "http://api.mainnet-beta.solana.com/";
        let testnet = format!("https://api.testnet.rpcpool.com/{testnet_api_token}", testnet_api_token = std::env::var("TESTNET_API_TOKEN").unwrap());
        let local = "http://localhost:8890";
        let rpc_client = solana_rpc_client::rpc_client::RpcClient::new(testnet.to_string());

        // error
        let slot = 256715762;
        let result = rpc_client.get_block(slot).unwrap();
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
