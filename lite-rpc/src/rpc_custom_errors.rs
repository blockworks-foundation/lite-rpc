use jsonrpsee::types::ErrorObject;
use serde_json::json;
use solana_rpc_client_api::custom_error::*;

/// adoption of solana-rpc-client-api custom_errors.rs for jsonrpsee

pub fn map_rpc_custom_error<'a>(error: RpcCustomError) -> ErrorObject<'a> {
    match error {
        RpcCustomError::BlockCleanedUp {
            slot,
            first_available_block,
        } =>
            server_error(
                JSON_RPC_SERVER_ERROR_BLOCK_CLEANED_UP,
                format!("Block {slot} cleaned up, does not exist on node. First available block: {first_available_block}"),
            ),
        RpcCustomError::SendTransactionPreflightFailure { message, result } =>
            server_error_data(
                JSON_RPC_SERVER_ERROR_SEND_TRANSACTION_PREFLIGHT_FAILURE,
                message,
                json!(result),
            ),
        RpcCustomError::TransactionSignatureVerificationFailure =>
            server_error(
                JSON_RPC_SERVER_ERROR_TRANSACTION_SIGNATURE_VERIFICATION_FAILURE,
                "Transaction signature verification failure".to_string(),
            ),
        RpcCustomError::BlockNotAvailable { slot } =>
            server_error(
                JSON_RPC_SERVER_ERROR_BLOCK_NOT_AVAILABLE,
                format!("Block not available for slot {slot}"),
            ),
        RpcCustomError::NodeUnhealthy { num_slots_behind } =>
            server_error_data(
                JSON_RPC_SERVER_ERROR_NODE_UNHEALTHY,
                if let Some(num_slots_behind) = num_slots_behind {
                    format!("Node is behind by {num_slots_behind} slots")
                } else {
                    "Node is unhealthy".to_string()
                },
                json!(NodeUnhealthyErrorData {
                    num_slots_behind
                })
            ),
        RpcCustomError::TransactionPrecompileVerificationFailure(e) => server_error(
            JSON_RPC_SERVER_ERROR_TRANSACTION_PRECOMPILE_VERIFICATION_FAILURE,
            format!("Transaction precompile verification failure {e:?}"),
        ),
        RpcCustomError::SlotSkipped { slot } =>
            server_error(
                JSON_RPC_SERVER_ERROR_SLOT_SKIPPED,
                format!("Slot {slot} was skipped, or missing due to ledger jump to recent snapshot"),
            ),
        RpcCustomError::NoSnapshot =>
            server_error(
                JSON_RPC_SERVER_ERROR_NO_SNAPSHOT,
                "No snapshot".to_string(),
            ),
        RpcCustomError::LongTermStorageSlotSkipped { slot } =>
            server_error(
                JSON_RPC_SERVER_ERROR_LONG_TERM_STORAGE_SLOT_SKIPPED,
                format!("Slot {slot} was skipped, or missing in long-term storage"),
            ),
        RpcCustomError::KeyExcludedFromSecondaryIndex { index_key } =>
            server_error(
                JSON_RPC_SERVER_ERROR_KEY_EXCLUDED_FROM_SECONDARY_INDEX,
                format!(
                    "{index_key} excluded from account secondary indexes; \
                            this RPC method unavailable for key"
                ),
            ),
        RpcCustomError::TransactionHistoryNotAvailable => server_error(
            JSON_RPC_SERVER_ERROR_TRANSACTION_HISTORY_NOT_AVAILABLE,
            "Transaction history is not available from this node".to_string(),
        ),
        RpcCustomError::ScanError { message } =>
            server_error(
                JSON_RPC_SCAN_ERROR,
                message,
            ),
        RpcCustomError::TransactionSignatureLenMismatch =>
            server_error(
                JSON_RPC_SERVER_ERROR_TRANSACTION_SIGNATURE_LEN_MISMATCH,
                "Transaction signature length mismatch".to_string(),
            ),
        RpcCustomError::BlockStatusNotAvailableYet { slot } =>
            server_error(
                JSON_RPC_SERVER_ERROR_BLOCK_STATUS_NOT_AVAILABLE_YET,
                format!("Block status not yet available for slot {slot}"),
            ),
        RpcCustomError::UnsupportedTransactionVersion(version) =>
            server_error(
                JSON_RPC_SERVER_ERROR_UNSUPPORTED_TRANSACTION_VERSION,
                format!(
                    "Transaction version ({version}) is not supported by the requesting client. \
                            Please try the request again with the following configuration parameter: \
                            \"maxSupportedTransactionVersion\": {version}"
                ),
            ),
        RpcCustomError::MinContextSlotNotReached { context_slot } =>
            server_error(
                JSON_RPC_SERVER_ERROR_MIN_CONTEXT_SLOT_NOT_REACHED,
                "Minimum context slot has not been reached".to_string(),
            ),
    }
}

fn server_error(code: i64, message: String) -> jsonrpsee::types::error::ErrorObject<'static> {
    jsonrpsee::types::error::ErrorObject::owned(code as i32, message, None::<()>)
}
fn server_error_data(
    code: i64,
    message: String,
    data: serde_json::Value,
) -> jsonrpsee::types::error::ErrorObject<'static> {
    jsonrpsee::types::error::ErrorObject::owned(code as i32, message, Some(data))
}
