use jsonrpsee::types::error::{CALL_EXECUTION_FAILED_CODE, ErrorCode};
use jsonrpsee::types::ErrorObject;
use log::warn;
use solana_rpc_client_api::custom_error::*;

/// adoption of solana-rpc-client-api custom_errors.rs for jsonrpsee

fn server_error(code: i64, message: String) -> jsonrpsee::types::error::ErrorObject<'static> {
    jsonrpsee::types::error::ErrorObject::owned(
        code as i32,
        message,
        None::<()>)
}

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
        // RpcCustomError::NodeUnhealthy { num_slots_behind } => Self {
        //     code: ErrorCode::ServerError(JSON_RPC_SERVER_ERROR_NODE_UNHEALTHY),
        //     message: if let Some(num_slots_behind) = num_slots_behind {
        //         format!("Node is behind by {num_slots_behind} slots")
        //     } else {
        //         "Node is unhealthy".to_string()
        //     },
        //     data: Some(serde_json::json!(NodeUnhealthyErrorData {
        //                 num_slots_behind
        //             })),
        // },
        // RpcCustomError::TransactionPrecompileVerificationFailure(e) => Self {
        //     code: ErrorCode::ServerError(
        //         JSON_RPC_SERVER_ERROR_TRANSACTION_PRECOMPILE_VERIFICATION_FAILURE,
        //     ),
        //     message: format!("Transaction precompile verification failure {e:?}"),
        //     data: None,
        // },
        // RpcCustomError::SlotSkipped { slot } => Self {
        //     code: ErrorCode::ServerError(JSON_RPC_SERVER_ERROR_SLOT_SKIPPED),
        //     message: format!(
        //         "Slot {slot} was skipped, or missing due to ledger jump to recent snapshot"
        //     ),
        //     data: None,
        // },
        // RpcCustomError::NoSnapshot => Self {
        //     code: ErrorCode::ServerError(JSON_RPC_SERVER_ERROR_NO_SNAPSHOT),
        //     message: "No snapshot".to_string(),
        //     data: None,
        // },
        // RpcCustomError::LongTermStorageSlotSkipped { slot } => Self {
        //     code: ErrorCode::ServerError(JSON_RPC_SERVER_ERROR_LONG_TERM_STORAGE_SLOT_SKIPPED),
        //     message: format!("Slot {slot} was skipped, or missing in long-term storage"),
        //     data: None,
        // },
        // RpcCustomError::KeyExcludedFromSecondaryIndex { index_key } => Self {
        //     code: ErrorCode::ServerError(
        //         JSON_RPC_SERVER_ERROR_KEY_EXCLUDED_FROM_SECONDARY_INDEX,
        //     ),
        //     message: format!(
        //         "{index_key} excluded from account secondary indexes; \
        //                 this RPC method unavailable for key"
        //     ),
        //     data: None,
        // },
        // RpcCustomError::TransactionHistoryNotAvailable => Self {
        //     code: ErrorCode::ServerError(
        //         JSON_RPC_SERVER_ERROR_TRANSACTION_HISTORY_NOT_AVAILABLE,
        //     ),
        //     message: "Transaction history is not available from this node".to_string(),
        //     data: None,
        // },
        // RpcCustomError::ScanError { message } => Self {
        //     code: ErrorCode::ServerError(JSON_RPC_SCAN_ERROR),
        //     message,
        //     data: None,
        // },
        // RpcCustomError::TransactionSignatureLenMismatch => Self {
        //     code: ErrorCode::ServerError(
        //         JSON_RPC_SERVER_ERROR_TRANSACTION_SIGNATURE_LEN_MISMATCH,
        //     ),
        //     message: "Transaction signature length mismatch".to_string(),
        //     data: None,
        // },
        // RpcCustomError::BlockStatusNotAvailableYet { slot } => Self {
        //     code: ErrorCode::ServerError(JSON_RPC_SERVER_ERROR_BLOCK_STATUS_NOT_AVAILABLE_YET),
        //     message: format!("Block status not yet available for slot {slot}"),
        //     data: None,
        // },
        // RpcCustomError::UnsupportedTransactionVersion(version) => Self {
        //     code: ErrorCode::ServerError(JSON_RPC_SERVER_ERROR_UNSUPPORTED_TRANSACTION_VERSION),
        //     message: format!(
        //         "Transaction version ({version}) is not supported by the requesting client. \
        //                 Please try the request again with the following configuration parameter: \
        //                 \"maxSupportedTransactionVersion\": {version}"
        //     ),
        //     data: None,
        // },
        // RpcCustomError::MinContextSlotNotReached { context_slot } => Self {
        //     code: ErrorCode::ServerError(JSON_RPC_SERVER_ERROR_MIN_CONTEXT_SLOT_NOT_REACHED),
        //     message: "Minimum context slot has not been reached".to_string(),
        //     data: Some(serde_json::json!(MinContextSlotNotReachedErrorData {
        //                 context_slot,
        //             })),
        // },
        _ => {
            warn!("UNMAPPED ERROR"); // TODO
            server_error(
                "TODO Unknown error".to_string(),
                -121212
            )
        }
    }
}





