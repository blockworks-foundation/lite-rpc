use solana_lite_rpc_core::encoding::BinaryCodecError;
use solana_sdk::{signature::ParseSignatureError, transport::TransportError};

#[derive(thiserror::Error, Debug)]
pub enum JsonRpcError {
    #[error("TransportError {0}")]
    TransportError(#[from] TransportError),
    #[error("BinaryCodecError {0}")]
    BinaryCodecError(#[from] BinaryCodecError),
    #[error("BincodeDeserializeError {0}")]
    BincodeDeserializeError(#[from] bincode::Error),
    #[error("SerdeError {0}")]
    SerdeError(#[from] serde_json::Error),
    #[error("ParseSignatureError {0}")]
    ParseSignatureError(#[from] ParseSignatureError),
}
