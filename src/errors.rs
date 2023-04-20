use crate::encoding::BinaryCodecError;
use jsonrpsee::core::Error;
use jsonrpsee::types::error::CallError;
use jsonrpsee::types::ErrorObjectOwned;
use jsonrpsee::IntoResponse;
use solana_sdk::{signature::ParseSignatureError, transport::TransportError};

pub type RpcCustomResult<T> = Result<T, RpcCustomError>;

#[derive(thiserror::Error, Debug)]
pub enum RpcCustomError {
    #[error("RpcError {0}")]
    RpcError(#[from] Error),
    #[error("Custom {0}")]
    Custom(String),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl From<RpcCustomError> for Error {
    fn from(err: RpcCustomError) -> Self {
        match err {
            RpcCustomError::RpcError(err) => err,
            err => Error::Custom(err.to_string()),
        }
    }
}

impl From<RpcCustomError> for ErrorObjectOwned {
    fn from(value: RpcCustomError) -> Self {
        todo!()
    }
}

impl From<RpcCustomError> for CallError {
    fn from(value: RpcCustomError) -> Self {
        todo!()
    }
}
