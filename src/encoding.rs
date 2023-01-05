use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum BinaryEncoding {
    #[default]
    Base58,
    Base64,
}

#[derive(thiserror::Error, Debug)]
pub enum BinaryCodecError {
    #[error("Base58DecodeError {0}")]
    Base58DecodeError(#[from] bs58::decode::Error),
    #[error("Base58EncodeError {0}")]
    Base58EncodeError(#[from] bs58::encode::Error),
    #[error("Base64DecodeError {0}")]
    Base64DecodeError(#[from] base64::DecodeError),
}

impl BinaryEncoding {
    pub fn decode<D: AsRef<[u8]>>(&self, to_decode: D) -> Result<Vec<u8>, BinaryCodecError> {
        match self {
            Self::Base58 => Ok(bs58::decode(to_decode).into_vec()?),
            Self::Base64 => Ok(base64::decode(to_decode)?),
        }
    }

    pub fn encode<E: AsRef<[u8]>>(&self, to_encode: E) -> String {
        match self {
            Self::Base58 => bs58::encode(to_encode).into_string(),
            Self::Base64 => base64::encode(to_encode),
        }
    }
}
