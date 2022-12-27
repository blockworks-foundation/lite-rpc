use crate::{configs::SendTransactionConfig, encoding::BinaryCodecError};

use actix_web::error::JsonPayloadError;
use actix_web::{http::StatusCode, HttpResponse, Responder};
use serde::{Deserialize, Serialize};
use serde_json::json;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::signature::ParseSignatureError;
use solana_sdk::transport::TransportError;

#[derive(Debug, Serialize, Deserialize)]
pub struct SendTransactionParams(pub String, #[serde(default)] pub SendTransactionConfig);

#[derive(Debug, Serialize, Deserialize)]
pub struct ConfirmTransactionParams(pub String, #[serde(default)] pub CommitmentConfig);

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum RpcMethod {
    SendTransaction,
    ConfirmTransaction,
    GetVersion,
    #[serde(other)]
    Other,
}

/// According to <https://www.jsonrpc.org/specification#overview>
#[derive(Debug, Deserialize, Serialize)]
pub struct JsonRpcReq {
    pub method: RpcMethod,
    #[serde(default)]
    pub params: serde_json::Value,
}

#[derive(Debug, Deserialize, Serialize)]
pub enum JsonRpcRes {
    Raw { status: u16, body: String },
    Err(serde_json::Value),
    Ok(serde_json::Value),
}

impl Responder for JsonRpcRes {
    type Body = String;

    fn respond_to(self, _: &actix_web::HttpRequest) -> HttpResponse<Self::Body> {
        if let Self::Raw { status, body } = self {
            return HttpResponse::new(StatusCode::from_u16(status).unwrap()).set_body(body);
        }
        let mut res = json!({
            "jsonrpc" : "2.0",
            // TODO: add id
        });

        match self {
            Self::Err(error) => {
                res["error"] = error;
                HttpResponse::new(StatusCode::from_u16(500).unwrap()).set_body(res.to_string())
            }
            Self::Ok(result) => {
                res["result"] = result;
                HttpResponse::new(StatusCode::OK).set_body(res.to_string())
            }
            _ => unreachable!(),
        }
    }
}

impl<T: serde::Serialize> TryFrom<Result<T, JsonRpcError>> for JsonRpcRes {
    type Error = serde_json::Error;

    fn try_from(result: Result<T, JsonRpcError>) -> Result<Self, Self::Error> {
        Ok(match result {
            Ok(value) => Self::Ok(serde_json::to_value(value)?),
            // TODO: add custom handle
            Err(error) => error.into(),
        })
    }
}

impl From<JsonRpcError> for JsonRpcRes {
    fn from(error: JsonRpcError) -> Self {
        Self::Err(serde_json::Value::String(format!("{error:?}")))
    }
}

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
    #[error("JsonPayloadError {0}")]
    JsonPayloadError(#[from] JsonPayloadError),
    #[error("ParseSignatureError {0}")]
    ParseSignatureError(#[from] ParseSignatureError),
}
