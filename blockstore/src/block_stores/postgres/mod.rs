pub mod postgres_block_store_importer;
pub mod postgres_block_store_query;
pub mod postgres_block_store_writer;
pub mod measure_database_roundtrip;

use anyhow::Context;
pub use postgres_config::BlockstorePostgresSessionConfig;
use serde::de::DeserializeOwned;
use serde::{Serialize};
use serde_json::Value;
// pub use postgres_session::BlockstorePostgresWriteSession;
pub use postgres_session::PostgresSession;

mod postgres_block;
mod postgres_config;
mod postgres_epoch;
mod postgres_session;
mod postgres_transaction;

// role for block store componente owner with full write access
pub const LITERPC_ROLE: &str = "r_literpc";
// role for accessing data
pub const LITERPC_QUERY_ROLE: &str = "ro_literpc";

pub fn json_serialize<T: Serialize>(data: &T) -> Value {
    serde_json::to_value(data)
        .context("serializing data to json for persistence")
        .unwrap()
}

pub fn json_deserialize<T: DeserializeOwned>(json: Value) -> T {
    serde_json::from_value::<T>(json)
        .context("deserializing data from json for persistence")
        .unwrap()
}
