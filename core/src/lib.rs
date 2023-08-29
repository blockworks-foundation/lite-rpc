use std::sync::{atomic::AtomicU64, Arc};

pub mod block_information_store;
pub mod cluster_info;
pub mod data_cache;
pub mod leader_schedule;
pub mod notifications;
pub mod quic_connection;
pub mod quic_connection_utils;
pub mod rotating_queue;
pub mod solana_utils;
pub mod structures;
pub mod subscription_sink;
pub mod subscription_store;
pub mod traits;
pub mod tx_store;

pub type WireTx = Vec<u8>;

pub type AnyhowJoinHandle = tokio::task::JoinHandle<anyhow::Result<()>>;
pub type AtomicSlot = Arc<AtomicU64>;
