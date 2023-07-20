use std::sync::{atomic::AtomicU64, Arc};

pub mod block_processor;
pub mod block_store;
pub mod leader_schedule;
pub mod notifications;
pub mod quic_connection;
pub mod quic_connection_utils;
pub mod rotating_queue;
pub mod solana_utils;
pub mod structures;
pub mod subscription_handler;
pub mod subscription_sink;
pub mod tx_store;
pub mod grpc_client;

pub type AnyhowJoinHandle = tokio::task::JoinHandle<anyhow::Result<()>>;
pub type AtomicSlot = Arc<AtomicU64>;
