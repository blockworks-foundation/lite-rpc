use std::time::Duration;
use solana_lite_rpc_core::quic_connection_utils::QuicConnectionParameters;
use crate::DEFAULT_FANOUT_SIZE;

#[derive(Clone, Copy)]
pub struct TpuServiceConfig {
    pub fanout_slots: u64,
    pub number_of_leaders_to_cache: usize,
    pub clusterinfo_refresh_time: Duration,
    pub leader_schedule_update_frequency: Duration,
    pub maximum_transaction_in_queue: usize,
    pub maximum_number_of_errors: usize,
    pub quic_connection_params: QuicConnectionParameters,
}

impl Default for TpuServiceConfig {
    fn default() -> Self {
        Self {
            fanout_slots: DEFAULT_FANOUT_SIZE,
            number_of_leaders_to_cache: 1024,
            clusterinfo_refresh_time: Duration::from_secs(60 * 60),
            leader_schedule_update_frequency: Duration::from_secs(10),
            maximum_transaction_in_queue: 20000,
            maximum_number_of_errors: 10,
            quic_connection_params: QuicConnectionParameters {
                connection_timeout: Duration::from_secs(1),
                connection_retry_count: 10,
                finalize_timeout: Duration::from_millis(200),
                max_number_of_connections: 10,
                unistream_timeout: Duration::from_millis(500),
                write_timeout: Duration::from_secs(1),
                number_of_transactions_per_unistream: 8,
            },
        }
    }
}