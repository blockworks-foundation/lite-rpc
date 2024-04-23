use std::time::Duration;

use solana_sdk::clock::Slot;
use yellowstone_grpc_proto::{geyser::SubscribeUpdate, tonic::transport::ClientTlsConfig};

#[derive(Clone, Debug)]
pub struct GrpcConnectionTimeouts {
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub subscribe_timeout: Duration,
    pub receive_timeout: Duration,
}

#[derive(Clone)]
pub struct GrpcSourceConfig {
    pub grpc_addr: String,
    pub grpc_x_token: Option<String>,
    _tls_config: Option<ClientTlsConfig>,
    _timeouts: Option<GrpcConnectionTimeouts>,
}

pub trait FromYellowstoneExtractor {
    // Target is something like ProducedBlock
    type Target;
    fn map_yellowstone_update(&self, update: SubscribeUpdate) -> Option<(Slot, Self::Target)>;
}
