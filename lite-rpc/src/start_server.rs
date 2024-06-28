use crate::{
    bridge::LiteBridge, bridge_pubsub::LitePubSubBridge, rpc::LiteRpcServer,
    rpc_pubsub::LiteRpcPubSubServer,
};

use hyper::Method;
use jsonrpsee::server::ServerBuilder;
use solana_lite_rpc_core::AnyhowJoinHandle;
use std::time::Duration;
use tower_http::cors::{Any, CorsLayer};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ServerConfiguration {
    pub max_request_body_size: u32,

    pub max_response_body_size: u32,

    pub max_connection: u32,

    pub max_subscriptions_per_connection: u32,
}

impl Default for ServerConfiguration {
    fn default() -> Self {
        Self {
            max_request_body_size: 50 * (1 << 10),       // 50kb
            max_response_body_size: 500_000 * (1 << 10), // 500MB response size
            max_connection: 1000000,
            max_subscriptions_per_connection: 1000,
        }
    }
}

pub async fn start_servers(
    rpc: LiteBridge,
    pubsub: LitePubSubBridge,
    ws_addr: String,
    http_addr: String,
    server_configuration: Option<ServerConfiguration>,
) -> anyhow::Result<()> {
    let rpc = rpc.into_rpc();
    let pubsub = pubsub.into_rpc();
    let server_configuration = server_configuration.unwrap_or_default();

    let ws_server_handle = ServerBuilder::default()
        .ws_only()
        .max_connections(server_configuration.max_connection)
        .max_subscriptions_per_connection(server_configuration.max_subscriptions_per_connection)
        .build(ws_addr.clone())
        .await?
        .start(pubsub);

    let cors = CorsLayer::new()
        .max_age(Duration::from_secs(86400))
        // Allow `POST` when accessing the resource
        .allow_methods([Method::POST, Method::GET, Method::OPTIONS])
        // Allow requests from any origin
        .allow_origin(Any)
        .allow_headers(Any);

    let middleware = tower::ServiceBuilder::new().layer(cors);

    let http_server_handle = ServerBuilder::default()
        .set_middleware(middleware)
        .max_connections(server_configuration.max_connection)
        .max_request_body_size(server_configuration.max_response_body_size)
        .max_response_body_size(server_configuration.max_response_body_size)
        .http_only()
        .build(http_addr.clone())
        .await?
        .start(rpc);

    let ws_server: AnyhowJoinHandle = tokio::spawn(async move {
        log::info!("Websocket Server started at {ws_addr:?}");
        ws_server_handle.stopped().await;
        anyhow::bail!("Websocket server stopped");
    });

    let http_server: AnyhowJoinHandle = tokio::spawn(async move {
        log::info!("HTTP Server started at {http_addr:?}");
        http_server_handle.stopped().await;
        anyhow::bail!("HTTP server stopped");
    });

    tokio::select! {
        res = ws_server => {
            anyhow::bail!("WebSocket server {res:?}");
        },
        res = http_server => {
            anyhow::bail!("HTTP server {res:?}");
        },
    }
}
