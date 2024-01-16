use crate::cli::RpcArgs;

use super::Strategy;

///- send txs on LiteRPC broadcast channel and consume them using the Solana quic-streamer
/// - see quic_proxy_tpu_integrationtest.rs (note: not only about proxy)
/// - run cargo test (maybe need to use release build)
/// - Goal: measure performance of LiteRPC internal channel/thread structure and the TPU_service performance
#[derive(clap::Args, Debug)]
pub struct Tc4 {
    #[command(flatten)]
    common: RpcArgs,
}

#[async_trait::async_trait]
impl Strategy for Tc4 {
    async fn execute(&self) -> anyhow::Result<Vec<serde_json::Value>> {
        todo!()
    }
}
