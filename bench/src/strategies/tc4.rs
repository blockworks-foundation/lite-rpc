use std::fs::File;
use csv::Writer;
use crate::cli::RpcArgs;
use crate::strategies::tc1::{Tc1, Tc1Result};

use super::Strategy;

#[derive(Debug, serde::Serialize)]
pub struct Tc4Result {
    // TODO
}

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
    type Output = Tc4Result;

    async fn execute(&self) -> anyhow::Result<Self::Output> {
        todo!()
    }
}

impl Tc4 {
    pub fn write_csv(csv_writer: &mut Writer<File>, result: &Tc4Result) -> anyhow::Result<()> {
        // TODO
        Ok(())
    }
}
