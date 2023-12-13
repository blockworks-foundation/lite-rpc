use async_stream::stream;
use futures::{Stream, StreamExt};
use itertools::Itertools;
use log::{info, warn};
use solana_lite_rpc_cluster_endpoints::grpc_subscription::map_produced_block;
use solana_lite_rpc_core::structures::produced_block::ProducedBlock;
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::CommitmentConfig;
use std::collections::HashMap;
use std::pin::pin;
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};
use yellowstone_grpc_client::{GeyserGrpcClient, GeyserGrpcClientResult};
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::{
    CommitmentLevel, SubscribeRequestFilterBlocks, SubscribeUpdate,
};
use yellowstone_grpc_proto::tonic::transport::ClientTlsConfig;
use yellowstone_grpc_proto::tonic::Status;

#[tokio::main]
pub async fn main() {
    // RUST_LOG=info,grpc_using_streams=debug
    tracing_subscriber::fmt::init();
    // console_subscriber::init();

    // mango validator (mainnet)
    let grpc_addr_mainnet_triton = "http://202.8.9.108:10000".to_string();
    // via toxiproxy
    let grpc_addr_mainnet_triton_toxi = "http://127.0.0.1:10001".to_string();
    // ams81 (mainnet)
    let grpc_addr_mainnet_ams81 = "http://202.8.8.12:10000".to_string();
    // testnet - NOTE: this connection has terrible lags (almost 5 minutes)
    // let grpc_addr = "http://147.28.169.13:10000".to_string();

    let green_config = GrpcSourceConfig::new("triton".to_string(), grpc_addr_mainnet_triton, None);
    let blue_config =
        GrpcSourceConfig::new("mangoams81".to_string(), grpc_addr_mainnet_ams81, None);
    let toxiproxy_config =
        GrpcSourceConfig::new("toxiproxy".to_string(), grpc_addr_mainnet_triton_toxi, None);

    let block_stream = create_multiplex(
        // vec![green_config, blue_config, toxiproxy_config],
        vec![blue_config],
        CommitmentLevel::Confirmed,
    );

    tokio::spawn(async move {
        let mut block_stream = pin!(block_stream);

        while let Some(Some(block)) = block_stream.next().await {
            info!(
                "received block #{} with {} txs",
                block.slot,
                block.transactions.len()
            );
        }
    });

    // "infinite" sleep
    sleep(Duration::from_secs(1800)).await;
}

fn create_multiplex(
    grpc_sources: Vec<GrpcSourceConfig>,
    commitment_level: CommitmentLevel,
) -> impl Stream<Item = Option<ProducedBlock>> {
    assert!(
        commitment_level == CommitmentLevel::Confirmed
            || commitment_level == CommitmentLevel::Finalized,
        "Only CONFIRMED and FINALIZED is supported"
    );
    // note: PROCESSED blocks are not sequential in presense of forks; this will break the logic

    if grpc_sources.len() < 1 {
        panic!("Must have at least one source");
    }

    let commitment_config = match commitment_level {
        CommitmentLevel::Confirmed => CommitmentConfig::confirmed(),
        CommitmentLevel::Finalized => CommitmentConfig::finalized(),
        // not used, not supported!
        CommitmentLevel::Processed => CommitmentConfig::processed(),
    };
    info!(
        "Starting multiplexer with {} sources: {}",
        grpc_sources.len(),
        grpc_sources
            .iter()
            .map(|source| source.label.clone())
            .join(", ")
    );

    let mut current_slot: Slot = 0;

    let build_block_process_stream = move |grpc_source: GrpcSourceConfig| {
        //connect to Geyzer stream
        create_geyser_reconnecting_stream(grpc_source.clone(), commitment_level)
            //filter with block with most recent slot
            .filter_map(move |block| async move {
                block.map(|update_message| match update_message.update_oneof {
                    Some(UpdateOneof::Block(update_block_message))
                        if update_block_message.slot > current_slot =>
                    {
                        Some(map_produced_block(update_block_message, commitment_config))
                    }
                    _ => None,
                })
            })
    };

    let mut futures = futures::stream::SelectAll::new();
    for grpc_source in grpc_sources {
        futures.push(Box::pin(build_block_process_stream(grpc_source)));
    }

    //update the current slot with the most recent block that get from the stream.
    futures.then(move |block| async move {
        if let Some(ref block) = block {
            *(&mut current_slot) = block.slot;
        }
        block
    })
}

#[derive(Clone, Debug)]
struct GrpcSourceConfig {
    // symbolic name used in logs
    label: String,
    grpc_addr: String,
    grpc_x_token: Option<String>,
    tls_config: Option<ClientTlsConfig>,
}

impl GrpcSourceConfig {
    fn new(label: String, grpc_addr: String, grpc_x_token: Option<String>) -> Self {
        Self {
            label,
            grpc_addr,
            grpc_x_token,
            tls_config: None,
        }
    }
}

enum ConnectionState<S: Stream<Item = Result<SubscribeUpdate, Status>>> {
    NotConnected,
    Connecting(JoinHandle<GeyserGrpcClientResult<S>>),
    Ready(S),
    WaitReconnect,
}

// TODO use GrpcSource
// note: stream never terminates
fn create_geyser_reconnecting_stream(
    grpc_source: GrpcSourceConfig,
    commitment_level: CommitmentLevel,
) -> impl Stream<Item = Option<SubscribeUpdate>> {
    let label = grpc_source.label.clone();

    // NOT_CONNECTED; CONNECTING
    let mut state = ConnectionState::NotConnected;

    // in case of cancellation, we restart from here:
    // thus we want to keep the progression in a state object outside the stream! makro
    stream! {
        loop{
            let yield_value;
            (state, yield_value) = match state {
                ConnectionState::NotConnected => {

                    let connection_task = tokio::spawn({
                        let addr = grpc_source.grpc_addr.clone();
                        let token = grpc_source.grpc_x_token.clone();
                        let config = grpc_source.tls_config.clone();
                        async move {
                              let connect_result = GeyserGrpcClient::connect_with_timeout(
                                addr, token, config,
                                Some(Duration::from_secs(2)), Some(Duration::from_secs(2)), false).await;


                            let mut client = connect_result?;

                            // Connected;

                            let mut blocks_subs = HashMap::new();
                            blocks_subs.insert(
                                "client".to_string(),
                                SubscribeRequestFilterBlocks {
                                    account_include: Default::default(),
                                    include_transactions: Some(true),
                                    include_accounts: Some(false),
                                    include_entries: Some(false),
                                },
                            );

                            let subscribe_result = client
                                .subscribe_once(
                                    HashMap::new(),
                                    Default::default(),
                                    HashMap::new(),
                                    Default::default(),
                                    blocks_subs,
                                    Default::default(),
                                    Some(commitment_level),
                                    Default::default(),
                                    None,
                                ).await;

                            subscribe_result
                        }
                    });

                    (ConnectionState::Connecting(connection_task), None)
                }
                ConnectionState::Connecting(connection_task) => {
                    let subscribe_result = connection_task.await;

                     match subscribe_result {
                        Ok(Ok(subscribed_stream)) => (ConnectionState::Ready(subscribed_stream), None),
                        Ok(Err(geyser_error)) => {
                             // TODO identify non-recoverable errors and cancel stream
                            warn!("Subscribe failed on {} - retrying: {:?}", label, geyser_error);
                            (ConnectionState::WaitReconnect, None)
                        },
                        Err(geyser_grpc_task_error) => {
                            panic!("Task aborted - should not happen :{geyser_grpc_task_error}");
                        }
                    }

                }
                ConnectionState::Ready(mut geyser_stream) => {

                    //for await update_message in geyser_stream {
                        match geyser_stream.next().await {
                            Some(Ok(update_message)) => {
                                info!(">message on {}", label);
                                (ConnectionState::Ready(geyser_stream), Some(update_message))
                            }
                            Some(Err(tonic_status)) => {
                                // TODO identify non-recoverable errors and cancel stream
                                warn!("Receive error on {} - retrying: {:?}", label, tonic_status);
                                (ConnectionState::WaitReconnect, None)
                            }
                            None =>  {
                                //TODO should not arrive. Mean the stream close.
                                warn!("Geyzer stream close on {} - retrying", label);
                                (ConnectionState::WaitReconnect, None)
                            }
                        }
                    //} // -- production loop

                }
                ConnectionState::WaitReconnect => {
                    // TODO implement backoff
                    sleep(Duration::from_secs(1)).await;
                    (ConnectionState::NotConnected, None)
                }
            }; // -- match
            yield yield_value
        }

    } // -- stream!
}
