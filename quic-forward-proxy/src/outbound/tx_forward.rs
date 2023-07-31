use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use log::{debug, info, warn};
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use fan::tokio::mpsc::FanOut;
use std::time::Duration;
use quinn::Endpoint;
use solana_streamer::tls_certificates::new_self_signed_tls_certificate;
use tokio::sync::mpsc::{channel, Receiver};
use tokio::time::timeout;
use crate::outbound::tpu_quic_client::{send_txs_to_tpu_static, TpuQuicClient};
use crate::outbound::validator_identity::ValidatorIdentity;
use crate::quic_connection_utils::QuicConnectionUtils;
use crate::quinn_auto_reconnect::AutoReconnect;
use crate::share::ForwardPacket;

// takes transactions from upstream clients and forwards them to the TPU
pub async fn tx_forwarder(validator_identity: ValidatorIdentity, mut transaction_channel: Receiver<ForwardPacket>, exit_signal: Arc<AtomicBool>) -> anyhow::Result<()> {
    info!("TPU Quic forwarder started");

    let endpoint = new_endpoint_with_validator_identity(validator_identity).await;

    let mut agents: HashMap<SocketAddr, FanOut<ForwardPacket>> = HashMap::new();

    loop {
        // TODO add exit

        let forward_packet = transaction_channel.recv().await.expect("channel closed unexpectedly");
        let tpu_address = forward_packet.tpu_address;

        if !agents.contains_key(&tpu_address) {
            // TODO cleanup agent after a while of iactivity

            let mut senders = Vec::new();
            for _i in 0..4 {
                let (sender, mut receiver) = channel::<ForwardPacket>(100000);
                senders.push(sender);
                let exit_signal = exit_signal.clone();
                let endpoint_copy = endpoint.clone();
                tokio::spawn(async move {
                    debug!("Start Quic forwarder agent for TPU {}", tpu_address);
                    // TODO pass+check the tpu_address
                    // TODO connect
                    // TODO consume queue
                    // TODO exit signal

                    let auto_connection = AutoReconnect::new(endpoint_copy, tpu_address);
                    // let mut connection = tpu_quic_client_copy.create_connection(tpu_address).await.expect("handshake");
                    loop {

                        let _exit_signal = exit_signal.clone();
                        loop {
                            let packet = receiver.recv().await.unwrap();
                            assert_eq!(packet.tpu_address, tpu_address, "routing error");

                            let mut transactions_batch = packet.transactions;

                            let mut batch_size = 1;
                            while let Ok(more) = receiver.try_recv() {
                                transactions_batch.extend(more.transactions);
                                batch_size += 1;
                            }
                            if batch_size > 1 {
                                debug!("encountered batch of size {}", batch_size);
                            }

                            debug!("forwarding transaction batch of size {} to address {}", transactions_batch.len(), packet.tpu_address);

                            // TODo move send_txs_to_tpu_static to tpu_quic_client
                            let result = timeout(Duration::from_millis(500),
                                                 send_txs_to_tpu_static(&auto_connection, &transactions_batch)).await;
                            // .expect("timeout sending data to TPU node");

                            if result.is_err() {
                                warn!("send_txs_to_tpu_static result {:?} - loop over errors", result);
                            } else {
                                debug!("send_txs_to_tpu_static sent {}", transactions_batch.len());
                            }

                        }

                    }

                });

            }

            let fanout = FanOut::new(senders);

            agents.insert(tpu_address, fanout);

        } // -- new agent

        let agent_channel = agents.get(&tpu_address).unwrap();

        agent_channel.send(forward_packet).await.unwrap();

        // let mut batch_size = 1;
        // while let Ok(more) = transaction_channel.try_recv() {
        //     agent_channel.send(more).await.unwrap();
        //     batch_size += 1;
        // }
        // if batch_size > 1 {
        //     debug!("encountered batch of size {}", batch_size);
        // }


        // check if the tpu has already a task+queue running, if not start one, sort+queue packets by tpu address
        // maintain the health of a TPU connection, debounce errors; if failing, drop the respective messages

        // let exit_signal_copy = exit_signal.clone();
        // debug!("send transaction batch of size {} to address {}", forward_packet.transactions.len(), forward_packet.tpu_address);
        // // TODO: this will block/timeout if the TPU is not available
        // timeout(Duration::from_millis(500),
        //         tpu_quic_client_copy.send_txs_to_tpu(tpu_address, &forward_packet.transactions, exit_signal_copy)).await;
        // tpu_quic_client_copy.send_txs_to_tpu(forward_packet.tpu_address, &forward_packet.transactions, exit_signal_copy).await;

    } // -- loop over transactions from ustream channels

    // not reachable
}

/// takes a validator identity and creates a new QUIC client; appears as staked peer to TPU
// note: ATM the provided identity might or might not be a valid validator keypair
async fn new_endpoint_with_validator_identity(validator_identity: ValidatorIdentity) -> Endpoint {
    info!("Setup TPU Quic stable connection with validator identity {} ...", validator_identity);
    let (certificate, key) = new_self_signed_tls_certificate(
        &validator_identity.get_keypair_for_tls(),
        IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
    )
        .expect("Failed to initialize QUIC connection certificates");

    let endpoint_outbound = QuicConnectionUtils::create_tpu_client_endpoint(certificate.clone(), key.clone());

    endpoint_outbound
}