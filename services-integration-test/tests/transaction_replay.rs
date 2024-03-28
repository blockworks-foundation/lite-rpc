// use anyhow::bail;
// use bench::{create_memo_tx, create_rng, BenchmarkTransactionParams};
// use log::{debug, info, trace, warn};
// use solana_lite_rpc_core::{
//     solana_utils::SerializableTransaction,
//     structures::transaction_sent_info::SentTransactionInfo,
//     types::{BlockStream, SlotStream},
//     AnyhowJoinHandle,
// };
// use solana_rpc_client::nonblocking::rpc_client::RpcClient;
// use solana_sdk::{
//     commitment_config::CommitmentConfig, hash::hash, nonce::state::Data, signature::Keypair,
//     signer::Signer, transaction::VersionedTransaction,
// };

// use solana_lite_rpc_services::tpu_utils::tpu_service::{TpuService, TpuServiceConfig};
// use solana_lite_rpc_services::{
//     quic_connection_utils::QuicConnectionParameters, tx_sender::TxSender,
// };
// use solana_lite_rpc_services::{
//     tpu_utils::tpu_connection_path::TpuConnectionPath, transaction_replayer::TransactionReplay,
// };

// use std::net::{SocketAddr, ToSocketAddrs};
// use std::time::Duration;
// use tokio::time::{timeout, Instant};
// use tokio::{sync::RwLock, task::JoinHandle};

// use crate::setup::setup_services;

// mod setup;

// #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
// /// - Goal: measure replay_service performance
// pub async fn transaction_replay() -> anyhow::Result<()> {
//     tracing_subscriber::fmt::init();
//     info!("START BENCHMARK: tpu_service");

//     let (_tx_sender, replay_service, tpu_service, data_cache, slot_stream) =
//         setup_services().await?;
//     let (replay_sender, mut replay_receiver) = tokio::sync::mpsc::unbounded_channel();
//     let replay_channel = replay_sender.clone();
//     let retry_offset = replay_service.retry_offset;

//     // let r2 = replay_receiver.re();

//     let mut rng = create_rng(None);
//     let payer = Keypair::new();
//     let mut blockhash = hash(&[1, 2, 3]);

//     let prio_fee = 1u64;
//     let params = BenchmarkTransactionParams {
//         tx_size: bench::tx_size::TxSize::Small,
//         cu_price_micro_lamports: prio_fee,
//     };

//     let tpu_jh = tpu_service.start(slot_stream.resubscribe());
//     let replay_jh = replay_service.start_service(replay_sender, replay_receiver);

//     // let replay_jh = tokio::spawn( async move {

//     //     while let Some(mut tx_replay) = replay_receiver.recv().await {
//     //         info!("received");
//     //     }

//     // });
//     info!("replayer spawned");

//     tokio::spawn(async move {
//         let mut i = 0;

//         loop {
//             while i < 10000 {
//                 blockhash = hash(&blockhash.as_ref());
//                 let tx = create_memo_tx(&payer, blockhash, &mut rng, &params);

//                 let sent_tx = SentTransactionInfo {
//                     signature: *tx.get_signature(),
//                     slot: data_cache.slot_cache.get_current_slot(),
//                     transaction: bincode::serialize::<VersionedTransaction>(&tx)
//                         .expect("Could not serialize VersionedTransaction"),
//                     last_valid_block_height: data_cache
//                         .block_information_store
//                         .get_last_blockheight(),
//                     prioritization_fee: prio_fee,
//                 };

//                 let replay_at = Instant::now() + retry_offset;

//                 let r = TransactionReplay {
//                     transaction: sent_tx,
//                     replay_count: 1,
//                     max_replay: 1,
//                     replay_at,
//                 };

//                 trace!("Sending replay: {:?}", i);
//                 replay_channel.send(r).unwrap();
//                 trace!("replay send success");
//                 i += 1
//             }
//             tokio::time::sleep(Duration::from_millis(500)).await;
//         }
//     });

//     let handle: AnyhowJoinHandle = tokio::spawn(async move {
//         tokio::select! {
//             res = tpu_jh => {
//                 bail!("Tpu Service {res:?}")
//             },
//             res = replay_jh => {
//                 bail!("Replay Service {res:?}")
//             },
//             // res = send_jh => {
//             //     bail!("Replay Send task {res:?}")
//             // }
//         }
//     });

//     handle.await?;

//     Ok(())
// }
