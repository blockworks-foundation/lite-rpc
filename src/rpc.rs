use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use solana_client::{
    pubsub_client::{BlockSubscription, PubsubClientError},
    tpu_client::TpuClientConfig,
};
use solana_pubsub_client::pubsub_client::{PubsubBlockClientSubscription, PubsubClient};
use solana_sdk::transaction::Transaction;
use std::{
    str::FromStr,
    sync::Mutex,
    thread::{Builder, JoinHandle},
    time::{Duration, Instant},
};

use crate::context::{
    BlockInformation, LiteRpcContext, NotificationType, PerformanceCounter, SignatureNotification,
    SignatureStatus, SlotNotification,
};
use crossbeam_channel::Sender;
use {
    bincode::config::Options,
    crossbeam_channel::Receiver,
    jsonrpc_core::{Error, Metadata, Result},
    jsonrpc_derive::rpc,
    solana_client::connection_cache::ConnectionCache,
    solana_client::rpc_client::RpcClient,
    solana_client::tpu_client::TpuClient,
    solana_perf::packet::PACKET_DATA_SIZE,
    solana_rpc_client_api::{
        config::*,
        response::{Response as RpcResponse, *},
    },
    solana_sdk::{
        commitment_config::{CommitmentConfig, CommitmentLevel},
        signature::Signature,
    },
    solana_transaction_status::{TransactionBinaryEncoding, UiTransactionEncoding},
    std::{
        any::type_name,
        sync::{atomic::Ordering, Arc},
    },
};

const TPU_BATCH_SIZE: usize = 8;

#[derive(Clone)]
pub struct LightRpcRequestProcessor {
    pub rpc_client: Arc<RpcClient>,
    pub last_valid_block_height: u64,
    pub ws_url: String,
    pub context: Arc<LiteRpcContext>,
    _connection_cache: Arc<ConnectionCache>,
    joinables: Arc<Mutex<Vec<JoinHandle<()>>>>,
    subscribed_clients: Arc<Mutex<Vec<PubsubBlockClientSubscription>>>,
    performance_counter: PerformanceCounter,
    tpu_producer_channel: Sender<Transaction>,
}

impl LightRpcRequestProcessor {
    pub fn new(
        json_rpc_url: &str,
        websocket_url: &str,
        notification_sender: Sender<NotificationType>,
        performance_counter: PerformanceCounter,
    ) -> LightRpcRequestProcessor {
        let rpc_client = Arc::new(RpcClient::new(json_rpc_url));
        let context = Arc::new(LiteRpcContext::new(rpc_client.clone(), notification_sender));

        let connection_cache = Arc::new(ConnectionCache::default());
        println!("ws_url {}", websocket_url);
        // subscribe for confirmed_blocks
        let (client_confirmed, receiver_confirmed) =
            Self::subscribe_block(websocket_url, CommitmentLevel::Confirmed).unwrap();

        // subscribe for finalized blocks
        let (client_finalized, receiver_finalized) =
            Self::subscribe_block(websocket_url, CommitmentLevel::Finalized).unwrap();

        let (tpu_producer, tpu_consumer) = crossbeam_channel::bounded(100000);

        // create threads to listen for finalized and confrimed blocks
        let joinables = vec![
            Self::build_thread_to_process_blocks(
                receiver_confirmed,
                &context,
                CommitmentLevel::Confirmed,
                performance_counter.clone(),
            ),
            Self::build_thread_to_process_blocks(
                receiver_finalized,
                &context,
                CommitmentLevel::Finalized,
                performance_counter.clone(),
            ),
            Self::build_thread_to_process_transactions(
                json_rpc_url.to_string(),
                websocket_url.to_string(),
                &context,
                tpu_consumer.clone(),
                performance_counter.clone(),
            ),
        ];

        LightRpcRequestProcessor {
            rpc_client,
            last_valid_block_height: 0,
            ws_url: websocket_url.to_string(),
            context,
            _connection_cache: connection_cache,
            joinables: Arc::new(Mutex::new(joinables)),
            subscribed_clients: Arc::new(Mutex::new(vec![client_confirmed, client_finalized])),
            performance_counter,
            tpu_producer_channel: tpu_producer,
        }
    }

    fn subscribe_block(
        websocket_url: &str,
        commitment: CommitmentLevel,
    ) -> std::result::Result<BlockSubscription, PubsubClientError> {
        PubsubClient::block_subscribe(
            websocket_url,
            RpcBlockSubscribeFilter::All,
            Some(RpcBlockSubscribeConfig {
                commitment: Some(CommitmentConfig { commitment }),
                encoding: None,
                transaction_details: Some(solana_transaction_status::TransactionDetails::Full),
                show_rewards: None,
                max_supported_transaction_version: None,
            }),
        )
    }

    fn build_thread_to_process_blocks(
        reciever: Receiver<RpcResponse<RpcBlockUpdate>>,
        context: &Arc<LiteRpcContext>,
        commitment: CommitmentLevel,
        performance_counters: PerformanceCounter,
    ) -> JoinHandle<()> {
        let context = context.clone();
        Builder::new()
            .name("thread working on confirmation block".to_string())
            .spawn(move || {
                let block_info = if commitment.eq(&CommitmentLevel::Finalized) {
                    &context.confirmed_block_info
                } else {
                    &context.finalized_block_info
                };
                Self::process_block(
                    reciever,
                    &context.signature_status,
                    commitment,
                    &context.notification_sender,
                    block_info,
                    performance_counters,
                );
            })
            .unwrap()
    }

    fn build_thread_to_process_transactions(
        json_rpc_url: String,
        websocket_url: String,
        context: &Arc<LiteRpcContext>,
        receiver: Receiver<Transaction>,
        performance_counters: PerformanceCounter,
    ) -> JoinHandle<()> {
        let context = context.clone();
        Builder::new()
            .name("thread working on confirmation block".to_string())
            .spawn(move || {
            let rpc_client =
                Arc::new(RpcClient::new(json_rpc_url.to_string()));

            let mut connection_cache = Arc::new(ConnectionCache::default());
            let tpu_client = TpuClient::new_with_connection_cache(
                rpc_client.clone(),
                websocket_url.as_str(),
                TpuClientConfig::default(), // value for max fanout slots
                connection_cache.clone(),
            );
            let mut tpu_client = Arc::new(tpu_client.unwrap());
            let mut consecutive_errors: u8 = 0;

            loop {
                let recv_res = receiver.recv();
                match recv_res {
                    Ok(transaction) => {
                        let (fut_res, count) = if TPU_BATCH_SIZE > 1 {
                            let mut transactions_vec = vec![transaction];
                            let mut time_remaining = Duration::from_micros(1000);
                            for _i in 1..TPU_BATCH_SIZE {
                                let start = std::time::Instant::now();
                                let another = receiver.recv_timeout(time_remaining);

                                match another {
                                    Ok(x) => transactions_vec.push(x),
                                    Err(_) => break,
                                }
                                match time_remaining.checked_sub(start.elapsed()) {
                                    Some(x) => time_remaining = x,
                                    None => break,
                                }
                            }
                            let count: u64 = transactions_vec.len() as u64;
                            let slice = transactions_vec.as_slice();
                            let fut_res = tpu_client.try_send_transaction_batch(slice);

                            // insert sent transactions into signature status map
                            transactions_vec.iter().for_each(|x| {
                                let signature = x.signatures[0].to_string();
                                context.signature_status.insert(
                                    signature.clone(),
                                    SignatureStatus {
                                        status: None,
                                        error: None,
                                        created: Instant::now(),
                                    },
                                );
                            });
                            (fut_res, count)
                        } else {
                            let fut_res = tpu_client.try_send_transaction(&transaction);
                            let signature = transaction.signatures[0].to_string();
                            context.signature_status.insert(
                                signature.clone(),
                                SignatureStatus {
                                    status: None,
                                    error: None,
                                    created: Instant::now(),
                                },
                            );
                            (fut_res, 1)
                        };

                        match fut_res {
                            Ok(_) => {
                                consecutive_errors = 0;

                                performance_counters
                                .total_transactions_sent
                                .fetch_add(count, Ordering::Relaxed);
                            },
                            Err(e) => {
                                println!("Got error while sending transaction batch of size {}, error {}", count, e.to_string());
                                consecutive_errors += 1;
                                if consecutive_errors > 3 {
                                    connection_cache = Arc::new(ConnectionCache::default());

                                    let new_tpu_client = TpuClient::new_with_connection_cache(
                                        rpc_client.clone(),
                                        websocket_url.as_str(),
                                        TpuClientConfig::default(), // value for max fanout slots
                                        connection_cache.clone(),
                                    );
                                    // reset TPU connection
                                    tpu_client = Arc::new(new_tpu_client.unwrap());
                                }
                                performance_counters
                                    .transaction_sent_error
                                    .fetch_add(count, Ordering::Relaxed);
                            }
                        };
                    }
                    Err(e) => {
                        println!("got error on tpu channel {}", e.to_string());
                        break;
                    }
                };
            }
        }).unwrap()
    }

    fn process_block(
        reciever: Receiver<RpcResponse<RpcBlockUpdate>>,
        signature_status: &DashMap<String, SignatureStatus>,
        commitment: CommitmentLevel,
        notification_sender: &crossbeam_channel::Sender<NotificationType>,
        block_information: &BlockInformation,
        performance_counters: PerformanceCounter,
    ) {
        loop {
            let block_data = reciever.recv();
            match block_data {
                Ok(data) => {
                    let block_update = &data.value;
                    block_information
                        .slot
                        .store(block_update.slot, Ordering::Relaxed);
                    let slot_notification = SlotNotification {
                        commitment: commitment,
                        slot: block_update.slot,
                        parent: 0,
                        root: 0,
                    };
                    if let Err(e) =
                        notification_sender.send(NotificationType::Slot(slot_notification))
                    {
                        println!("Error sending slot notification error : {}", e.to_string());
                    }

                    if let Some(block) = &block_update.block {
                        block_information
                            .block_height
                            .store(block.block_height.unwrap(), Ordering::Relaxed);
                        // context to update blockhash
                        {
                            let mut lock = block_information.block_hash.write().unwrap();
                            *lock = block.blockhash.clone();
                        }

                        if let Some(transactions) = &block.transactions {
                            for transaction in transactions {
                                let decoded_transaction =
                                    &transaction.transaction.decode().unwrap();

                                let signature = decoded_transaction.signatures[0].to_string();
                                match signature_status.entry(signature.clone()) {
                                    dashmap::mapref::entry::Entry::Occupied(mut x) => {
                                        // get signature status
                                        let transaction_error = match &transaction.meta {
                                            Some(x) => x.err.clone(),
                                            None => {
                                                println!("cannot decode transaction error");
                                                None
                                            }
                                        };
                                        let signature_notification = SignatureNotification {
                                            signature: Signature::from_str(signature.as_str())
                                                .unwrap(),
                                            commitment,
                                            slot: block_update.slot,
                                            error: transaction_error.clone().map(|x| x.to_string()),
                                        };
                                        if let Err(e) = notification_sender.send(
                                            NotificationType::Signature(signature_notification),
                                        ) {
                                            println!(
                                                "Error sending signature notification error : {}",
                                                e.to_string()
                                            );
                                        }
                                        if commitment.eq(&CommitmentLevel::Finalized) {
                                            performance_counters
                                                .total_finalized
                                                .fetch_add(1, Ordering::Relaxed);
                                        } else {
                                            performance_counters
                                                .total_confirmations
                                                .fetch_add(1, Ordering::Relaxed);
                                        }

                                        x.insert(SignatureStatus {
                                            status: Some(commitment),
                                            error: transaction_error,
                                            created: Instant::now(),
                                        });
                                    }
                                    dashmap::mapref::entry::Entry::Vacant(_x) => {
                                        // do nothing transaction not sent by lite rpc
                                    }
                                }
                            }
                        } else {
                            println!(
                                "Cannot get signatures at slot {} block hash {}",
                                block_update.slot, block.blockhash,
                            );
                        }
                    } else {
                        println!("Cannot get a block at slot {}", block_update.slot);
                    }
                }
                Err(e) => {
                    println!("Got error when recieving the block ({})", e);
                }
            }
        }
    }

    pub fn free(&mut self) {
        let subscribed_clients = &mut self.subscribed_clients.lock().unwrap();
        let len_sc = subscribed_clients.len();
        for _i in 0..len_sc {
            let mut subscribed_client = subscribed_clients.pop().unwrap();
            subscribed_client.send_unsubscribe().unwrap();
            subscribed_client.shutdown().unwrap();
        }

        let joinables = &mut self.joinables.lock().unwrap();
        let len = joinables.len();
        for _i in 0..len {
            joinables.pop().unwrap().join().unwrap();
        }
    }
}

impl Metadata for LightRpcRequestProcessor {}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcPerformanceCounterResults {
    pub transactions_per_seconds: u64,
    pub confirmations_per_seconds: u64,
    pub total_transactions_count: u64,
    pub total_confirmations_count: u64,
    pub memory_used: u64,
    pub nb_threads: u64,
}

pub mod lite_rpc {
    use std::str::FromStr;

    use itertools::Itertools;
    use solana_sdk::{fee_calculator::FeeCalculator, pubkey::Pubkey, transaction::Transaction};
    use solana_transaction_status::{TransactionConfirmationStatus, TransactionStatus};

    use super::*;
    #[rpc]
    pub trait Lite {
        type Metadata;

        #[rpc(meta, name = "sendTransaction")]
        fn send_transaction(
            &self,
            meta: Self::Metadata,
            data: String,
            config: Option<RpcSendTransactionConfig>,
        ) -> Result<String>;

        #[rpc(meta, name = "getRecentBlockhash")]
        fn get_recent_blockhash(
            &self,
            meta: Self::Metadata,
            commitment: Option<CommitmentConfig>,
        ) -> Result<RpcResponse<RpcBlockhashFeeCalculator>>;

        #[rpc(meta, name = "confirmTransaction")]
        fn confirm_transaction(
            &self,
            meta: Self::Metadata,
            signature_str: String,
            commitment: Option<CommitmentConfig>,
        ) -> Result<RpcResponse<bool>>;

        #[rpc(meta, name = "requestAirdrop")]
        fn request_airdrop(
            &self,
            meta: Self::Metadata,
            pubkey_str: String,
            lamports: u64,
            config: Option<RpcRequestAirdropConfig>,
        ) -> Result<String>;

        #[rpc(meta, name = "getLatestBlockhash")]
        fn get_latest_blockhash(
            &self,
            meta: Self::Metadata,
            config: Option<RpcContextConfig>,
        ) -> Result<RpcResponse<RpcBlockhash>>;

        #[rpc(meta, name = "getSignatureStatuses")]
        fn get_signature_statuses(
            &self,
            meta: Self::Metadata,
            signature_strs: Vec<String>,
            config: Option<RpcSignatureStatusConfig>,
        ) -> Result<RpcResponse<Vec<Option<TransactionStatus>>>>;

        #[rpc(name = "getVersion")]
        fn get_version(&self) -> Result<RpcVersionInfo>;
    }

    pub struct LightRpc;
    impl Lite for LightRpc {
        type Metadata = LightRpcRequestProcessor;

        fn send_transaction(
            &self,
            meta: Self::Metadata,
            data: String,
            config: Option<RpcSendTransactionConfig>,
        ) -> Result<String> {
            let config = config.unwrap_or_default();
            let encoding = config.encoding;
            let tx_encoding = encoding.unwrap_or(UiTransactionEncoding::Base58);
            let binary_encoding = tx_encoding.into_binary_encoding().ok_or_else(|| {
                Error::invalid_params(format!(
                    "unsupported encoding: {}. Supported encodings: base58, base64",
                    tx_encoding
                ))
            })?;
            let transaction = decode_and_deserialize::<Transaction>(data, binary_encoding)?;
            let signature = transaction.signatures[0].to_string();
            meta.performance_counter
                .total_transactions_recieved
                .fetch_add(1, Ordering::Relaxed);

            match meta.tpu_producer_channel.send(transaction) {
                Ok(_) => Ok(signature),
                Err(e) => {
                    println!("got error while sending on channel {}", e.to_string());
                    Err(jsonrpc_core::Error::new(
                        jsonrpc_core::ErrorCode::InternalError,
                    ))
                }
            }
        }

        fn get_recent_blockhash(
            &self,
            meta: Self::Metadata,
            commitment: Option<CommitmentConfig>,
        ) -> Result<RpcResponse<RpcBlockhashFeeCalculator>> {
            let commitment = match commitment {
                Some(x) => x.commitment,
                None => CommitmentLevel::Finalized,
            };
            let (block_hash, slot) = match commitment {
                CommitmentLevel::Finalized => {
                    let slot = meta
                        .context
                        .finalized_block_info
                        .slot
                        .load(Ordering::Relaxed);
                    let lock = meta.context.finalized_block_info.block_hash.read().unwrap();
                    (lock.clone(), slot)
                }
                _ => {
                    let slot = meta
                        .context
                        .confirmed_block_info
                        .slot
                        .load(Ordering::Relaxed);
                    let lock = meta.context.confirmed_block_info.block_hash.read().unwrap();
                    (lock.clone(), slot)
                }
            };

            Ok(RpcResponse {
                context: RpcResponseContext::new(slot),
                value: RpcBlockhashFeeCalculator {
                    blockhash: block_hash,
                    fee_calculator: FeeCalculator::default(),
                },
            })
        }

        fn get_latest_blockhash(
            &self,
            meta: Self::Metadata,
            config: Option<RpcContextConfig>,
        ) -> Result<RpcResponse<RpcBlockhash>> {
            let commitment = match config {
                Some(x) => match x.commitment {
                    Some(x) => x.commitment,
                    None => CommitmentLevel::Finalized,
                },
                None => CommitmentLevel::Finalized,
            };

            let block_info = match commitment {
                CommitmentLevel::Finalized => &meta.context.finalized_block_info,
                _ => &meta.context.confirmed_block_info,
            };

            let slot = block_info.slot.load(Ordering::Relaxed);
            let last_valid_block_height = block_info.block_height.load(Ordering::Relaxed);
            let blockhash = block_info.block_hash.read().unwrap().clone();

            Ok(RpcResponse {
                context: RpcResponseContext::new(slot),
                value: RpcBlockhash {
                    blockhash,
                    last_valid_block_height,
                },
            })
        }

        fn confirm_transaction(
            &self,
            meta: Self::Metadata,
            signature_str: String,
            commitment_cfg: Option<CommitmentConfig>,
        ) -> Result<RpcResponse<bool>> {
            let singature_status = meta.context.signature_status.get(&signature_str);
            let k_value = singature_status;

            let commitment = match commitment_cfg {
                Some(x) => x.commitment,
                None => CommitmentLevel::Confirmed,
            };

            let slot = if commitment.eq(&CommitmentLevel::Finalized) {
                meta.context
                    .finalized_block_info
                    .slot
                    .load(Ordering::Relaxed)
            } else {
                meta.context
                    .confirmed_block_info
                    .slot
                    .load(Ordering::Relaxed)
            };

            match k_value {
                Some(value) => match value.status {
                    Some(commitment) => {
                        let commitment_matches = if commitment.eq(&CommitmentLevel::Finalized) {
                            commitment.eq(&CommitmentLevel::Finalized)
                        } else {
                            commitment.eq(&CommitmentLevel::Finalized)
                                || commitment.eq(&CommitmentLevel::Confirmed)
                        };
                        Ok(RpcResponse {
                            context: RpcResponseContext::new(slot),
                            value: commitment_matches && value.error.is_none(),
                        })
                    }
                    None => Ok(RpcResponse {
                        context: RpcResponseContext::new(slot),
                        value: false,
                    }),
                },
                None => {
                    let signature = Signature::from_str(signature_str.as_str()).unwrap();
                    let ans = match commitment_cfg {
                        None => meta.rpc_client.confirm_transaction(&signature).unwrap(),
                        Some(cfg) => {
                            meta.rpc_client
                                .confirm_transaction_with_commitment(&signature, cfg)
                                .unwrap()
                                .value
                        }
                    };
                    Ok(RpcResponse {
                        context: RpcResponseContext::new(slot),
                        value: ans,
                    })
                }
            }
        }

        fn get_signature_statuses(
            &self,
            meta: Self::Metadata,
            signature_strs: Vec<String>,
            _config: Option<RpcSignatureStatusConfig>,
        ) -> Result<RpcResponse<Vec<Option<TransactionStatus>>>> {
            let confirmed_slot = meta
                .context
                .confirmed_block_info
                .slot
                .load(Ordering::Relaxed);
            let status = signature_strs
                .iter()
                .map(|x| {
                    let singature_status = meta.context.signature_status.get(x);
                    let k_value = singature_status;
                    match k_value {
                        Some(value) => match value.status {
                            Some(commitment_level) => {
                                let slot = meta
                                    .context
                                    .confirmed_block_info
                                    .slot
                                    .load(Ordering::Relaxed);

                                let status = match commitment_level {
                                    CommitmentLevel::Finalized => {
                                        TransactionConfirmationStatus::Finalized
                                    }
                                    _ => TransactionConfirmationStatus::Confirmed,
                                };
                                Some(TransactionStatus {
                                    slot,
                                    confirmations: None,
                                    status: Ok(()),
                                    err: value.error.clone(),
                                    confirmation_status: Some(status),
                                })
                            }
                            None => None,
                        },
                        None => None,
                    }
                })
                .collect_vec();
            Ok(RpcResponse {
                context: RpcResponseContext::new(confirmed_slot),
                value: status,
            })
        }

        fn request_airdrop(
            &self,
            meta: Self::Metadata,
            pubkey_str: String,
            lamports: u64,
            config: Option<RpcRequestAirdropConfig>,
        ) -> Result<String> {
            let pubkey = Pubkey::from_str(pubkey_str.as_str()).unwrap();
            let signature = match config {
                Some(c) => meta
                    .rpc_client
                    .request_airdrop_with_config(&pubkey, lamports, c),
                None => meta.rpc_client.request_airdrop(&pubkey, lamports),
            };
            Ok(signature.unwrap().to_string())
        }

        fn get_version(&self) -> Result<RpcVersionInfo> {
            let version = solana_version::Version::default();
            Ok(RpcVersionInfo {
                solana_core: version.to_string(),
                feature_set: Some(version.feature_set),
            })
        }
    }
}

const MAX_BASE58_SIZE: usize = 1683; // Golden, bump if PACKET_DATA_SIZE changes
const MAX_BASE64_SIZE: usize = 1644; // Golden, bump if PACKET_DATA_SIZE changes
fn decode_and_deserialize<T>(encoded: String, encoding: TransactionBinaryEncoding) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    let wire_output = match encoding {
        TransactionBinaryEncoding::Base58 => {
            if encoded.len() > MAX_BASE58_SIZE {
                return Err(Error::invalid_params(format!(
                    "base58 encoded {} too large: {} bytes (max: encoded/raw {}/{})",
                    type_name::<T>(),
                    encoded.len(),
                    MAX_BASE58_SIZE,
                    PACKET_DATA_SIZE,
                )));
            }
            bs58::decode(encoded)
                .into_vec()
                .map_err(|e| Error::invalid_params(format!("invalid base58 encoding: {:?}", e)))?
        }
        TransactionBinaryEncoding::Base64 => {
            if encoded.len() > MAX_BASE64_SIZE {
                return Err(Error::invalid_params(format!(
                    "base64 encoded {} too large: {} bytes (max: encoded/raw {}/{})",
                    type_name::<T>(),
                    encoded.len(),
                    MAX_BASE64_SIZE,
                    PACKET_DATA_SIZE,
                )));
            }
            base64::decode(encoded)
                .map_err(|e| Error::invalid_params(format!("invalid base64 encoding: {:?}", e)))?
        }
    };
    if wire_output.len() > PACKET_DATA_SIZE {
        return Err(Error::invalid_params(format!(
            "decoded {} too large: {} bytes (max: {} bytes)",
            type_name::<T>(),
            wire_output.len(),
            PACKET_DATA_SIZE
        )));
    }
    bincode::options()
        .with_limit(PACKET_DATA_SIZE as u64)
        .with_fixint_encoding()
        .allow_trailing_bytes()
        .deserialize_from(&wire_output[..])
        .map_err(|err| {
            Error::invalid_params(format!(
                "failed to deserialize {}: {}",
                type_name::<T>(),
                &err.to_string()
            ))
        })
}
