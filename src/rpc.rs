use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use solana_client::{
    pubsub_client::{BlockSubscription, PubsubClientError},
    tpu_client::TpuClientConfig,
};
use solana_pubsub_client::pubsub_client::{PubsubBlockClientSubscription, PubsubClient};
use std::{
    str::FromStr,
    sync::Mutex,
    thread::{Builder, JoinHandle},
};

use crate::context::{
    BlockInformation, LiteRpcContext, NotificationType, PerformanceCounter, SignatureNotification,
    SlotNotification,
};
use crossbeam_channel::Sender;
use {
    bincode::config::Options,
    crossbeam_channel::Receiver,
    jsonrpc_core::{Error, Metadata, Result},
    jsonrpc_derive::rpc,
    solana_client::connection_cache::ConnectionCache,
    solana_client::{rpc_client::RpcClient, tpu_client::TpuClient},
    solana_perf::packet::PACKET_DATA_SIZE,
    solana_rpc_client_api::{
        config::*,
        response::{Response as RpcResponse, *},
    },
    solana_sdk::{
        commitment_config::{CommitmentConfig, CommitmentLevel},
        signature::Signature,
        transaction::VersionedTransaction,
    },
    solana_transaction_status::{TransactionBinaryEncoding, UiTransactionEncoding},
    std::{
        any::type_name,
        sync::{atomic::Ordering, Arc},
    },
};

#[derive(Clone)]
pub struct LightRpcRequestProcessor {
    pub rpc_client: Arc<RpcClient>,
    pub tpu_client: Arc<TpuClient>,
    pub last_valid_block_height: u64,
    pub ws_url: String,
    pub context: Arc<LiteRpcContext>,
    _connection_cache: Arc<ConnectionCache>,
    joinables: Arc<Mutex<Vec<JoinHandle<()>>>>,
    subscribed_clients: Arc<Mutex<Vec<PubsubBlockClientSubscription>>>,
    performance_counter: PerformanceCounter,
}

impl LightRpcRequestProcessor {
    pub fn new(
        json_rpc_url: &str,
        websocket_url: &str,
        notification_sender: Sender<NotificationType>,
        performance_counter: PerformanceCounter,
    ) -> LightRpcRequestProcessor {
        let rpc_client = Arc::new(RpcClient::new(json_rpc_url));
        let connection_cache = Arc::new(ConnectionCache::default());
        let tpu_client = Arc::new(
            TpuClient::new_with_connection_cache(
                rpc_client.clone(),
                websocket_url,
                TpuClientConfig::default(),
                connection_cache.clone(),
            )
            .unwrap(),
        );

        let context = Arc::new(LiteRpcContext::new(rpc_client.clone(), notification_sender));

        // subscribe for confirmed_blocks
        let (client_confirmed, receiver_confirmed) =
            Self::subscribe_block(websocket_url, CommitmentLevel::Confirmed).unwrap();

        // subscribe for finalized blocks
        let (client_finalized, receiver_finalized) =
            Self::subscribe_block(websocket_url, CommitmentLevel::Finalized).unwrap();

        // create threads to listen for finalized and confrimed blocks
        let joinables = vec![
            Self::build_thread_to_process_blocks(
                receiver_confirmed,
                &context,
                CommitmentLevel::Confirmed,
            ),
            Self::build_thread_to_process_blocks(
                receiver_finalized,
                &context,
                CommitmentLevel::Finalized,
            ),
        ];

        LightRpcRequestProcessor {
            rpc_client,
            tpu_client,
            last_valid_block_height: 0,
            ws_url: websocket_url.to_string(),
            context,
            _connection_cache: connection_cache,
            joinables: Arc::new(Mutex::new(joinables)),
            subscribed_clients: Arc::new(Mutex::new(vec![client_confirmed, client_finalized])),
            performance_counter,
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
                transaction_details: Some(
                    solana_transaction_status::TransactionDetails::Signatures,
                ),
                show_rewards: None,
                max_supported_transaction_version: None,
            }),
        )
    }

    fn build_thread_to_process_blocks(
        reciever: Receiver<RpcResponse<RpcBlockUpdate>>,
        context: &Arc<LiteRpcContext>,
        commitment: CommitmentLevel,
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
                );
            })
            .unwrap()
    }

    fn process_block(
        reciever: Receiver<RpcResponse<RpcBlockUpdate>>,
        signature_status: &DashMap<String, Option<CommitmentLevel>>,
        commitment: CommitmentLevel,
        notification_sender: &crossbeam_channel::Sender<NotificationType>,
        block_information: &BlockInformation,
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

                        if let Some(signatures) = &block.signatures {
                            for signature in signatures {
                                match signature_status.entry(signature.clone()) {
                                    dashmap::mapref::entry::Entry::Occupied(mut x) => {
                                        
                                        let signature_notification = SignatureNotification {
                                            signature: Signature::from_str(signature.as_str())
                                                .unwrap(),
                                            commitment,
                                            slot: block_update.slot,
                                            error: None,
                                        };
                                        if let Err(e) = notification_sender.send(
                                            NotificationType::Signature(signature_notification),
                                        ) {
                                            println!(
                                                "Error sending signature notification error : {}",
                                                e.to_string()
                                            );
                                        }
                                        x.insert(Some(commitment));
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
    use solana_sdk::{fee_calculator::FeeCalculator, pubkey::Pubkey};
    use solana_transaction_status::{TransactionStatus, TransactionConfirmationStatus};

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

        #[rpc(meta, name = "getPerformanceCounters")]
        fn get_performance_counters(
            &self,
            meta: Self::Metadata,
        ) -> Result<RpcPerformanceCounterResults>;

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
            let (wire_transaction, transaction) =
                decode_and_deserialize::<VersionedTransaction>(data, binary_encoding)?;

            meta.context
                .signature_status
                .insert(transaction.signatures[0].to_string(), None);
            meta.tpu_client.send_wire_transaction(wire_transaction);
            meta.performance_counter.update_sent_transactions_counter();
            Ok(transaction.signatures[0].to_string())
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
            meta.performance_counter
                .update_confirm_transaction_counter();

            match k_value {
                Some(value) => match *value {
                    Some(commitment_for_signature) => {
                        Ok(RpcResponse {
                            context: RpcResponseContext::new(slot),
                            value: if commitment.eq(&CommitmentLevel::Finalized) {
                                commitment_for_signature.eq(&CommitmentLevel::Finalized)
                            } else {
                                commitment_for_signature.eq(&CommitmentLevel::Finalized)
                                    || commitment_for_signature.eq(&CommitmentLevel::Confirmed)
                            },
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
            let confirmed_slot = meta.context.confirmed_block_info.slot.load(Ordering::Relaxed);
            let status = signature_strs.iter().map(|x| {
                let singature_status = meta.context.signature_status.get(x);
                let k_value = singature_status;
                match k_value {                        
                    Some(value) => match *value {
                        Some(commitment_for_signature) => {
                            let slot = meta.context
                                    .confirmed_block_info
                                    .slot
                                    .load(Ordering::Relaxed);
                            meta.performance_counter
                                .update_confirm_transaction_counter();

                            let status = match commitment_for_signature {
                                CommitmentLevel::Finalized => TransactionConfirmationStatus::Finalized,
                                _ => TransactionConfirmationStatus::Confirmed,
                            };
                            Some(TransactionStatus {
                                slot,
                                confirmations: Some(1),
                                status: Ok(()),
                                err: None,
                                confirmation_status : Some(status)
                            })
                        }
                        None => None,
                    },
                    None => None
                }
            }).collect_vec();
            Ok(
                RpcResponse {
                    context : RpcResponseContext::new(confirmed_slot),
                    value: status,
                }
            )
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

        fn get_performance_counters(
            &self,
            meta: Self::Metadata,
        ) -> Result<RpcPerformanceCounterResults> {
            let total_transactions_count = meta
                .performance_counter
                .total_transactions_sent
                .load(Ordering::Relaxed);
            let total_confirmations_count = meta
                .performance_counter
                .total_confirmations
                .load(Ordering::Relaxed);
            let transactions_per_seconds = meta
                .performance_counter
                .transactions_per_seconds
                .load(Ordering::Acquire);
            let confirmations_per_seconds = meta
                .performance_counter
                .confirmations_per_seconds
                .load(Ordering::Acquire);

            let procinfo::pid::Statm { size, .. } = procinfo::pid::statm_self().unwrap();
            let procinfo::pid::Stat { num_threads, .. } = procinfo::pid::stat_self().unwrap();

            Ok(RpcPerformanceCounterResults {
                confirmations_per_seconds,
                transactions_per_seconds,
                total_confirmations_count,
                total_transactions_count,
                memory_used: size as u64,
                nb_threads: num_threads as u64,
            })
        }
    }
}

const MAX_BASE58_SIZE: usize = 1683; // Golden, bump if PACKET_DATA_SIZE changes
const MAX_BASE64_SIZE: usize = 1644; // Golden, bump if PACKET_DATA_SIZE changes
fn decode_and_deserialize<T>(
    encoded: String,
    encoding: TransactionBinaryEncoding,
) -> Result<(Vec<u8>, T)>
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
        .map(|output| (wire_output, output))
}
