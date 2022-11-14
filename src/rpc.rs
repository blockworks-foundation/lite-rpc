use solana_client::{
    pubsub_client::{BlockSubscription, PubsubClientError},
    tpu_client::TpuClientConfig,
};
use solana_pubsub_client::pubsub_client::{PubsubBlockClientSubscription, PubsubClient};
use std::thread::{Builder, JoinHandle};

use crate::context::{BlockInformation, LiteRpcContext};
use {
    bincode::config::Options,
    crossbeam_channel::Receiver,
    jsonrpc_core::{Error, Metadata, Result},
    jsonrpc_derive::rpc,
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
    solana_tpu_client::connection_cache::ConnectionCache,
    solana_transaction_status::{TransactionBinaryEncoding, UiTransactionEncoding},
    std::{
        any::type_name,
        collections::HashMap,
        sync::{atomic::Ordering, Arc, RwLock},
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
    _joinables: Arc<Vec<JoinHandle<()>>>,
    _subscribed_clients: Arc<Vec<PubsubBlockClientSubscription>>,
}

impl LightRpcRequestProcessor {
    pub fn new(json_rpc_url: &String, websocket_url: &String) -> LightRpcRequestProcessor {
        let rpc_client = Arc::new(RpcClient::new(json_rpc_url.as_str()));
        let connection_cache = Arc::new(ConnectionCache::default());
        let tpu_client = Arc::new(
            TpuClient::new_with_connection_cache(
                rpc_client.clone(),
                websocket_url.as_str(),
                TpuClientConfig::default(),
                connection_cache.clone(),
            )
            .unwrap(),
        );

        let context = Arc::new(LiteRpcContext::new(rpc_client.clone()));

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
            ws_url: websocket_url.clone(),
            context: context,
            _connection_cache: connection_cache,
            _joinables: Arc::new(joinables),
            _subscribed_clients: Arc::new(vec![client_confirmed, client_finalized]),
        }
    }

    fn subscribe_block(
        websocket_url: &String,
        commitment: CommitmentLevel,
    ) -> std::result::Result<BlockSubscription, PubsubClientError> {
        PubsubClient::block_subscribe(
            websocket_url.as_str(),
            RpcBlockSubscribeFilter::All,
            Some(RpcBlockSubscribeConfig {
                commitment: Some(CommitmentConfig {
                    commitment: commitment,
                }),
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
                Self::process_block(reciever, &context.signature_status, commitment, block_info);
            })
            .unwrap()
    }

    fn process_block(
        reciever: Receiver<RpcResponse<RpcBlockUpdate>>,
        signature_status: &RwLock<HashMap<String, Option<CommitmentLevel>>>,
        commitment: CommitmentLevel,
        block_information: &BlockInformation,
    ) {
        println!("processing blocks for {}", commitment.to_string());
        loop {
            let block_data = reciever.recv();

            match block_data {
                Ok(data) => {
                    let block_update = &data.value;
                    block_information
                        .slot
                        .store(block_update.slot, Ordering::Relaxed);

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
                            let mut lock = signature_status.write().unwrap();
                            for signature in signatures {
                                if lock.contains_key(signature) {
                                    println!(
                                        "found signature {} for commitment {}",
                                        signature, commitment
                                    );
                                    lock.insert(signature.clone(), Some(commitment));
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
                    println!("Got error when recieving the block ({})", e.to_string());
                }
            }
        }
    }
}

impl Metadata for LightRpcRequestProcessor {}

pub mod lite_rpc {
    use std::str::FromStr;

    use solana_sdk::{fee_calculator::FeeCalculator, pubkey::Pubkey};

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
                decode_and_deserialize::<VersionedTransaction>(data.clone(), binary_encoding)?;

            {
                let mut lock = meta.context.signature_status.write().unwrap();
                lock.insert(transaction.signatures[0].to_string(), None);
                println!("added {} to map", transaction.signatures[0].to_string());
            }
            meta.tpu_client
                .send_wire_transaction(wire_transaction.clone());
            Ok(transaction.signatures[0].to_string())
        }

        fn get_recent_blockhash(
            &self,
            meta: Self::Metadata,
            commitment: Option<CommitmentConfig>,
        ) -> Result<RpcResponse<RpcBlockhashFeeCalculator>> {
            let commitment = match commitment {
                Some(x) => x.commitment,
                None => CommitmentLevel::Confirmed,
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

        fn confirm_transaction(
            &self,
            meta: Self::Metadata,
            signature_str: String,
            commitment_cfg: Option<CommitmentConfig>,
        ) -> Result<RpcResponse<bool>> {
            let lock = meta.context.signature_status.read().unwrap();
            let k_value = lock.get_key_value(&signature_str);
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
                Some(value) => match value.1 {
                    Some(commitment_for_signature) => {
                        println!("found in cache");
                        return Ok(RpcResponse {
                            context: RpcResponseContext::new(slot),
                            value: if commitment.eq(&CommitmentLevel::Finalized) {
                                commitment_for_signature.eq(&CommitmentLevel::Finalized)
                            } else {
                                commitment_for_signature.eq(&CommitmentLevel::Finalized)
                                    || commitment_for_signature.eq(&CommitmentLevel::Confirmed)
                            },
                        });
                    }
                    None => {
                        return Ok(RpcResponse {
                            context: RpcResponseContext::new(slot),
                            value: false,
                        })
                    }
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
                    return Ok(RpcResponse {
                        context: RpcResponseContext::new(slot),
                        value: ans,
                    });
                }
            };
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
