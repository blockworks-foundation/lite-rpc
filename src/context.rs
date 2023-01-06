use crossbeam_channel::Sender;
use dashmap::DashMap;
use serde::Serialize;
use solana_client::{
    rpc_client::RpcClient,
    rpc_response::{ProcessedSignatureResult, RpcResponseContext, RpcSignatureResult, SlotInfo},
};
use solana_rpc::rpc_subscription_tracker::{
    SignatureSubscriptionParams, SubscriptionParams,
};
use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel},
    signature::Signature,
    transaction::TransactionError,
};
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, RwLock,
    },
    thread::{self, Builder, JoinHandle},
    time::{Duration, Instant},
};
use tokio::sync::broadcast;

pub struct BlockInformation {
    pub block_hash: RwLock<String>,
    pub block_height: AtomicU64,
    pub slot: AtomicU64,
    pub confirmation_level: CommitmentLevel,
}

impl BlockInformation {
    pub fn new(rpc_client: Arc<RpcClient>, commitment: CommitmentLevel) -> Self {
        let slot = rpc_client
            .get_slot_with_commitment(CommitmentConfig { commitment })
            .unwrap();

        let (blockhash, blockheight) = rpc_client
            .get_latest_blockhash_with_commitment(CommitmentConfig { commitment })
            .unwrap();

        BlockInformation {
            block_hash: RwLock::new(blockhash.to_string()),
            block_height: AtomicU64::new(blockheight),
            slot: AtomicU64::new(slot),
            confirmation_level: commitment,
        }
    }
}

pub struct SignatureStatus {
    pub status: Option<CommitmentLevel>,
    pub error: Option<TransactionError>,
    pub created: Instant,
}

pub struct LiteRpcContext {
    pub signature_status: DashMap<String, SignatureStatus>,
    pub finalized_block_info: BlockInformation,
    pub confirmed_block_info: BlockInformation,
    pub notification_sender: Sender<NotificationType>,
}

impl LiteRpcContext {
    pub fn new(rpc_client: Arc<RpcClient>, notification_sender: Sender<NotificationType>) -> Self {
        LiteRpcContext {
            signature_status: DashMap::new(),
            confirmed_block_info: BlockInformation::new(
                rpc_client.clone(),
                CommitmentLevel::Confirmed,
            ),
            finalized_block_info: BlockInformation::new(rpc_client, CommitmentLevel::Finalized),
            notification_sender,
        }
    }

    pub fn remove_stale_data(&self, purgetime_in_seconds: u64) {
        self.signature_status
            .retain(|_k, v| v.created.elapsed().as_secs() < purgetime_in_seconds);
    }
}

pub struct SignatureNotification {
    pub signature: Signature,
    pub commitment: CommitmentLevel,
    pub slot: u64,
    pub error: Option<String>,
}

pub struct SlotNotification {
    pub slot: u64,
    pub commitment: CommitmentLevel,
    pub parent: u64,
    pub root: u64,
}

pub enum NotificationType {
    Signature(SignatureNotification),
    Slot(SlotNotification),
}

type SubscriptionId = u64;

#[derive(Debug, Serialize)]
struct NotificationParams<T> {
    result: T,
    subscription: SubscriptionId,
}

#[derive(Debug, Serialize)]
struct Notification<T> {
    jsonrpc: Option<jsonrpc_core::Version>,
    method: &'static str,
    params: NotificationParams<T>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Response<T> {
    pub context: RpcResponseContext,
    pub value: T,
}

#[derive(Debug, Clone, PartialEq)]
struct RpcNotificationResponse<T> {
    context: RpcNotificationContext,
    value: T,
}

impl<T> From<RpcNotificationResponse<T>> for Response<T> {
    fn from(notification: RpcNotificationResponse<T>) -> Self {
        let RpcNotificationResponse {
            context: RpcNotificationContext { slot },
            value,
        } = notification;
        Self {
            context: RpcResponseContext {
                slot,
                api_version: None,
            },
            value,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RpcNotificationContext {
    slot: u64,
}

#[derive(Debug, Clone)]
pub struct LiteRpcNotification {
    pub subscription_id: SubscriptionId,
    pub is_final: bool,
    pub json: String,
    pub created_at: Instant,
}

pub struct LiteRpcSubsrciptionControl {
    pub broadcast_sender: broadcast::Sender<LiteRpcNotification>,
    notification_reciever: crossbeam_channel::Receiver<NotificationType>,
    pub subscriptions: DashMap<SubscriptionParams, SubscriptionId>,
    pub last_subscription_id: AtomicU64,
}

impl LiteRpcSubsrciptionControl {
    pub fn new(
        broadcast_sender: broadcast::Sender<LiteRpcNotification>,
        notification_reciever: crossbeam_channel::Receiver<NotificationType>,
    ) -> Self {
        Self {
            broadcast_sender,
            notification_reciever,
            subscriptions: DashMap::new(),
            last_subscription_id: AtomicU64::new(2),
        }
    }

    pub fn start_broadcasting(&self) {
        loop {
            let notification = self.notification_reciever.recv();
            match notification {
                Ok(notification_type) => {
                    let rpc_notification = match notification_type {
                        NotificationType::Signature(data) => {
                            let signature_params = SignatureSubscriptionParams {
                                commitment: CommitmentConfig {
                                    commitment: data.commitment,
                                },
                                signature: data.signature,
                                enable_received_notification: false,
                            };

                            let param = SubscriptionParams::Signature(signature_params);

                            match self.subscriptions.entry(param) {
                                dashmap::mapref::entry::Entry::Occupied(x) => {
                                    let subscription_id = *x.get();
                                    let slot = data.slot;
                                    let value = Response::from(RpcNotificationResponse {
                                        context: RpcNotificationContext { slot },
                                        value: RpcSignatureResult::ProcessedSignature(
                                            ProcessedSignatureResult { err: None },
                                        ),
                                    });

                                    let notification = Notification {
                                        jsonrpc: Some(jsonrpc_core::Version::V2),
                                        method: &"signatureNotification",
                                        params: NotificationParams {
                                            result: value,
                                            subscription: subscription_id,
                                        },
                                    };
                                    let json = serde_json::to_string(&notification).unwrap();
                                    let subscription_id = *x.get();

                                    // no more notification for this signature has been finalized
                                    if data.commitment.eq(&CommitmentLevel::Finalized) {
                                        x.remove();
                                    }
                                    Some(LiteRpcNotification {
                                        subscription_id,
                                        created_at: Instant::now(),
                                        is_final: false,
                                        json,
                                    })
                                }
                                dashmap::mapref::entry::Entry::Vacant(_x) => None,
                            }
                        }
                        NotificationType::Slot(data) => {
                            // SubscriptionId 0 will be used for slots
                            let subscription_id = if data.commitment == CommitmentLevel::Confirmed {
                                0
                            } else {
                                1
                            };
                            let value = SlotInfo {
                                parent: data.parent,
                                slot: data.slot,
                                root: data.root,
                            };

                            let notification = Notification {
                                jsonrpc: Some(jsonrpc_core::Version::V2),
                                method: &"slotNotification",
                                params: NotificationParams {
                                    result: value,
                                    subscription: subscription_id,
                                },
                            };
                            let json = serde_json::to_string(&notification).unwrap();
                            Some(LiteRpcNotification {
                                subscription_id: subscription_id,
                                created_at: Instant::now(),
                                is_final: false,
                                json,
                            })
                        }
                    };
                    if let Some(rpc_notification) = rpc_notification {
                        self.broadcast_sender.send(rpc_notification).unwrap();
                    }
                }
                Err(_e) => {
                    break;
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct PerformanceCounter {
    pub total_finalized: Arc<AtomicU64>,
    pub total_confirmations: Arc<AtomicU64>,
    pub total_transactions_sent: Arc<AtomicU64>,
    pub transaction_sent_error: Arc<AtomicU64>,
    pub total_transactions_recieved: Arc<AtomicU64>,

    last_count_for_finalized: Arc<AtomicU64>,
    last_count_for_confirmations: Arc<AtomicU64>,
    last_count_for_transactions_sent: Arc<AtomicU64>,
    last_count_for_sent_errors: Arc<AtomicU64>,
    last_count_for_transactions_recieved: Arc<AtomicU64>,
}

pub struct PerformancePerSec {
    pub finalized_per_seconds: u64,
    pub confirmations_per_seconds: u64,
    pub transactions_per_seconds: u64,
    pub send_transactions_errors_per_seconds: u64,
    pub transaction_recieved_per_second: u64,
}

impl PerformanceCounter {
    pub fn new() -> Self {
        Self {
            total_finalized: Arc::new(AtomicU64::new(0)),
            total_confirmations: Arc::new(AtomicU64::new(0)),
            total_transactions_sent: Arc::new(AtomicU64::new(0)),
            total_transactions_recieved: Arc::new(AtomicU64::new(0)),
            transaction_sent_error: Arc::new(AtomicU64::new(0)),
            last_count_for_finalized: Arc::new(AtomicU64::new(0)),
            last_count_for_confirmations: Arc::new(AtomicU64::new(0)),
            last_count_for_transactions_sent: Arc::new(AtomicU64::new(0)),
            last_count_for_transactions_recieved: Arc::new(AtomicU64::new(0)),
            last_count_for_sent_errors: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn update_per_seconds_transactions(&self) -> PerformancePerSec {
        let total_finalized: u64 = self.total_finalized.load(Ordering::Relaxed);
        let total_confirmations: u64 = self.total_confirmations.load(Ordering::Relaxed);
        let total_transactions: u64 = self.total_transactions_sent.load(Ordering::Relaxed);
        let total_errors: u64 = self.transaction_sent_error.load(Ordering::Relaxed);
        let total_transactions_recieved: u64 =
            self.total_transactions_recieved.load(Ordering::Relaxed);

        let finalized_per_seconds = total_finalized
            - self
                .last_count_for_finalized
                .swap(total_finalized, Ordering::Relaxed);
        let confirmations_per_seconds = total_confirmations
            - self
                .last_count_for_confirmations
                .swap(total_confirmations, Ordering::Relaxed);
        let transactions_per_seconds = total_transactions
            - self
                .last_count_for_transactions_sent
                .swap(total_transactions, Ordering::Relaxed);
        let send_transactions_errors_per_seconds = total_errors
            - self
                .last_count_for_sent_errors
                .swap(total_errors, Ordering::Relaxed);
        let transaction_recieved_per_second = total_transactions_recieved
            - self
                .last_count_for_transactions_recieved
                .swap(total_transactions_recieved, Ordering::Relaxed);

        PerformancePerSec {
            confirmations_per_seconds,
            finalized_per_seconds,
            send_transactions_errors_per_seconds,
            transaction_recieved_per_second,
            transactions_per_seconds,
        }
    }
}

const PRINT_COUNTERS : bool = true;
pub fn launch_performance_updating_thread(
    performance_counter: PerformanceCounter,
) -> JoinHandle<()> {
    Builder::new()
        .name("Performance Counter".to_string())
        .spawn(move || {
            let mut nb_seconds: u64 = 0;
            loop {
                let start = Instant::now();

                let wait_time = Duration::from_millis(1000);
                let performance_counter = performance_counter.clone();
                let data = performance_counter.update_per_seconds_transactions();
                if PRINT_COUNTERS {
                    println!(
                        "At {} second, Recieved {}, Sent {} transactions, finalized {} and confirmed {} transactions",
                        nb_seconds, data.transaction_recieved_per_second, data.transactions_per_seconds, data.finalized_per_seconds, data.confirmations_per_seconds
                    );
                }
                let runtime = start.elapsed();
                nb_seconds += 1;
                if let Some(remaining) = wait_time.checked_sub(runtime) {
                    thread::sleep(remaining);
                }
            }
        })
        .unwrap()
}
