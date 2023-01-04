use crossbeam_channel::Sender;
use dashmap::DashMap;
use serde::Serialize;
use solana_client::{
    rpc_client::RpcClient,
    rpc_response::{ProcessedSignatureResult, RpcResponseContext, RpcSignatureResult, SlotInfo},
};
use solana_rpc::rpc_subscription_tracker::{
    SignatureSubscriptionParams, SubscriptionId, SubscriptionParams,
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

#[derive(Clone)]
pub struct SignatureStatus {
    pub commitment_level: CommitmentLevel,
    pub transaction_error: Option<TransactionError>,
}

pub struct LiteRpcContext {
    pub signature_status: DashMap<String, Option<SignatureStatus>>,
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
                                    Some(LiteRpcNotification {
                                        subscription_id: *x.get(),
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
                                SubscriptionId::from(0)
                            } else {
                                SubscriptionId::from(1)
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
    pub total_confirmations: Arc<AtomicU64>,
    pub total_transactions_sent: Arc<AtomicU64>,

    pub confirmations_per_seconds: Arc<AtomicU64>,
    pub transactions_per_seconds: Arc<AtomicU64>,

    last_count_for_confirmations: Arc<AtomicU64>,
    last_count_for_transactions_sent: Arc<AtomicU64>,
}

impl PerformanceCounter {
    pub fn new() -> Self {
        Self {
            total_confirmations: Arc::new(AtomicU64::new(0)),
            total_transactions_sent: Arc::new(AtomicU64::new(0)),
            confirmations_per_seconds: Arc::new(AtomicU64::new(0)),
            transactions_per_seconds: Arc::new(AtomicU64::new(0)),
            last_count_for_confirmations: Arc::new(AtomicU64::new(0)),
            last_count_for_transactions_sent: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn update_per_seconds_transactions(&self) {
        let total_confirmations: u64 = self.total_confirmations.load(Ordering::Relaxed);

        let total_transactions: u64 = self.total_transactions_sent.load(Ordering::Relaxed);

        self.confirmations_per_seconds.store(
            total_confirmations - self.last_count_for_confirmations.load(Ordering::Relaxed),
            Ordering::Release,
        );
        self.transactions_per_seconds.store(
            total_transactions
                - self
                    .last_count_for_transactions_sent
                    .load(Ordering::Relaxed),
            Ordering::Release,
        );

        self.last_count_for_confirmations
            .store(total_confirmations, Ordering::Relaxed);
        self.last_count_for_transactions_sent
            .store(total_transactions, Ordering::Relaxed);
    }

    pub fn update_sent_transactions_counter(&self) {
        self.total_transactions_sent.fetch_add(1, Ordering::Relaxed);
    }

    pub fn update_confirm_transaction_counter(&self) {
        self.total_confirmations.fetch_add(1, Ordering::Relaxed);
    }
}

pub fn launch_performance_updating_thread(
    performance_counter: PerformanceCounter,
) -> JoinHandle<()> {
    Builder::new()
        .name("Performance Counter".to_string())
        .spawn(move || loop {
            let start = Instant::now();

            let wait_time = Duration::from_millis(1000);
            let performance_counter = performance_counter.clone();
            performance_counter.update_per_seconds_transactions();
            let confirmations_per_seconds = performance_counter
                .confirmations_per_seconds
                .load(Ordering::Acquire);
            let total_transactions_per_seconds = performance_counter
                .transactions_per_seconds
                .load(Ordering::Acquire);

            let runtime = start.elapsed();
            if let Some(remaining) = wait_time.checked_sub(runtime) {
                println!(
                    "Sent {} transactions and confrimed {} transactions",
                    total_transactions_per_seconds, confirmations_per_seconds
                );
                thread::sleep(remaining);
            }
        })
        .unwrap()
}
