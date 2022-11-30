use crossbeam_channel::Sender;
use dashmap::DashMap;
use serde::Serialize;
use solana_client::{
    rpc_client::RpcClient,
    rpc_response::{ReceivedSignatureResult, RpcResponseContext, RpcSignatureResult, SlotInfo},
};
use solana_rpc::rpc_subscription_tracker::{
    SignatureSubscriptionParams, SubscriptionId, SubscriptionParams,
};
use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel},
    signature::Signature,
};
use std::{
    sync::{atomic::AtomicU64, Arc, RwLock},
    time::Instant,
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

pub struct LiteRpcContext {
    pub signature_status: DashMap<String, Option<CommitmentLevel>>,
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
                            println!(
                                "getting signature notification {} confirmation {}",
                                data.signature,
                                data.commitment.to_string()
                            );
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
                                        value: RpcSignatureResult::ReceivedSignature(
                                            ReceivedSignatureResult::ReceivedSignature,
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
                Err(e) => {
                    break;
                }
            }
        }
    }
}
