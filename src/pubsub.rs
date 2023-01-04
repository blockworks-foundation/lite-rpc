use dashmap::DashMap;
use jsonrpc_core::{ErrorCode, IoHandler};
use soketto::handshake::{server, Server};
use solana_rpc::rpc_subscription_tracker::{SignatureSubscriptionParams, SubscriptionParams};
use std::{net::SocketAddr, str::FromStr, thread::JoinHandle, time::Instant};
use stream_cancel::{Trigger, Tripwire};
use tokio::{net::TcpStream, pin, select};
use tokio_util::compat::TokioAsyncReadCompatExt;

use crate::context::{LiteRpcSubsrciptionControl, PerformanceCounter};
use {
    jsonrpc_core::{Error, Result},
    jsonrpc_derive::rpc,
    solana_rpc::rpc_subscription_tracker::SubscriptionId,
    solana_rpc_client_api::config::*,
    solana_sdk::signature::Signature,
    std::sync::Arc,
};

#[rpc]
pub trait LiteRpcPubSub {
    // Get notification when signature is verified
    // Accepts signature parameter as base-58 encoded string
    #[rpc(name = "signatureSubscribe")]
    fn signature_subscribe(
        &self,
        signature_str: String,
        config: Option<RpcSignatureSubscribeConfig>,
    ) -> Result<SubscriptionId>;

    // Unsubscribe from signature notification subscription.
    #[rpc(name = "signatureUnsubscribe")]
    fn signature_unsubscribe(&self, id: SubscriptionId) -> Result<bool>;

    // Get notification when slot is encountered
    #[rpc(name = "slotSubscribe")]
    fn slot_subscribe(&self) -> Result<SubscriptionId>;

    // Unsubscribe from slot notification subscription.
    #[rpc(name = "slotUnsubscribe")]
    fn slot_unsubscribe(&self, id: SubscriptionId) -> Result<bool>;
}

#[derive(Clone)]
pub struct SubscriptionParamsWithTime {
    params: SubscriptionParams,
    time: Instant,
}

#[derive(Clone)]
pub struct LiteRpcPubSubImpl {
    subscription_control: Arc<LiteRpcSubsrciptionControl>,
    pub current_subscriptions: DashMap<SubscriptionId, SubscriptionParamsWithTime>,
}

impl LiteRpcPubSubImpl {
    pub fn new(subscription_control: Arc<LiteRpcSubsrciptionControl>) -> Self {
        Self {
            current_subscriptions: DashMap::new(),
            subscription_control,
        }
    }

    fn subscribe(&self, params: SubscriptionParams) -> Result<SubscriptionId> {
        match self
            .subscription_control
            .subscriptions
            .entry(params.clone())
        {
            dashmap::mapref::entry::Entry::Occupied(x) => Ok(*x.get()),
            dashmap::mapref::entry::Entry::Vacant(x) => {
                let new_subscription_id = self
                    .subscription_control
                    .last_subscription_id
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let new_subsription_id = SubscriptionId::from(new_subscription_id);
                x.insert(new_subsription_id);
                self.current_subscriptions.insert(
                    new_subsription_id,
                    SubscriptionParamsWithTime {
                        params,
                        time: Instant::now(),
                    },
                );
                Ok(new_subsription_id)
            }
        }
    }

    fn unsubscribe(&self, id: SubscriptionId) -> Result<bool> {
        match self.current_subscriptions.entry(id) {
            dashmap::mapref::entry::Entry::Occupied(x) => {
                x.remove();
                Ok(true)
            }
            dashmap::mapref::entry::Entry::Vacant(_) => Ok(false),
        }
    }
}

fn param<T: FromStr>(param_str: &str, thing: &str) -> Result<T> {
    param_str.parse::<T>().map_err(|_e| Error {
        code: ErrorCode::InvalidParams,
        message: format!("Invalid Request: Invalid {} provided", thing),
        data: None,
    })
}

impl LiteRpcPubSub for LiteRpcPubSubImpl {
    fn signature_subscribe(
        &self,
        signature_str: String,
        config: Option<RpcSignatureSubscribeConfig>,
    ) -> Result<SubscriptionId> {
        let config = config.unwrap_or_default();
        let params = SignatureSubscriptionParams {
            signature: param::<Signature>(&signature_str, "signature")?,
            commitment: config.commitment.unwrap_or_default(),
            enable_received_notification: false,
        };
        self.subscribe(SubscriptionParams::Signature(params))
    }

    fn signature_unsubscribe(&self, id: SubscriptionId) -> Result<bool> {
        self.unsubscribe(id)
    }

    // Get notification when slot is encountered
    fn slot_subscribe(&self) -> Result<SubscriptionId> {
        self.current_subscriptions.insert(
            SubscriptionId::from(0),
            SubscriptionParamsWithTime {
                params: SubscriptionParams::Slot,
                time: Instant::now(),
            },
        );
        Ok(SubscriptionId::from(0))
    }

    // Unsubscribe from slot notification subscription.
    fn slot_unsubscribe(&self, _id: SubscriptionId) -> Result<bool> {
        self.current_subscriptions.remove(&SubscriptionId::from(0));
        Ok(true)
    }
}

pub struct LitePubSubService {
    thread_hdl: JoinHandle<()>,
}

#[derive(Debug, thiserror::Error)]
enum HandleError {
    #[error("handshake error: {0}")]
    Handshake(#[from] soketto::handshake::Error),
    #[error("connection error: {0}")]
    Connection(#[from] soketto::connection::Error),
    #[error("broadcast queue error: {0}")]
    Broadcast(#[from] tokio::sync::broadcast::error::RecvError),
}

async fn handle_connection(
    socket: TcpStream,
    subscription_control: Arc<LiteRpcSubsrciptionControl>,
    performance_counter: PerformanceCounter,
) -> core::result::Result<(), HandleError> {
    let mut server = Server::new(socket.compat());
    let request = server.receive_request().await?;
    let accept = server::Response::Accept {
        key: request.key(),
        protocol: None,
    };
    server.send_response(&accept).await?;
    let (mut sender, mut receiver) = server.into_builder().finish();

    let mut broadcast_receiver = subscription_control.broadcast_sender.subscribe();
    let mut json_rpc_handler = IoHandler::new();
    let rpc_impl = LiteRpcPubSubImpl::new(subscription_control);
    json_rpc_handler.extend_with(rpc_impl.clone().to_delegate());
    loop {
        let mut data = Vec::new();
        // Extra block for dropping `receive_future`.
        {
            // soketto is not cancel safe, so we have to introduce an inner loop to poll
            // `receive_data` to completion.
            let receive_future = receiver.receive_data(&mut data);
            pin!(receive_future);
            loop {
                select! {
                    result = &mut receive_future => match result {
                        Ok(_) => break,
                        Err(soketto::connection::Error::Closed) => return Ok(()),
                        Err(err) => return Err(err.into()),
                    },
                    result = broadcast_receiver.recv() => {
                        if let Ok(x) = result {
                            if rpc_impl.current_subscriptions.contains_key(&x.subscription_id) {
                                performance_counter.update_confirm_transaction_counter();
                                sender.send_text(&x.json).await?;
                            }
                        }
                    },
                }
            }
        }
        let data_str = String::from_utf8(data).unwrap();
        if let Some(response) = json_rpc_handler.handle_request(data_str.as_str()).await {
            sender.send_text(&response).await?;
        }
    }
}

async fn listen(
    listen_address: SocketAddr,
    subscription_control: Arc<LiteRpcSubsrciptionControl>,
    mut tripwire: Tripwire,
    performance_counter: PerformanceCounter,
) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(&listen_address).await?;
    loop {
        select! {
            result = listener.accept() => match result {
                Ok((socket, addr)) => {
                    let subscription_control = subscription_control.clone();
                    let performance_counter = performance_counter.clone();
                    tokio::spawn(async move {
                        let handle = handle_connection(
                            socket, subscription_control, performance_counter,
                        );
                        match handle.await {
                            Ok(()) => println!("connection closed ({:?})", addr),
                            Err(err) => println!("connection handler error ({:?}): {}", addr, err),
                        }
                    });
                },
                Err(e) => println!("couldn't accept connection: {:?}", e),
            },
            _ = &mut tripwire => return Ok(()),
        }
    }
}

impl LitePubSubService {
    pub fn new(
        subscription_control: Arc<LiteRpcSubsrciptionControl>,
        pubsub_addr: SocketAddr,
        performance_counter: PerformanceCounter,
    ) -> (Trigger, Self) {
        let (trigger, tripwire) = Tripwire::new();

        let thread_hdl = std::thread::Builder::new()
            .name("solRpcPubSub".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(1)
                    .enable_all()
                    .build()
                    .expect("runtime creation failed");
                if let Err(err) = runtime.block_on(listen(
                    pubsub_addr,
                    subscription_control,
                    tripwire,
                    performance_counter,
                )) {
                    println!("pubsub service failed: {}", err);
                };
            })
            .expect("thread spawn failed");

        (trigger, Self { thread_hdl })
    }

    pub fn close(self) -> std::thread::Result<()> {
        self.join()
    }

    pub fn join(self) -> std::thread::Result<()> {
        self.thread_hdl.join()
    }
}
