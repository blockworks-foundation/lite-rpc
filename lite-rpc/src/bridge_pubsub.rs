use prometheus::{opts, register_int_counter, IntCounter};
use solana_lite_rpc_core::{stores::data_cache::DataCache, types::BlockStream};
use std::{str::FromStr, sync::Arc};
use tokio::sync::broadcast::error::RecvError::{Closed, Lagged};

use crate::{
    jsonrpsee_subscrption_handler_sink::JsonRpseeSubscriptionHandlerSink,
    rpc_pubsub::LiteRpcPubSubServer,
};
use jsonrpsee::{
    core::{StringError, SubscriptionResult},
    DisconnectError, PendingSubscriptionSink,
};
use solana_lite_rpc_prioritization_fees::{
    account_prio_service::AccountPrioService,
    rpc_data::{AccountPrioFeesUpdateMessage, PrioFeesUpdateMessage},
    PrioFeesService,
};
use solana_rpc_client_api::{
    config::{
        RpcAccountInfoConfig, RpcBlockSubscribeConfig, RpcBlockSubscribeFilter,
        RpcProgramAccountsConfig, RpcSignatureSubscribeConfig, RpcTransactionLogsConfig,
        RpcTransactionLogsFilter,
    },
    response::{Response as RpcResponse, RpcResponseContext, SlotInfo},
};
use solana_sdk::pubkey::Pubkey;

lazy_static::lazy_static! {
    static ref RPC_SIGNATURE_SUBSCRIBE: IntCounter =
    register_int_counter!(opts!("literpc_rpc_signature_subscribe", "RPC call to subscribe to signature")).unwrap();
    static ref RPC_BLOCK_PRIOFEES_SUBSCRIBE: IntCounter =
    register_int_counter!(opts!("literpc_rpc_block_priofees_subscribe", "RPC call to subscribe to block prio fees")).unwrap();
    static ref RPC_ACCOUNT_PRIOFEES_SUBSCRIBE: IntCounter =
    register_int_counter!(opts!("literpc_rpc_account_priofees_subscribe", "RPC call to subscribe to account prio fees")).unwrap();
}

pub struct LitePubSubBridge {
    data_cache: DataCache,
    prio_fees_service: PrioFeesService,
    account_priofees_service: AccountPrioService,
    block_stream: BlockStream,
}

impl LitePubSubBridge {
    pub fn new(
        data_cache: DataCache,
        prio_fees_service: PrioFeesService,
        account_priofees_service: AccountPrioService,
        block_stream: BlockStream,
    ) -> Self {
        Self {
            data_cache,
            prio_fees_service,
            account_priofees_service,
            block_stream,
        }
    }
}

#[jsonrpsee::core::async_trait]
impl LiteRpcPubSubServer for LitePubSubBridge {
    async fn slot_subscribe(&self, pending: PendingSubscriptionSink) -> SubscriptionResult {
        let sink = pending.accept().await?;
        let mut block_stream = self.block_stream.resubscribe();
        tokio::spawn(async move {
            loop {
                match block_stream.recv().await {
                    Ok(produced_block) => {
                        if !produced_block.commitment_config.is_processed() {
                            continue;
                        }
                        let slot_info = SlotInfo {
                            slot: produced_block.slot,
                            parent: produced_block.parent_slot,
                            root: 0,
                        };
                        let result_message = jsonrpsee::SubscriptionMessage::from_json(&slot_info);

                        match sink.send(result_message.unwrap()).await {
                            Ok(()) => {
                                // success
                                continue;
                            }
                            Err(DisconnectError(_subscription_message)) => {
                                log::debug!("Stopping subscription task on disconnect");
                                return;
                            }
                        };
                    }
                    Err(e) => match e {
                        Closed => {
                            break;
                        }
                        Lagged(_) => {
                            log::error!("Slot subscription stream lagged");
                            continue;
                        }
                    },
                }
            }
        });
        Ok(())
    }

    async fn block_subscribe(
        &self,
        _pending: PendingSubscriptionSink,
        _filter: RpcBlockSubscribeFilter,
        _config: Option<RpcBlockSubscribeConfig>,
    ) -> SubscriptionResult {
        todo!()
    }

    async fn logs_subscribe(
        &self,
        _pending: PendingSubscriptionSink,
        _filter: RpcTransactionLogsFilter,
        _config: Option<RpcTransactionLogsConfig>,
    ) -> SubscriptionResult {
        todo!()
    }

    // WARN: enable_received_notification: bool is ignored
    async fn signature_subscribe(
        &self,
        pending: PendingSubscriptionSink,
        signature: String,
        config: RpcSignatureSubscribeConfig,
    ) -> SubscriptionResult {
        RPC_SIGNATURE_SUBSCRIBE.inc();
        let sink = pending.accept().await?;

        let jsonrpsee_sink = JsonRpseeSubscriptionHandlerSink::new(sink);
        self.data_cache.tx_subs.signature_subscribe(
            signature,
            config.commitment.unwrap_or_default(),
            Arc::new(jsonrpsee_sink),
        );

        Ok(())
    }

    async fn slot_updates_subscribe(
        &self,
        _pending: PendingSubscriptionSink,
    ) -> SubscriptionResult {
        todo!()
    }

    async fn vote_subscribe(&self, _pending: PendingSubscriptionSink) -> SubscriptionResult {
        todo!()
    }

    // use websocket-tungstenite-retry->examples/consume_literpc_priofees.rs to test
    async fn latest_block_priofees_subscribe(
        &self,
        pending: PendingSubscriptionSink,
    ) -> SubscriptionResult {
        let sink = pending.accept().await?;

        let mut block_fees_stream = self.prio_fees_service.block_fees_stream.subscribe();
        tokio::spawn(async move {
            RPC_BLOCK_PRIOFEES_SUBSCRIBE.inc();

            'recv_loop: loop {
                match block_fees_stream.recv().await {
                    Ok(PrioFeesUpdateMessage {
                        slot: confirmation_slot,
                        priofees_stats,
                    }) => {
                        let result_message =
                            jsonrpsee::SubscriptionMessage::from_json(&RpcResponse {
                                context: RpcResponseContext {
                                    slot: confirmation_slot,
                                    api_version: None,
                                },
                                value: priofees_stats,
                            });

                        match sink.send(result_message.unwrap()).await {
                            Ok(()) => {
                                // success
                                continue 'recv_loop;
                            }
                            Err(DisconnectError(_subscription_message)) => {
                                log::debug!("Stopping subscription task on disconnect");
                                return;
                            }
                        };
                    }
                    Err(Lagged(lagged)) => {
                        // this usually happens if there is one "slow receiver", see https://docs.rs/tokio/latest/tokio/sync/broadcast/index.html#lagging
                        log::warn!(
                            "subscriber laggs some({}) priofees update messages - continue",
                            lagged
                        );
                        continue 'recv_loop;
                    }
                    Err(Closed) => {
                        log::error!("failed to receive block, sender closed - aborting");
                        return;
                    }
                }
            }
        });

        Ok(())
    }

    async fn latest_account_priofees_subscribe(
        &self,
        pending: PendingSubscriptionSink,
        account: String,
    ) -> SubscriptionResult {
        let Ok(account) = Pubkey::from_str(&account) else {
            return Err(StringError::from("Invalid account".to_string()));
        };
        let sink = pending.accept().await?;
        let mut account_fees_stream = self
            .account_priofees_service
            .priofees_update_sender
            .subscribe();
        tokio::spawn(async move {
            RPC_BLOCK_PRIOFEES_SUBSCRIBE.inc();

            'recv_loop: loop {
                match account_fees_stream.recv().await {
                    Ok(AccountPrioFeesUpdateMessage {
                        slot,
                        accounts_data,
                    }) => {
                        if let Some(account_data) = accounts_data.get(&account) {
                            let result_message =
                                jsonrpsee::SubscriptionMessage::from_json(&RpcResponse {
                                    context: RpcResponseContext {
                                        slot,
                                        api_version: None,
                                    },
                                    value: account_data,
                                });

                            match sink.send(result_message.unwrap()).await {
                                Ok(()) => {
                                    // success
                                    continue 'recv_loop;
                                }
                                Err(DisconnectError(_subscription_message)) => {
                                    log::debug!("Stopping subscription task on disconnect");
                                    return;
                                }
                            };
                        }
                    }
                    Err(Lagged(lagged)) => {
                        // this usually happens if there is one "slow receiver", see https://docs.rs/tokio/latest/tokio/sync/broadcast/index.html#lagging
                        log::warn!(
                            "subscriber laggs some({}) priofees update messages - continue",
                            lagged
                        );
                        continue 'recv_loop;
                    }
                    Err(Closed) => {
                        log::error!("failed to receive block, sender closed - aborting");
                        return;
                    }
                }
            }
        });

        Ok(())
    }

    async fn account_subscribe(
        &self,
        _pending: PendingSubscriptionSink,
        _account: String,
        _config: Option<RpcAccountInfoConfig>,
    ) -> SubscriptionResult {
        todo!();
    }

    async fn program_subscribe(
        &self,
        _pending: PendingSubscriptionSink,
        _pubkey_str: String,
        _config: Option<RpcProgramAccountsConfig>,
    ) -> SubscriptionResult {
        todo!();
    }
}
