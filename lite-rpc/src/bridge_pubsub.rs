use prometheus::{opts, register_int_counter, IntCounter};
use solana_lite_rpc_accounts::account_service::AccountService;
use solana_lite_rpc_core::{
    commitment_utils::Commitment, stores::data_cache::DataCache,
    structures::account_data::AccountNotificationMessage, types::BlockStream,
};
use std::{str::FromStr, sync::Arc, time::Duration};
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
    response::{Response as RpcResponse, RpcKeyedAccount, RpcResponseContext, SlotInfo},
};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;

lazy_static::lazy_static! {
    static ref RPC_SIGNATURE_SUBSCRIBE: IntCounter =
    register_int_counter!(opts!("literpc_rpc_signature_subscribe", "RPC call to subscribe to signature")).unwrap();
    static ref RPC_BLOCK_PRIOFEES_SUBSCRIBE: IntCounter =
    register_int_counter!(opts!("literpc_rpc_block_priofees_subscribe", "RPC call to subscribe to block prio fees")).unwrap();
    static ref RPC_ACCOUNT_PRIOFEES_SUBSCRIBE: IntCounter =
    register_int_counter!(opts!("literpc_rpc_account_priofees_subscribe", "RPC call to subscribe to account prio fees")).unwrap();
    static ref RPC_ACCOUNT_SUBSCRIBE: IntCounter =
    register_int_counter!(opts!("literpc_rpc_account_subscribe", "RPC call to subscribe to account")).unwrap();
    static ref RPC_PROGRAM_ACCOUNT_SUBSCRIBE: IntCounter =
    register_int_counter!(opts!("literpc_rpc_program_account_subscribe", "RPC call to subscribe to program account")).unwrap();
}

pub struct LitePubSubBridge {
    data_cache: DataCache,
    prio_fees_service: PrioFeesService,
    account_priofees_service: AccountPrioService,
    block_stream: BlockStream,
    accounts_service: Option<AccountService>,
}

impl LitePubSubBridge {
    pub fn new(
        data_cache: DataCache,
        prio_fees_service: PrioFeesService,
        account_priofees_service: AccountPrioService,
        block_stream: BlockStream,
        accounts_service: Option<AccountService>,
    ) -> Self {
        Self {
            data_cache,
            prio_fees_service,
            account_priofees_service,
            block_stream,
            accounts_service,
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
        signature: Signature,
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
                match tokio::time::timeout(Duration::from_secs(1), account_fees_stream.recv()).await
                {
                    Ok(Ok(AccountPrioFeesUpdateMessage {
                        slot,
                        accounts_data,
                    })) => {
                        if let Some(account_stats) = accounts_data.get(&account) {
                            let result_message =
                                jsonrpsee::SubscriptionMessage::from_json(&RpcResponse {
                                    context: RpcResponseContext {
                                        slot,
                                        api_version: None,
                                    },
                                    value: account_stats,
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
                    Ok(Err(Lagged(lagged))) => {
                        // this usually happens if there is one "slow receiver", see https://docs.rs/tokio/latest/tokio/sync/broadcast/index.html#lagging
                        log::warn!(
                            "subscriber laggs some({}) priofees update messages - continue",
                            lagged
                        );
                        continue 'recv_loop;
                    }
                    Ok(Err(Closed)) => {
                        log::error!("failed to receive block, sender closed - aborting");
                        return;
                    }
                    Err(_elapsed) => {
                        // check if subscription is closed
                        if sink.is_closed() {
                            break 'recv_loop;
                        }
                    }
                }
            }
        });

        Ok(())
    }

    async fn account_subscribe(
        &self,
        pending: PendingSubscriptionSink,
        account: String,
        config: Option<RpcAccountInfoConfig>,
    ) -> SubscriptionResult {
        let Ok(account) = Pubkey::from_str(&account) else {
            return Err(StringError::from("Invalid account".to_string()));
        };

        let Some(accounts_service) = &self.accounts_service else {
            return Err(StringError::from(
                "Accounts service not configured".to_string(),
            ));
        };

        let account_config = config.clone().unwrap_or_default();
        let config_commitment = account_config.commitment.unwrap_or_default();
        let min_context_slot = account_config.min_context_slot.unwrap_or_default();

        let sink = pending.accept().await?;
        let mut accounts_stream = accounts_service.account_notification_sender.subscribe();

        tokio::spawn(async move {
            RPC_ACCOUNT_SUBSCRIBE.inc();

            loop {
                match tokio::time::timeout(Duration::from_secs(1), accounts_stream.recv()).await {
                    Ok(Ok(AccountNotificationMessage {
                        data, commitment, ..
                    })) => {
                        if sink.is_closed() {
                            // sink is already closed
                            return;
                        }

                        if data.pubkey != account {
                            // notification is different account
                            continue;
                        }
                        // check config
                        // check if commitment match
                        if Commitment::from(config_commitment) != commitment {
                            continue;
                        }
                        // check for min context slot
                        if data.updated_slot < min_context_slot {
                            continue;
                        }

                        let result_message =
                            jsonrpsee::SubscriptionMessage::from_json(&RpcResponse {
                                context: RpcResponseContext {
                                    slot: data.updated_slot,
                                    api_version: None,
                                },
                                value: AccountService::convert_account_data_to_ui_account(
                                    &data,
                                    config.clone(),
                                ),
                            });

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
                    Ok(Err(Lagged(lagged))) => {
                        // this usually happens if there is one "slow receiver", see https://docs.rs/tokio/latest/tokio/sync/broadcast/index.html#lagging
                        log::warn!(
                            "subscriber laggs some({}) accounts messages - continue",
                            lagged
                        );
                        continue;
                    }
                    Ok(Err(Closed)) => {
                        log::error!(
                            "failed to receive account notifications, sender closed - aborting"
                        );
                        return;
                    }
                    Err(_elapsed) => {
                        // on timeout check if sink is still open
                        if sink.is_closed() {
                            break;
                        }
                    }
                }
            }
        });

        Ok(())
    }

    async fn program_subscribe(
        &self,
        pending: PendingSubscriptionSink,
        pubkey_str: String,
        config: Option<RpcProgramAccountsConfig>,
    ) -> SubscriptionResult {
        let Ok(program_id) = Pubkey::from_str(&pubkey_str) else {
            return Err(StringError::from("Invalid account".to_string()));
        };

        let Some(accounts_service) = &self.accounts_service else {
            return Err(StringError::from(
                "Accounts service not configured".to_string(),
            ));
        };
        let sink = pending.accept().await?;
        let mut accounts_stream = accounts_service.account_notification_sender.subscribe();

        let program_config = config.clone().unwrap_or_default();
        let config_commitment = program_config.account_config.commitment.unwrap_or_default();
        let min_context_slot = program_config
            .account_config
            .min_context_slot
            .unwrap_or_default();

        tokio::spawn(async move {
            RPC_ACCOUNT_SUBSCRIBE.inc();

            loop {
                match tokio::time::timeout(Duration::from_secs(1), accounts_stream.recv()).await {
                    Ok(Ok(AccountNotificationMessage {
                        data, commitment, ..
                    })) => {
                        if sink.is_closed() {
                            // sink is already closed
                            return;
                        }
                        if data.account.owner != program_id {
                            // wrong program owner
                            continue;
                        }
                        // check config
                        // check if commitment match
                        if Commitment::from(config_commitment) != commitment {
                            continue;
                        }
                        // check for min context slot
                        if data.updated_slot < min_context_slot {
                            continue;
                        }
                        // check filters
                        if let Some(filters) = &program_config.filters {
                            if filters.iter().any(|filter| !data.allows(filter)) {
                                // filters not stasfied
                                continue;
                            }
                        }

                        let value = RpcKeyedAccount {
                            pubkey: data.pubkey.to_string(),
                            account: AccountService::convert_account_data_to_ui_account(
                                &data,
                                config.clone().map(|x| x.account_config),
                            ),
                        };

                        let result_message =
                            jsonrpsee::SubscriptionMessage::from_json(&RpcResponse {
                                context: RpcResponseContext {
                                    slot: data.updated_slot,
                                    api_version: None,
                                },
                                value,
                            });
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
                    Ok(Err(Lagged(lagged))) => {
                        // this usually happens if there is one "slow receiver", see https://docs.rs/tokio/latest/tokio/sync/broadcast/index.html#lagging
                        log::warn!(
                            "subscriber laggs some({}) program accounts messages - continue",
                            lagged
                        );
                        continue;
                    }
                    Ok(Err(Closed)) => {
                        log::error!(
                            "failed to receive account notifications, sender closed - aborting"
                        );
                        return;
                    }
                    Err(_elapsed) => {
                        // on timeout check if sink is still open
                        if sink.is_closed() {
                            break;
                        }
                    }
                }
            }
        });
        Ok(())
    }
}
