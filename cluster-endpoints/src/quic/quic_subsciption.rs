use itertools::Itertools;
use prometheus::{opts, register_int_gauge, IntGauge};
use quic_geyser_client::non_blocking::client::Client;
use quic_geyser_common::{
    filters::AccountFilter as GeyserAccountFilter,
    filters::AccountFilterType as GeyserAccountFilterType, filters::Filter as QuicGeyserFilter,
    filters::MemcmpFilter as QuicGeyserMemcmpFilter, types::block::Block as QuicGeyserBlock,
    types::block_meta::BlockMeta as QuicGeyserBlockMeta,
    types::connections_parameters::ConnectionParameters,
};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_lite_rpc_core::{
    commitment_utils::Commitment,
    structures::{
        account_data::{Account, AccountData, AccountNotificationMessage, Data},
        account_filter::AccountFilters,
        block_info::BlockInfo,
        produced_block::{ProducedBlock, ProducedBlockInner, TransactionInfo},
        slot_notification::SlotNotification,
    },
    AnyhowJoinHandle,
};
use solana_sdk::{clock::Slot, commitment_config::CommitmentConfig, hash::Hash};
use std::{collections::BTreeMap, str::FromStr, sync::Arc, time::Duration};

use crate::{
    endpoint_stremers::EndpointStreaming,
    message_utils::{get_cu_requested_from_message, get_prioritization_fees_from_message},
    rpc_polling::vote_accounts_and_cluster_info_polling::{poll_cluster_info, poll_vote_accounts},
};

lazy_static::lazy_static! {
    static ref QUIC_GEYSER_NOTIFICATIONS: IntGauge =
       register_int_gauge!(opts!("literpc_quic_geyser_notifications", "Quic geyser notifications")).unwrap();
    static ref QUIC_GEYSER_ACCOUNT_NOTIFICATIONS: IntGauge =
       register_int_gauge!(opts!("literpc_quic_geyser_accounts_notifications", "Quic geyser accounts notification")).unwrap();
    static ref QUIC_GEYSER_SLOT_NOTIFICATIONS: IntGauge =
       register_int_gauge!(opts!("literpc_quic_geyser_slot_notifications", "Quic geyser slot notification")).unwrap();
    static ref QUIC_GEYSER_BLOCKMETA_NOTIFICATIONS: IntGauge =
       register_int_gauge!(opts!("literpc_quic_geyser_blockmeta_notifications", "Quic geyser blockmeta notification")).unwrap();
    static ref QUIC_GEYSER_BLOCK_NOTIFICATIONS: IntGauge =
       register_int_gauge!(opts!("literpc_quic_geyser_block_notifications", "Quic geyser block notification")).unwrap();
}

struct SlotData {
    block_meta: Option<QuicGeyserBlockMeta>,
    block: Option<QuicGeyserBlock>,
    commitment_config: CommitmentConfig,
}

impl Default for SlotData {
    fn default() -> Self {
        Self {
            block_meta: None,
            block: None,
            commitment_config: CommitmentConfig::processed(),
        }
    }
}

fn convert_quic_block_meta(
    block_meta: &QuicGeyserBlockMeta,
    commitment_config: CommitmentConfig,
) -> BlockInfo {
    solana_lite_rpc_core::structures::block_info::BlockInfo {
        slot: block_meta.slot,
        block_height: block_meta.block_height.unwrap_or_default(),
        blockhash: Hash::from_str(&block_meta.blockhash.clone())
            .expect("blockhash should be valid"),
        commitment_config,
        block_time: 0,
    }
}

fn convert_quic_block(
    block: &QuicGeyserBlock,
    commitment_config: CommitmentConfig,
) -> ProducedBlock {
    let transactions = block
        .get_transactions()
        .expect("should return valid transactions");
    ProducedBlock::new(
        ProducedBlockInner {
            transactions: transactions
                .iter()
                .map(|transaction| TransactionInfo {
                    signature: transaction.signatures[0],
                    is_vote: transaction.is_vote,
                    err: transaction.transasction_meta.error.clone(),
                    cu_requested: get_cu_requested_from_message(&transaction.message),
                    prioritization_fees: get_prioritization_fees_from_message(&transaction.message),
                    cu_consumed: transaction.transasction_meta.compute_units_consumed,
                    recent_blockhash: transaction.message.recent_blockhash,
                    message: solana_sdk::message::VersionedMessage::V0(transaction.message.clone()),
                    writable_accounts: transaction
                        .transasction_meta
                        .loaded_addresses
                        .writable
                        .clone(),
                    readable_accounts: transaction
                        .transasction_meta
                        .loaded_addresses
                        .readonly
                        .clone(),
                    address_lookup_tables: transaction.message.address_table_lookups.clone(),
                })
                .collect_vec(),
            leader_id: None,
            blockhash: Hash::from_str(&block.meta.blockhash).expect("blockhash is valid"),
            block_height: block.meta.block_height.unwrap_or_default(),
            slot: block.meta.slot,
            parent_slot: block.meta.parent_slot,
            block_time: 0,
            previous_blockhash: Hash::from_str(&block.meta.parent_blockhash)
                .expect("previous blockhash is valid"),
            rewards: Some(block.meta.rewards.clone()),
        },
        commitment_config,
    )
}

pub async fn create_quic_endpoint(
    rpc_client: Arc<RpcClient>,
    endpoint_url: String,
    accounts_filter: AccountFilters,
) -> anyhow::Result<(Client, EndpointStreaming, Vec<AnyhowJoinHandle>)> {
    let (slot_sx, slot_notifier) = tokio::sync::broadcast::channel(16);
    let (block_sx, blocks_notifier) = tokio::sync::broadcast::channel(16);
    let (blockinfo_sx, blockinfo_notifier) = tokio::sync::broadcast::channel(16);
    let (cluster_info_sx, cluster_info_notifier) = tokio::sync::broadcast::channel(16);
    let (va_sx, vote_account_notifier) = tokio::sync::broadcast::channel(16);
    let mut endpoint_tasks = vec![];

    let cluster_info_polling = poll_cluster_info(rpc_client.clone(), cluster_info_sx);
    endpoint_tasks.push(cluster_info_polling);

    let vote_accounts_polling = poll_vote_accounts(rpc_client.clone(), va_sx);
    endpoint_tasks.push(vote_accounts_polling);

    let (quic_client, mut quic_notification_reciever) =
        Client::new(endpoint_url, ConnectionParameters::default()).await?;
    let mut subscriptions = vec![
        QuicGeyserFilter::Slot,
        QuicGeyserFilter::BlockMeta,
        QuicGeyserFilter::BlockAll,
    ];

    let (processed_account_stream, account_sender) = if !accounts_filter.is_empty() {
        let (sx, rx) = tokio::sync::broadcast::channel(64 * 1024);
        (Some(rx), Some(sx))
    } else {
        (None, None)
    };

    for account_filter in accounts_filter {
        let accounts = Some(account_filter.accounts.iter().copied().collect());
        let geyser_account_filter = GeyserAccountFilter {
            owner: account_filter.program_id,
            accounts,
            filter: None,
        };

        if let Some(filters) = account_filter.filters {
            let mut geyser_account_filter = geyser_account_filter.clone();
            geyser_account_filter.accounts = None;
            for filter in filters {
                let quic_geyser_filter = match filter {
                    solana_lite_rpc_core::structures::account_filter::AccountFilterType::Datasize(size) => GeyserAccountFilterType::Datasize(size),
                    solana_lite_rpc_core::structures::account_filter::AccountFilterType::Memcmp(memcmp) => GeyserAccountFilterType::Memcmp(QuicGeyserMemcmpFilter{
                        offset: memcmp.offset,
                        data: quic_geyser_common::filters::MemcmpFilterData::Bytes(memcmp.bytes()),
                    }),
                };
                geyser_account_filter.filter = Some(quic_geyser_filter);
                subscriptions.push(QuicGeyserFilter::Account(geyser_account_filter.clone()));
            }
        } else {
            // subscribe to all the accounts
            subscriptions.push(QuicGeyserFilter::Account(geyser_account_filter));
        }
    }
    log::info!(
        "Subscribing to quic geyser plugin with {} subscriptions",
        subscriptions.len()
    );
    quic_client.subscribe(subscriptions).await?;

    let quic_geyser_client_task = tokio::spawn(async move {
        let slot_sx = slot_sx;
        let block_sx = block_sx;
        let blockinfo_sx = blockinfo_sx;
        let account_sender = account_sender;
        let mut current_slot = 0;
        let mut map_of_slot_data = BTreeMap::<Slot, SlotData>::new();

        loop {
            let message_or_timeout =
                tokio::time::timeout(Duration::from_secs(10), quic_notification_reciever.recv())
                    .await;
            let Ok(maybe_message) = message_or_timeout else {
                log::error!("quic geyser plugin timedout");
                break;
            };

            let Some(notification_message) = maybe_message else {
                log::error!("quic geyser plugin client broken");
                break;
            };
            QUIC_GEYSER_NOTIFICATIONS.inc();

            match notification_message {
                quic_geyser_common::message::Message::AccountMsg(account_mesage) => {
                    QUIC_GEYSER_ACCOUNT_NOTIFICATIONS.inc();
                    if account_mesage.slot_identifier.slot > current_slot {
                        current_slot = account_mesage.slot_identifier.slot;
                        slot_sx
                            .send(SlotNotification {
                                processed_slot: account_mesage.slot_identifier.slot,
                                estimated_processed_slot: account_mesage.slot_identifier.slot,
                            })
                            .expect("slot notification should be sent");
                    }
                    if let Some(account_sender) = &account_sender {
                        account_sender.send(AccountNotificationMessage{
                            data: AccountData {
                                pubkey: account_mesage.pubkey,
                                account: Arc::new(Account {
                                    lamports: account_mesage.lamports,
                                    data: match account_mesage.compression_type {
                                        quic_geyser_common::compression::CompressionType::None => {
                                            Data::Uncompressed(account_mesage.data)
                                        },
                                        quic_geyser_common::compression::CompressionType::Lz4Fast(_) | quic_geyser_common::compression::CompressionType::Lz4(_) => {
                                            Data::Lz4 { binary: account_mesage.data, len: account_mesage.data_length as usize }
                                        },
                                    },
                                    owner: account_mesage.owner,
                                    executable: account_mesage.executable,
                                    rent_epoch: account_mesage.rent_epoch,
                                }),
                                updated_slot: account_mesage.slot_identifier.slot,
                                write_version: account_mesage.write_version,
                            },
                            commitment: solana_lite_rpc_core::commitment_utils::Commitment::Processed,
                        }).expect("account notification should be sent");
                    }
                }
                quic_geyser_common::message::Message::SlotMsg(slot_message) => {
                    QUIC_GEYSER_SLOT_NOTIFICATIONS.inc();
                    if slot_message.slot > current_slot {
                        current_slot = slot_message.slot;
                        slot_sx
                            .send(SlotNotification {
                                processed_slot: slot_message.slot,
                                estimated_processed_slot: slot_message.slot,
                            })
                            .expect("slot notification should be sent");
                    }

                    let slot_data = match map_of_slot_data.get_mut(&slot_message.slot) {
                        Some(value) => value,
                        None => {
                            map_of_slot_data.insert(slot_message.slot, SlotData::default());
                            map_of_slot_data.get_mut(&slot_message.slot).unwrap()
                        }
                    };
                    let slot_message_commitment = Commitment::from(slot_message.commitment_config);
                    let slot_data_commitment = Commitment::from(slot_data.commitment_config);
                    if slot_data_commitment < slot_message_commitment {
                        // update commitment
                        slot_data.commitment_config = slot_message.commitment_config;
                        if let Some(block_meta) = &slot_data.block_meta {
                            // redispatch meta with new commitment
                            blockinfo_sx
                                .send(convert_quic_block_meta(
                                    block_meta,
                                    slot_message.commitment_config,
                                ))
                                .expect("block meta notification should be sent");
                        }

                        if let Some(block) = &slot_data.block {
                            // redispatch meta with new commitment
                            block_sx
                                .send(convert_quic_block(block, slot_message.commitment_config))
                                .expect("block meta notification should be sent");
                        }
                    }

                    if slot_message.commitment_config == CommitmentConfig::finalized() {
                        // remove old data
                        while let Some((key, value)) = map_of_slot_data.first_key_value() {
                            // all old slots that already have been notified
                            if (*key <= slot_message.slot
                                && value.block.is_some()
                                && value.block_meta.is_some()) // slot already notified for finalized
                                || slot_message.slot.saturating_sub(*key) > 150
                            // slot too old
                            {
                                map_of_slot_data.pop_first();
                            } else {
                                break;
                            }
                        }
                    }
                }
                quic_geyser_common::message::Message::BlockMetaMsg(block_meta) => {
                    QUIC_GEYSER_BLOCKMETA_NOTIFICATIONS.inc();
                    if block_meta.slot > current_slot {
                        current_slot = block_meta.slot;
                        slot_sx
                            .send(SlotNotification {
                                processed_slot: block_meta.slot,
                                estimated_processed_slot: block_meta.slot,
                            })
                            .expect("slot notification should be sent");
                    }

                    let slot_data = match map_of_slot_data.get_mut(&block_meta.slot) {
                        Some(value) => value,
                        None => {
                            map_of_slot_data.insert(block_meta.slot, SlotData::default());
                            map_of_slot_data.get_mut(&block_meta.slot).unwrap()
                        }
                    };
                    // probably the slot was already in confrimed or finalized commitment
                    blockinfo_sx
                        .send(convert_quic_block_meta(
                            &block_meta,
                            slot_data.commitment_config,
                        ))
                        .expect("block meta notification should be sent");

                    slot_data.block_meta = Some(block_meta);
                }
                quic_geyser_common::message::Message::BlockMsg(block) => {
                    QUIC_GEYSER_BLOCK_NOTIFICATIONS.inc();
                    if block.meta.slot > current_slot {
                        current_slot = block.meta.slot;
                        slot_sx
                            .send(SlotNotification {
                                processed_slot: block.meta.slot,
                                estimated_processed_slot: block.meta.slot,
                            })
                            .expect("slot notification should be sent");
                    }

                    let slot_data = match map_of_slot_data.get_mut(&block.meta.slot) {
                        Some(value) => value,
                        None => {
                            map_of_slot_data.insert(block.meta.slot, SlotData::default());
                            map_of_slot_data.get_mut(&block.meta.slot).unwrap()
                        }
                    };
                    block_sx
                        .send(convert_quic_block(&block, slot_data.commitment_config))
                        .expect("block notification should be sent");
                    slot_data.block = Some(block);
                }
                _ => {
                    log::error!("quic geyser send notification which is not supported");
                }
            }
        }

        log::error!("quic geyser plugin exited because the client connection failed or connection timed out");
        panic!("quic geyser plugin exit");
    });
    endpoint_tasks.push(quic_geyser_client_task);

    let streamers = EndpointStreaming {
        blocks_notifier,
        blockinfo_notifier,
        slot_notifier,
        cluster_info_notifier,
        vote_account_notifier,
        processed_account_stream,
    };

    Ok((quic_client, streamers, endpoint_tasks))
}
