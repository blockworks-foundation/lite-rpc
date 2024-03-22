use crate::grpc_multiplex::{
    create_grpc_multiplex_blocks_subscription, create_grpc_multiplex_slots_subscription,
};
use crate::{
    endpoint_stremers::EndpointStreaming,
    rpc_polling::vote_accounts_and_cluster_info_polling::poll_vote_accounts_and_cluster_info,
};
use anyhow::Context;
use futures::StreamExt;
use geyser_grpc_connector::grpc_subscription_autoreconnect::GrpcSourceConfig;
use itertools::Itertools;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_lite_rpc_core::{
    encoding::BASE64,
    structures::produced_block::{ProducedBlock, TransactionInfo},
    AnyhowJoinHandle,
};
use solana_sdk::program_utils::limited_deserialize;
use solana_sdk::vote::instruction::VoteInstruction;
use solana_sdk::{
    borsh0_10::try_from_slice_unchecked,
    commitment_config::CommitmentConfig,
    compute_budget::{self, ComputeBudgetInstruction},
    hash::Hash,
    instruction::CompiledInstruction,
    message::{
        v0::{self, MessageAddressTableLookup},
        MessageHeader, VersionedMessage,
    },
    pubkey::Pubkey,
    signature::Signature,
    transaction::TransactionError,
};
use solana_transaction_status::{Reward, RewardType};
use std::{collections::HashMap, sync::Arc};
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::geyser::{SubscribeRequestFilterSlots, SubscribeUpdateSlot};

use yellowstone_grpc_proto::prelude::{
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequestFilterBlocks,
    SubscribeUpdateBlock,
};

/// grpc version of ProducedBlock mapping
pub fn from_grpc_block_update(
    block: SubscribeUpdateBlock,
    commitment_config: CommitmentConfig,
) -> ProducedBlock {
    let txs: Vec<TransactionInfo> = block
        .transactions
        .into_iter()
        .filter_map(|tx| {
            let meta = tx.meta?;

            let transaction = tx.transaction?;

            let message = transaction.message?;

            let header = message.header?;

            let signatures = transaction
                .signatures
                .into_iter()
                .filter_map(|sig| match Signature::try_from(sig) {
                    Ok(sig) => Some(sig),
                    Err(_) => {
                        log::warn!(
                            "Failed to read signature from transaction in block {} - skipping",
                            block.blockhash
                        );
                        None
                    }
                })
                .collect_vec();

            let err = meta.err.map(|x| {
                bincode::deserialize::<TransactionError>(&x.err)
                    .expect("TransactionError should be deserialized")
            });

            let signature = signatures[0];
            let compute_units_consumed = meta.compute_units_consumed;

            let message = VersionedMessage::V0(v0::Message {
                header: MessageHeader {
                    num_required_signatures: header.num_required_signatures as u8,
                    num_readonly_signed_accounts: header.num_readonly_signed_accounts as u8,
                    num_readonly_unsigned_accounts: header.num_readonly_unsigned_accounts as u8,
                },
                account_keys: message
                    .account_keys
                    .into_iter()
                    .map(|key| {
                        let bytes: [u8; 32] =
                            key.try_into().unwrap_or(Pubkey::default().to_bytes());
                        Pubkey::new_from_array(bytes)
                    })
                    .collect(),
                recent_blockhash: Hash::new(&message.recent_blockhash),
                instructions: message
                    .instructions
                    .into_iter()
                    .map(|ix| CompiledInstruction {
                        program_id_index: ix.program_id_index as u8,
                        accounts: ix.accounts,
                        data: ix.data,
                    })
                    .collect(),
                address_table_lookups: message
                    .address_table_lookups
                    .into_iter()
                    .map(|table| {
                        let bytes: [u8; 32] = table
                            .account_key
                            .try_into()
                            .unwrap_or(Pubkey::default().to_bytes());
                        MessageAddressTableLookup {
                            account_key: Pubkey::new_from_array(bytes),
                            writable_indexes: table.writable_indexes,
                            readonly_indexes: table.readonly_indexes,
                        }
                    })
                    .collect(),
            });

            let legacy_compute_budget: Option<(u32, Option<u64>)> =
                message.instructions().iter().find_map(|i| {
                    if i.program_id(message.static_account_keys())
                        .eq(&compute_budget::id())
                    {
                        if let Ok(ComputeBudgetInstruction::RequestUnitsDeprecated {
                            units,
                            additional_fee,
                        }) = try_from_slice_unchecked(i.data.as_slice())
                        {
                            if additional_fee > 0 {
                                return Some((
                                    units,
                                    Some(((units * 1000) / additional_fee) as u64),
                                ));
                            } else {
                                return Some((units, None));
                            }
                        }
                    }
                    None
                });

            let legacy_cu_requested = legacy_compute_budget.map(|x| x.0);
            let legacy_prioritization_fees = legacy_compute_budget.map(|x| x.1).unwrap_or(None);

            let cu_requested = message
                .instructions()
                .iter()
                .find_map(|i| {
                    if i.program_id(message.static_account_keys())
                        .eq(&compute_budget::id())
                    {
                        if let Ok(ComputeBudgetInstruction::SetComputeUnitLimit(limit)) =
                            try_from_slice_unchecked(i.data.as_slice())
                        {
                            return Some(limit);
                        }
                    }
                    None
                })
                .or(legacy_cu_requested);

            let prioritization_fees = message
                .instructions()
                .iter()
                .find_map(|i| {
                    if i.program_id(message.static_account_keys())
                        .eq(&compute_budget::id())
                    {
                        if let Ok(ComputeBudgetInstruction::SetComputeUnitPrice(price)) =
                            try_from_slice_unchecked(i.data.as_slice())
                        {
                            return Some(price);
                        }
                    }

                    None
                })
                .or(legacy_prioritization_fees);

            let is_vote_transaction = message.instructions().iter().any(|i| {
                i.program_id(message.static_account_keys())
                    .eq(&solana_sdk::vote::program::id())
                    && limited_deserialize::<VoteInstruction>(&i.data)
                        .map(|vi| vi.is_simple_vote())
                        .unwrap_or(false)
            });

            Some(TransactionInfo {
                signature: signature.to_string(),
                is_vote: is_vote_transaction,
                err,
                cu_requested,
                prioritization_fees,
                cu_consumed: compute_units_consumed,
                recent_blockhash: message.recent_blockhash().to_string(),
                message: BASE64.encode(message.serialize()),
            })
        })
        .collect();

    let rewards = block.rewards.map(|rewards| {
        rewards
            .rewards
            .into_iter()
            .map(|reward| Reward {
                pubkey: reward.pubkey.to_owned(),
                lamports: reward.lamports,
                post_balance: reward.post_balance,
                reward_type: match reward.reward_type() {
                    yellowstone_grpc_proto::prelude::RewardType::Unspecified => None,
                    yellowstone_grpc_proto::prelude::RewardType::Fee => Some(RewardType::Fee),
                    yellowstone_grpc_proto::prelude::RewardType::Rent => Some(RewardType::Rent),
                    yellowstone_grpc_proto::prelude::RewardType::Staking => {
                        Some(RewardType::Staking)
                    }
                    yellowstone_grpc_proto::prelude::RewardType::Voting => Some(RewardType::Voting),
                },
                commission: None,
            })
            .collect_vec()
    });

    let leader_id = if let Some(rewards) = &rewards {
        rewards
            .iter()
            .find(|reward| Some(RewardType::Fee) == reward.reward_type)
            .map(|leader_reward| leader_reward.pubkey.clone())
    } else {
        None
    };

    ProducedBlock {
        transactions: txs,
        block_height: block
            .block_height
            .map(|block_height| block_height.block_height)
            .unwrap(),
        block_time: block.block_time.map(|time| time.timestamp).unwrap() as u64,
        blockhash: block.blockhash,
        previous_blockhash: block.parent_blockhash,
        commitment_config,
        leader_id,
        parent_slot: block.parent_slot,
        slot: block.slot,
        rewards,
    }
}

pub fn create_block_processing_task(
    grpc_addr: String,
    grpc_x_token: Option<String>,
    block_sx: async_channel::Sender<SubscribeUpdateBlock>,
    commitment_level: CommitmentLevel,
) -> AnyhowJoinHandle {
    tokio::spawn(async move {
        loop {
            let mut blocks_subs = HashMap::new();
            blocks_subs.insert(
                "block_client".to_string(),
                SubscribeRequestFilterBlocks {
                    account_include: Default::default(),
                    include_transactions: Some(true),
                    include_accounts: Some(false),
                    include_entries: Some(false),
                },
            );

            // connect to grpc
            let mut client =
                GeyserGrpcClient::connect(grpc_addr.clone(), grpc_x_token.clone(), None)?;
            let mut stream = client
                .subscribe_once(
                    HashMap::new(),
                    Default::default(),
                    HashMap::new(),
                    Default::default(),
                    blocks_subs,
                    Default::default(),
                    Some(commitment_level),
                    Default::default(),
                    None,
                )
                .await?;

            while let Some(message) = stream.next().await {
                let message = message?;

                let Some(update) = message.update_oneof else {
                    continue;
                };

                match update {
                    UpdateOneof::Block(block) => {
                        log::trace!("received block, hash: {} slot: {}", block.blockhash, block.slot);
                        block_sx
                            .send(block)
                            .await
                            .context("Problem sending on block channel")?;
                    }
                    UpdateOneof::Ping(_) => {
                        log::trace!("GRPC Ping");
                    }
                    _ => {
                        log::trace!("unknown GRPC notification");
                    }
                };
            }
            log::error!("Grpc block subscription broken (resubscribing)");
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    })
}

pub fn create_slot_stream_task(
    grpc_addr: String,
    grpc_x_token: Option<String>,
    slot_sx: async_channel::Sender<SubscribeUpdateSlot>,
    commitment_level: CommitmentLevel,
) -> AnyhowJoinHandle {
    tokio::spawn(async move {
        loop {
            let mut slots = HashMap::new();
            slots.insert(
                "client_slot".to_string(),
                SubscribeRequestFilterSlots {
                    filter_by_commitment: Some(true),
                },
            );

            // connect to grpc
            let mut client =
                GeyserGrpcClient::connect(grpc_addr.clone(), grpc_x_token.clone(), None)?;
            let mut stream = client
                .subscribe_once(
                    slots,
                    Default::default(),
                    HashMap::new(),
                    Default::default(),
                    HashMap::new(),
                    Default::default(),
                    Some(commitment_level),
                    Default::default(),
                    None,
                )
                .await?;

            while let Some(message) = stream.next().await {
                let message = message?;

                let Some(update) = message.update_oneof else {
                    continue;
                };

                match update {
                    UpdateOneof::Slot(slot) => {
                        slot_sx
                            .send(slot)
                            .await
                            .context("Problem sending on block channel")?;
                    }
                    UpdateOneof::Ping(_) => {
                        log::trace!("GRPC Ping");
                    }
                    _ => {
                        log::trace!("unknown GRPC notification");
                    }
                };
            }
            log::error!("Grpc block subscription broken (resubscribing)");
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    })
}

pub fn create_grpc_subscription(
    rpc_client: Arc<RpcClient>,
    grpc_sources: Vec<GrpcSourceConfig>,
) -> anyhow::Result<(EndpointStreaming, Vec<AnyhowJoinHandle>)> {
    let (cluster_info_sx, cluster_info_notifier) = tokio::sync::broadcast::channel(10);
    let (va_sx, vote_account_notifier) = tokio::sync::broadcast::channel(10);

    // processed slot is required to keep up with leader schedule
    let (slot_multiplex_channel, jh_multiplex_slotstream) =
        create_grpc_multiplex_slots_subscription(grpc_sources.clone());

    let (block_multiplex_channel, jh_multiplex_blockstream) =
        create_grpc_multiplex_blocks_subscription(grpc_sources);

    let cluster_info_polling =
        poll_vote_accounts_and_cluster_info(rpc_client, cluster_info_sx, va_sx);

    let streamers = EndpointStreaming {
        blocks_notifier: block_multiplex_channel,
        slot_notifier: slot_multiplex_channel,
        cluster_info_notifier,
        vote_account_notifier,
    };

    let endpoint_tasks = vec![
        jh_multiplex_slotstream,
        jh_multiplex_blockstream,
        cluster_info_polling,
    ];
    Ok((streamers, endpoint_tasks))
}
