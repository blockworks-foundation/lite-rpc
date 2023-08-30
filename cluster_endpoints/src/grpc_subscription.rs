use crate::{
    endpoint_stremers::EndpointStreaming,
    rpc_polling::vote_accounts_and_cluster_info_polling::poll_vote_accounts_and_cluster_info,
};
use anyhow::{bail, Context};
use futures::StreamExt;
use itertools::Itertools;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_lite_rpc_core::{
    structures::{
        processed_block::ProcessedBlock, slot_notification::SlotNotification,
        transaction_info::TransactionInfo,
    },
    AnyhowJoinHandle,
};
use solana_sdk::{
    borsh::try_from_slice_unchecked,
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
use tokio::sync::broadcast::Sender;
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::prelude::{
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequestFilterBlocks,
    SubscribeRequestFilterSlots, SubscribeUpdateBlock,
};

fn process_block(
    block: SubscribeUpdateBlock,
    commitment_config: CommitmentConfig,
) -> ProcessedBlock {
    let txs: Vec<TransactionInfo> = block
        .transactions
        .into_iter()
        .filter_map(|tx| {
            let Some(meta) = tx.meta else {
                return None;
            };

            let Some(transaction) = tx.transaction else {
                return None;
            };

            let Some(message) = transaction.message else {
                return None;
            };

            let Some(header) = message.header else {
                return None;
            };

            let signatures = transaction
                .signatures
                .into_iter()
                .map(|sig| Signature::new(&sig))
                .collect_vec();

            let err = meta.err.map(|x| {
                bincode::deserialize::<TransactionError>(&x.err)
                    .expect("TransactionError should be deserialized")
            });
            let status = match &err {
                Some(e) => Err(e.clone()),
                None => Ok(()),
            };
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

            let legacy_compute_budget = message.instructions().iter().find_map(|i| {
                if i.program_id(message.static_account_keys())
                    .eq(&compute_budget::id())
                {
                    if let Ok(ComputeBudgetInstruction::RequestUnitsDeprecated {
                        units,
                        additional_fee,
                    }) = try_from_slice_unchecked(i.data.as_slice())
                    {
                        return Some((units, additional_fee));
                    }
                }
                None
            });

            let mut cu_requested = message.instructions().iter().find_map(|i| {
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
            });

            let mut prioritization_fees = message.instructions().iter().find_map(|i| {
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
            });

            if let Some((units, additional_fee)) = legacy_compute_budget {
                cu_requested = Some(units);
                if additional_fee > 0 {
                    prioritization_fees = Some(((units * 1000) / additional_fee).into())
                }
            };

            Some(TransactionInfo {
                signature: signature.to_string(),
                err,
                status,
                cu_requested,
                prioritization_fees,
                cu_consumed: compute_units_consumed,
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
                // Warn: idk how to convert string to option u8
                commission: None,
            })
            .collect_vec()
    });

    let leader_id = if let Some(rewards) = rewards {
        rewards
            .iter()
            .find(|reward| Some(RewardType::Fee) == reward.reward_type)
            .map(|leader_reward| leader_reward.pubkey.clone())
    } else {
        None
    };

    ProcessedBlock {
        txs,
        block_height: block
            .block_height
            .map(|block_height| block_height.block_height)
            .unwrap(),
        block_time: block.block_time.map(|time| time.timestamp).unwrap() as u64,
        blockhash: block.blockhash,
        commitment_config,
        leader_id,
        parent_slot: block.parent_slot,
        slot: block.slot,
    }
}

pub fn create_block_processing_task(
    grpc_addr: String,
    block_sx: Sender<ProcessedBlock>,
    commitment_level: CommitmentLevel,
) -> AnyhowJoinHandle {
    let mut blocks_subs = HashMap::new();
    blocks_subs.insert(
        "client".to_string(),
        SubscribeRequestFilterBlocks {
            account_include: Default::default(),
            include_transactions: Some(true),
            include_accounts: Some(false),
            include_entries: Some(false),
        },
    );

    let commitment_config = match commitment_level {
        CommitmentLevel::Confirmed => CommitmentConfig::confirmed(),
        CommitmentLevel::Finalized => CommitmentConfig::finalized(),
        CommitmentLevel::Processed => CommitmentConfig::processed(),
    };

    tokio::spawn(async move {
        // connect to grpc
        let mut client = GeyserGrpcClient::connect(grpc_addr, None::<&'static str>, None)?;
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
            )
            .await?;

        while let Some(message) = stream.next().await {
            let message = message?;

            let Some(update) = message.update_oneof else {
                continue;
            };

            match update {
                UpdateOneof::Block(block) => {
                    let block = process_block(block, commitment_config);
                    block_sx
                        .send(block)
                        .context("Grpc failed to send a block")?;
                }
                UpdateOneof::Ping(_) => {
                    log::trace!("GRPC Ping");
                }
                k => {
                    bail!("Unexpected update: {k:?}");
                }
            };
        }
        bail!("gyser slot stream ended");
    })
}

pub async fn create_grpc_subscription(
    rpc_client: Arc<RpcClient>,
    grpc_addr: String,
    expected_grpc_version: String,
) -> anyhow::Result<(EndpointStreaming, Vec<AnyhowJoinHandle>)> {
    let (slot_sx, slot_notifier) = tokio::sync::broadcast::channel(10);
    let (block_sx, blocks_notifier) = tokio::sync::broadcast::channel(10);
    let (cluster_info_sx, cluster_info_notifier) = tokio::sync::broadcast::channel(10);
    let (va_sx, vote_account_notifier) = tokio::sync::broadcast::channel(10);

    let mut slots = HashMap::new();
    slots.insert("client".to_string(), SubscribeRequestFilterSlots {});

    let grpc_addr_cp = grpc_addr.clone();
    let slot_task: AnyhowJoinHandle = tokio::spawn(async move {
        // connect to grpc
        let mut client = GeyserGrpcClient::connect(grpc_addr_cp, None::<&'static str>, None)?;

        let version = client.get_version().await?.version;
        if version != expected_grpc_version {
            log::warn!(
                "Expected version {:?}, got {:?}",
                expected_grpc_version,
                version
            );
        }
        let mut stream = client
            .subscribe_once(
                slots,
                Default::default(),
                HashMap::new(),
                Default::default(),
                HashMap::new(),
                Default::default(),
                Some(CommitmentLevel::Processed),
                Default::default(),
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
                        .send(SlotNotification {
                            estimated_processed_slot: slot.slot,
                            processed_slot: slot.slot,
                        })
                        .context("Error sending slot notification")?;
                }
                UpdateOneof::Ping(_) => {
                    log::trace!("GRPC Ping");
                }
                k => {
                    bail!("Unexpected update: {k:?}");
                }
            };
        }
        bail!("gyser slot stream ended");
    });

    let block_confirmed_task: AnyhowJoinHandle = create_block_processing_task(
        grpc_addr.clone(),
        block_sx.clone(),
        CommitmentLevel::Confirmed,
    );
    let block_finalized_task: AnyhowJoinHandle =
        create_block_processing_task(grpc_addr, block_sx, CommitmentLevel::Finalized);

    let cluster_info_polling =
        poll_vote_accounts_and_cluster_info(rpc_client, cluster_info_sx, va_sx);

    let streamers = EndpointStreaming {
        blocks_notifier,
        slot_notifier,
        cluster_info_notifier,
        vote_account_notifier,
    };

    let endpoint_tasks = vec![
        slot_task,
        block_confirmed_task,
        block_finalized_task,
        cluster_info_polling,
    ];
    Ok((streamers, endpoint_tasks))
}
