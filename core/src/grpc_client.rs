use std::collections::HashMap;

use anyhow::{bail, Context};

use bytes::Bytes;
use futures::StreamExt;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    hash::Hash,
    instruction::CompiledInstruction,
    message::{
        v0::{self, MessageAddressTableLookup},
        MessageHeader, VersionedMessage,
    },
    pubkey::Pubkey,
    signature::Signature,
    slot_history::Slot,
    transaction::VersionedTransaction,
};
use solana_transaction_status::{Reward, RewardType};
use tokio::sync::mpsc::UnboundedSender;
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::{
    prelude::{
        subscribe_update::UpdateOneof, CommitmentLevel as GrpcCommitmentLevel,
        SubscribeRequestFilterBlocks, SubscribeRequestFilterSlots,
        SubscribeRequestFilterTransactions, SubscribeUpdateBlock, SubscribeUpdateTransaction,
    },
    tonic::service::Interceptor,
};

use crate::processed_block::{
    FilteredBlock, FilteredTransaction, FilteredTransactionMeta, ProcessedBlock,
};

pub const GRPC_VERSION: &str = "1.16.1";
pub const DEFAULT_GRPC_ADDR: &str = "http://127.0.0.1:10000";

pub struct GrpcClient;

impl GrpcClient {
    pub async fn create_client(
        addr: impl Into<Bytes>,
    ) -> anyhow::Result<GeyserGrpcClient<impl Interceptor>> {
        let mut client = GeyserGrpcClient::connect(addr, None::<&'static str>, None)?;

        let version = client.get_version().await?.version;

        if version != GRPC_VERSION {
            log::warn!("Expected version {:?}, got {:?}", GRPC_VERSION, version);
        }

        Ok(client)
    }

    pub async fn subscribe(
        mut client: GeyserGrpcClient<impl Interceptor>,
        slots_sender: Option<UnboundedSender<Slot>>,
        tx_sender: Option<UnboundedSender<SubscribeUpdateTransaction>>,
        block_sender: Option<UnboundedSender<ProcessedBlock>>,
        commitment_config: CommitmentConfig,
    ) -> anyhow::Result<()> {
        let commitment = if commitment_config.is_confirmed() {
            GrpcCommitmentLevel::Confirmed
        } else if commitment_config.is_finalized() {
            GrpcCommitmentLevel::Finalized
        } else {
            GrpcCommitmentLevel::Processed
        };
        // check if atomic_slot is None
        // subscribe to slot only if atomic_slot is Some
        let mut slots = HashMap::new();
        if slots_sender.is_some() {
            slots.insert("client".to_string(), SubscribeRequestFilterSlots {});
        }

        // subscribe to transactions
        let mut txs = HashMap::new();
        if tx_sender.is_some() {
            txs.insert(
                "client".to_string(),
                SubscribeRequestFilterTransactions {
                    vote: None,
                    failed: Some(true), // get failed transactions as well
                    signature: None,
                    account_include: Default::default(),
                    account_exclude: Default::default(),
                    account_required: Default::default(),
                },
            );
        }

        let mut blocks = HashMap::new();
        if block_sender.is_some() {
            blocks.insert(
                "client".to_string(),
                SubscribeRequestFilterBlocks {
                    account_include: Default::default(),
                    include_transactions: Some(true),
                    include_accounts: Some(false),
                    include_entries: Some(false),
                },
            );
        }

        let mut stream = client
            .subscribe_once(
                slots,
                Default::default(),
                txs,
                Default::default(),
                blocks,
                Default::default(),
                Some(commitment),
                Default::default(),
            )
            .await?;

        while let Some(message) = stream.next().await {
            let message = message?;

            let Some(update) = message.update_oneof else {
                continue;
            };

            match update {
                UpdateOneof::Slot(slot) => slots_sender
                    .as_ref()
                    .context("Unexpected slot notification")?
                    .send(slot.slot)
                    .context("Grpc slot sender closed")?,
                UpdateOneof::Block(block) => {
                    let block = FilteredBlock::from(block);

                    let Ok(processed_block) = ProcessedBlock::process(block, commitment_config)
                    else {
                        continue;
                    };

                    block_sender
                        .as_ref()
                        .context("Unexpected block notification")?
                        .send(processed_block)
                        .context("Grpc block sender closed")?
                }
                UpdateOneof::Transaction(tx) => tx_sender
                    .as_ref()
                    .context("Unexpected transaction notification")?
                    .send(tx)
                    .context("Grpc tx sender closed")?,
                UpdateOneof::Ping(_) => {
                    log::trace!("GRPC Ping {commitment:?}");
                }
                k => {
                    bail!("Unexpected update: {k:?}");
                }
            };
        }

        bail!("Stream closed unexpectedly")
    }
}

impl From<SubscribeUpdateBlock> for FilteredBlock {
    fn from(block: SubscribeUpdateBlock) -> Self {
        let transactions: Vec<FilteredTransaction> = block
            .transactions
            .into_iter()
            .filter_map(|tx| {
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
                    .collect();

                let message = VersionedMessage::V0(v0::Message {
                    header: MessageHeader {
                        num_required_signatures: header.num_required_signatures as u8,
                        num_readonly_signed_accounts: header.num_readonly_signed_accounts as u8,
                        num_readonly_unsigned_accounts: header.num_readonly_unsigned_accounts as u8,
                    },
                    account_keys: message
                        .account_keys
                        .into_iter()
                        .map(|key| Pubkey::new(&key))
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
                        .map(|table| MessageAddressTableLookup {
                            account_key: Pubkey::new(&table.account_key),
                            writable_indexes: table.writable_indexes,
                            readonly_indexes: table.readonly_indexes,
                        })
                        .collect(),
                });

                Some(FilteredTransaction {
                    transaction: VersionedTransaction {
                        signatures,
                        message,
                    },
                    meta: tx.meta.map(|meta| FilteredTransactionMeta {
                        err: meta.err.map(|err| {
                            String::from_utf8(err.err).expect("Error converting err to error")
                        }),
                        compute_units_consumed: meta.compute_units_consumed,
                        fee: meta.fee,
                        pre_balances: meta.pre_balances,
                        post_balances: meta.post_balances,
                    }),
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
                        yellowstone_grpc_proto::prelude::RewardType::Voting => {
                            Some(RewardType::Voting)
                        }
                    },
                    // Warn: idk how to convert string to option u8
                    commission: None,
                })
                .collect()
        });

        FilteredBlock {
            blockhash: block.blockhash,
            slot: block.slot,
            transactions: Some(transactions),
            block_height: block
                .block_height
                .map(|block_height| block_height.block_height),
            parent_slot: block.parent_slot,
            block_time: block.block_time.map(|time| time.timestamp),
            rewards,
        }
    }
}
