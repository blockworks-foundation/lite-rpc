use futures::channel::mpsc::SendError as FuturesSendError;
use futures::Sink;
use futures::SinkExt;
use futures_util::StreamExt;
use solana_lite_rpc_core::block_store::BlockInformation;
use solana_transaction_status::EncodedConfirmedBlock;
use std::collections::HashMap;
use thiserror::Error;
use tokio::pin;
use tokio::task::JoinHandle;
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_client::GeyserGrpcClientError;
use yellowstone_grpc_proto::geyser::CommitmentLevel;
use yellowstone_grpc_proto::prelude::SubscribeRequestFilterAccounts;
use yellowstone_grpc_proto::prelude::SubscribeRequestFilterBlocks;
use yellowstone_grpc_proto::prelude::{subscribe_update::UpdateOneof, SubscribeRequestFilterSlots};

#[derive(Debug, Error)]
pub enum HistoryError {
    #[error("Error during during send slot '{0}'")]
    SendSlotError(String),

    #[error("IO Error during history processing '{0}'")]
    IOError(#[from] std::io::Error),

    #[error("Geyser Error during history processing '{0}'")]
    GeyserError(#[from] GeyserGrpcClientError),
}

pub async fn start_module(
    grpc_url: String,
    slot_sinck: impl Sink<u64, Error = FuturesSendError> + std::marker::Send + 'static,
    full_block_sinck: impl Sink<EncodedConfirmedBlock, Error = FuturesSendError>
        + std::marker::Send
        + 'static,
    block_info_sinck: impl Sink<BlockInformation, Error = FuturesSendError>
        + std::marker::Send
        + 'static,
) -> JoinHandle<Result<(), HistoryError>> {
    let handle = tokio::spawn(async move {
        pin!(full_block_sinck);
        pin!(block_info_sinck);

        //subscribe Geyser grpc
        let mut slots = HashMap::new();
        slots.insert("client".to_string(), SubscribeRequestFilterSlots {});

        let mut accounts: HashMap<String, SubscribeRequestFilterAccounts> = HashMap::new();

        accounts.insert(
            "client".to_owned(),
            SubscribeRequestFilterAccounts {
                account: vec![],
                owner: vec![
                    solana_sdk::stake::program::ID.to_string(),
                    solana_sdk::vote::program::ID.to_string(),
                ],
                filters: vec![],
            },
        );

        let mut blocks = HashMap::new();
        blocks.insert(
            "client".to_string(),
            SubscribeRequestFilterBlocks {
                account_include: vec![], //vec![SYSTEM_PROGRAM.to_string()],
                include_transactions: Some(true),
                include_accounts: None,
                include_entries: None,
            },
        );

        let mut client = GeyserGrpcClient::connect(grpc_url, None::<&'static str>, None)?;
        let mut confirmed_stream = client
            .subscribe_once(
                slots,
                accounts,           //accounts
                Default::default(), //tx
                Default::default(), //entry
                blocks,             //full block
                Default::default(), //block meta
                Some(CommitmentLevel::Confirmed),
                vec![],
            )
            .await?;

        loop {
            tokio::select! {
                ret = confirmed_stream.next() => {
                    match ret {
                         Some(message) => {
                            //process the message
                            match message {
                                Ok(msg) => {
                                    //log::info!("new message: {msg:?}");
                                    match msg.update_oneof {
                                        Some(UpdateOneof::Block(block)) => {
                                            let info_block = convert_into_blockinfo(&block);
                                            if let Err(err) = block_info_sinck.send(info_block).await {
                                                log::error!("Error during sending block info:{err}");
                                            }
                                            let rpc_block = convert_block(block);
                                            if let Err(err) = full_block_sinck.send(rpc_block).await {
                                                log::error!("Error during sending full block:{err}");
                                            }
                                        }
                                        Some(UpdateOneof::Account(account)) => {
                                        }
                                        Some(UpdateOneof::Slot(slot)) => {
                                        }
                                        Some(UpdateOneof::Ping(_)) => (),
                                        bad_msg => {
                                            log::info!("Geyser stream unexpected message received:{:?}",bad_msg);
                                        }
                                    }
                                }
                                Err(error) => {
                                    log::error!("Geyser stream receive an error has message: {error:?}, try to reconnect and resynchronize.");
                                    break;
                                }
                            }
                         }
                         None => {
                            log::warn!("The geyser stream close try to reconnect and resynchronize.");
                            break; //TODO reconnect.
                         }
                     }

                }

            }
        }

        Ok(())
    });
    handle
}

fn convert_into_blockinfo(
    geyser_block: &yellowstone_grpc_proto::geyser::SubscribeUpdateBlock,
) -> BlockInformation {
    BlockInformation {
        slot: geyser_block.slot,
        block_height: geyser_block
            .block_height
            .clone()
            .map(|h| h.block_height)
            .unwrap_or(0),
        last_valid_blockheight: 0,  //PUT INSIDE SENDTX
        cleanup_slot: 0,            //PUT INSIDE SENDTX
        processed_local_time: None, //change to u128 timestamp.
    }
}

fn convert_block(
    geyser_block: yellowstone_grpc_proto::geyser::SubscribeUpdateBlock,
) -> EncodedConfirmedBlock {
    EncodedConfirmedBlock {
        previous_blockhash: geyser_block.parent_blockhash,
        blockhash: geyser_block.blockhash,
        parent_slot: geyser_block.parent_slot,
        transactions: vec![], //TODO convert Tx
        rewards: vec![],
        block_time: geyser_block.block_time.map(|t| t.timestamp),
        block_height: geyser_block.block_height.map(|h| h.block_height),
    }
}
