use futures::channel::mpsc::SendError as FuturesSendError;
use futures::{channel::mpsc, sink::Sink, stream::Stream};
use solana_lite_rpc_core::block_store::BlockInformation;
use solana_sdk::epoch_info::EpochInfo;
use solana_transaction_status::EncodedConfirmedBlock;
use thiserror::Error;
use tokio::sync::broadcast::Sender as BroadcastSender;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::wrappers::BroadcastStream;

#[derive(Debug, Error)]
pub enum StreamError {
    #[error("Error during during send slot '{0}'")]
    SendSlotError(String),
}

pub type Slot = u64;

macro_rules! create_stream {
    ($connector_name:ident,$connector_sender:ident, $stream_data:ident) => {
        pub struct $connector_name {
            data_receiver: mpsc::UnboundedReceiver<$stream_data>,
            data_sender: mpsc::UnboundedSender<$stream_data>,
        }
        impl $connector_name {
            pub fn new() -> Self {
                let (data_sender, data_receiver) = mpsc::unbounded();

                Self {
                    data_receiver,
                    data_sender,
                }
            }

            pub fn subscribe(self) -> impl Stream<Item = $stream_data> {
                self.data_receiver
            }

            //return an error if the fullblock subscription hasn't been done.
            pub fn get_sender_sink(&self) -> impl Sink<$stream_data, Error = FuturesSendError> {
                self.data_sender.clone()
            }
        }
    };
}

create_stream!(BlockInfokStreamConnector, BlockInfoSender, BlockInformation);
create_stream!(
    FullblockStreamConnector,
    FullBlockSender,
    EncodedConfirmedBlock
);
create_stream!(EpochStreamConnector, EpochSender, EpochInfo);

pub struct SlotStreamConnector {
    slot_sender: BroadcastSender<Slot>,
}

impl SlotStreamConnector {
    pub fn new(capacity: usize) -> Self {
        let (slot_sender, _) = tokio::sync::broadcast::channel::<Slot>(capacity);

        SlotStreamConnector { slot_sender }
    }

    pub fn subscribe_slot(&self) -> impl Stream<Item = Result<Slot, BroadcastStreamRecvError>> {
        BroadcastStream::new(self.slot_sender.subscribe())
    }

    pub fn send_slot(&self, slot: Slot) -> anyhow::Result<()> {
        Ok(self.slot_sender.send(slot).map(|_| ())?)
    }

    pub fn get_sender(&self) -> BroadcastSender<Slot> {
        self.slot_sender.clone()
    }
}

// pub struct FullblockStreamConnector {
//     fullblock_receiver: UnboundedReceiver<EncodedConfirmedBlock>,
//     fullblock_sender: UnboundedSender<EncodedConfirmedBlock>,
// }

// impl FullblockStreamConnector {
//     pub fn new() -> Self {
//         let (fullblock_sender, fullblock_receiver) = tokio::sync::mpsc::unbounded_channel();

//         FullblockStreamConnector {
//             fullblock_receiver,
//             fullblock_sender,
//         }
//     }

//     pub fn subscribe_fullblock(self) -> impl Stream<Item = EncodedConfirmedBlock> {
//         UnboundedReceiverStream::new(self.fullblock_receiver)
//     }

//     //return an error if the fullblock subscription hasn't been done.
//     pub fn get_block_sender(&self) -> FullBlockSender {
//         FullBlockSender {
//             sender: self.fullblock_sender.clone(),
//         }
//     }
// }

// #[derive(Debug, Clone)]
// pub struct FullBlockSender {
//     sender: UnboundedSender<EncodedConfirmedBlock>,
// }

// impl FullBlockSender {
//     pub fn send_fullblock(&self, block: EncodedConfirmedBlock) {
//         if let Err(err) = self.sender.send(block) {
//             log::error!("Send block fail cause:{err}");
//         }
//     }
// }
