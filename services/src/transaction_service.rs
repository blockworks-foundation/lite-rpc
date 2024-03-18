// This class will manage the lifecycle for a transaction
// It will send, replay if necessary and confirm by listening to blocks

use std::time::Duration;

use crate::{
    tpu_utils::tpu_service::TpuService,
    transaction_replayer::{TransactionReplay, TransactionReplayer, MESSAGES_IN_REPLAY_QUEUE},
    tx_sender::TxSender,
};
use anyhow::bail;
use prometheus::{histogram_opts, register_histogram, Histogram};
use solana_lite_rpc_core::{
    solana_utils::SerializableTransaction, structures::transaction_sent_info::SentTransactionInfo,
    types::SlotStream,
};
use solana_lite_rpc_core::{
    stores::block_information_store::{BlockInformation, BlockInformationStore},
    structures::notifications::NotificationSender,
    AnyhowJoinHandle,
};
use solana_sdk::{
    borsh0_10::try_from_slice_unchecked,
    compute_budget::{self, ComputeBudgetInstruction},
    transaction::VersionedTransaction,
};
use tokio::{
    sync::mpsc::{self, Sender, UnboundedSender},
    time::Instant,
};

lazy_static::lazy_static! {
static ref PRIORITY_FEES_HISTOGRAM: Histogram = register_histogram!(histogram_opts!(
    "literpc_txs_priority_fee",
    "Priority fees of transactions sent by lite-rpc",
))
.unwrap();
}

#[derive(Clone)]
pub struct TransactionServiceBuilder {
    tx_sender: TxSender,
    tx_replayer: TransactionReplayer,
    tpu_service: TpuService,
    max_nb_txs_in_queue: usize,
}

impl TransactionServiceBuilder {
    pub fn new(
        tx_sender: TxSender,
        tx_replayer: TransactionReplayer,
        tpu_service: TpuService,
        max_nb_txs_in_queue: usize,
    ) -> Self {
        Self {
            tx_sender,
            tx_replayer,
            tpu_service,
            max_nb_txs_in_queue,
        }
    }

    pub fn start(
        self,
        notifier: Option<NotificationSender>,
        block_information_store: BlockInformationStore,
        max_retries: usize,
        slot_notifications: SlotStream,
    ) -> (TransactionService, AnyhowJoinHandle) {
        let (transaction_channel, tx_recv) = mpsc::channel(self.max_nb_txs_in_queue);
        let (replay_channel, replay_reciever) = tokio::sync::mpsc::unbounded_channel();

        let jh_services: AnyhowJoinHandle = {
            let tx_sender = self.tx_sender.clone();
            let tx_replayer = self.tx_replayer.clone();
            let tpu_service = self.tpu_service.clone();
            let replay_channel_task = replay_channel.clone();

            tokio::spawn(async move {
                let tpu_service_fx = tpu_service.start(slot_notifications);

                let tx_sender_jh = tx_sender.clone().execute(tx_recv, notifier.clone());

                let replay_service =
                    tx_replayer.start_service(replay_channel_task, replay_reciever);

                tokio::select! {
                    res = tpu_service_fx => {
                        bail!("Tpu Service {res:?}")
                    },
                    res = tx_sender_jh => {
                        bail!("Tx Sender {res:?}")
                    },
                    res = replay_service => {
                        bail!("Replay Service {res:?}")
                    },
                }
            })
        };

        (
            TransactionService {
                transaction_channel,
                replay_channel,
                block_information_store,
                max_retries,
                replay_offset: self.tx_replayer.retry_offset,
            },
            jh_services,
        )
    }
}

#[derive(Clone)]
pub struct TransactionService {
    pub transaction_channel: Sender<SentTransactionInfo>,
    pub replay_channel: UnboundedSender<TransactionReplay>,
    pub block_information_store: BlockInformationStore,
    pub max_retries: usize,
    pub replay_offset: Duration,
}

impl TransactionService {
    pub async fn send_transaction(
        &self,
        raw_tx: Vec<u8>,
        max_retries: Option<u16>,
    ) -> anyhow::Result<String> {
        let tx = match bincode::deserialize::<VersionedTransaction>(&raw_tx) {
            Ok(tx) => tx,
            Err(err) => {
                bail!(err.to_string());
            }
        };
        let signature = tx.signatures[0];

        let Some(BlockInformation {
            slot,
            last_valid_blockheight,
            ..
        }) = self
            .block_information_store
            .get_block_info(&tx.get_recent_blockhash().to_string())
        else {
            bail!("Blockhash not found in block store".to_string());
        };

        if self.block_information_store.get_last_blockheight() > last_valid_blockheight {
            bail!("Blockhash is expired");
        }

        let prioritization_fee = {
            let mut prioritization_fee = 0;
            for ix in tx.message.instructions() {
                if ix
                    .program_id(tx.message.static_account_keys())
                    .eq(&compute_budget::id())
                {
                    let ix_which =
                        try_from_slice_unchecked::<ComputeBudgetInstruction>(ix.data.as_slice());
                    if let Ok(ComputeBudgetInstruction::SetComputeUnitPrice(fees)) = ix_which {
                        prioritization_fee = fees;
                    }
                }
            }
            prioritization_fee
        };

        PRIORITY_FEES_HISTOGRAM.observe(prioritization_fee as f64);

        let max_replay = max_retries.map_or(self.max_retries, |x| x as usize);
        let transaction_info = SentTransactionInfo {
            signature: signature.to_string(),
            last_valid_block_height: last_valid_blockheight,
            slot,
            transaction: raw_tx,
            prioritization_fee,
        };
        if let Err(e) = self
            .transaction_channel
            .send(transaction_info.clone())
            .await
        {
            bail!(
                "Internal error sending transaction on send channel error {}",
                e
            );
        }
        let replay_at = Instant::now() + self.replay_offset;
        // ignore error for replay service
        if self
            .replay_channel
            .send(TransactionReplay {
                transaction: transaction_info,
                replay_count: 0,
                max_replay,
                replay_at,
            })
            .is_ok()
        {
            MESSAGES_IN_REPLAY_QUEUE.inc();
        }
        Ok(signature.to_string())
    }
}
