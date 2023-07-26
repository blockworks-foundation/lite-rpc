// This class will manage the lifecycle for a transaction
// It will send, replay if necessary and confirm by listening to blocks

use std::time::Duration;

use crate::{
    block_listenser::BlockListener,
    cleaner::Cleaner,
    tpu_utils::tpu_service::TpuService,
    transaction_replayer::{TransactionReplay, TransactionReplayer, MESSAGES_IN_REPLAY_QUEUE},
    tx_sender::{TransactionInfo, TxSender},
};
use anyhow::bail;
use solana_lite_rpc_core::{
    block_store::{BlockMeta
    notifications::NotificationSender,
    AnyhowJoinHandle,
};
use solana_rpc_client::rpc_client::SerializableTransaction;
use solana_sdk::{commitment_config::CommitmentConfig, transaction::VersionedTransaction};
use tokio::{
    sync::mpsc::{self, Sender, UnboundedSender},
    time::Instant,
};

#[derive(Clone)]
pub struct TransactionServiceBuilder {
    tx_sender: TxSender,
    tx_replayer: TransactionReplayer,
    block_listner: BlockListener,
    tpu_service: TpuService,
    max_nb_txs_in_queue: usize,
}

impl TransactionServiceBuilder {
    pub fn new(
        tx_sender: TxSender,
        tx_replayer: TransactionReplayer,
        block_listner: BlockListener,
        tpu_service: TpuService,
        max_nb_txs_in_queue: usize,
    ) -> Self {
        Self {
            tx_sender,
            tx_replayer,
            tpu_service,
            block_listner,
            max_nb_txs_in_queue,
        }
    }

    pub fn start(
        self,
        notifier: Option<NotificationSender>,
        block_store: BlockStore,
        max_retries: usize,
    ) -> (TransactionService, AnyhowJoinHandle) {
        let (transaction_channel, tx_recv) = mpsc::channel(self.max_nb_txs_in_queue);
        let (replay_channel, replay_reciever) = tokio::sync::mpsc::unbounded_channel();

        let jh_services: AnyhowJoinHandle = {
            let tx_sender = self.tx_sender.clone();
            let block_listner = self.block_listner.clone();
            let tx_replayer = self.tx_replayer.clone();
            let tpu_service = self.tpu_service.clone();
            let replay_channel_task = replay_channel.clone();
            let block_store_t = block_store.clone();

            tokio::spawn(async move {
                let tpu_service_fx = tpu_service.start();

                let tx_sender_jh = tx_sender.clone().execute(tx_recv, notifier.clone());

                let replay_service =
                    tx_replayer.start_service(replay_channel_task, replay_reciever);

                let finalized_block_listener = block_listner.clone().listen(
                    CommitmentConfig::finalized(),
                    notifier.clone(),
                    tpu_service.get_estimated_slot_holder(),
                );

                let confirmed_block_listener = block_listner.clone().listen(
                    CommitmentConfig::confirmed(),
                    None,
                    tpu_service.get_estimated_slot_holder(),
                );

                let processed_block_listener = block_listner.clone().listen_processed();

                // transactions get invalid in around 1 mins, because the block hash expires in 150 blocks so 150 * 400ms = 60s
                // Setting it to two to give some margin of error / as not all the blocks are filled.
                let cleaner = Cleaner::new(tx_sender.clone(), block_listner.clone(), block_store_t)
                    .start(Duration::from_secs(120));

                tokio::select! {
                    res = tpu_service_fx => {
                        bail!("Tpu Service {res:?}")
                    },
                    res = tx_sender_jh => {
                        bail!("Tx Sender {res:?}")
                    },
                    res = finalized_block_listener => {
                        bail!("Finalized Block Listener {res:?}")
                    },
                    res = confirmed_block_listener => {
                        bail!("Confirmed Block Listener {res:?}")
                    },
                    res = processed_block_listener => {
                        bail!("Processed Block Listener {res:?}")
                    },
                    res = replay_service => {
                        bail!("Replay Service {res:?}")
                    },
                    res = cleaner => {
                        bail!("Cleaner {res:?}")
                    },
                }
            })
        };

        (
            TransactionService {
                transaction_channel,
                replay_channel,
                block_store,
                max_retries,
                replay_after: self.tx_replayer.retry_after,
            },
            jh_services,
        )
    }
}

#[derive(Clone)]
pub struct TransactionService {
    pub transaction_channel: Sender<TransactionInfo>,
    pub replay_channel: UnboundedSender<TransactionReplay>,
    pub block_store: BlockStore,
    pub max_retries: usize,
    pub replay_after: Duration,
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

        let Some(BlockInfovalid_blockheight, .. }) = self
            .block_store
            .get_block_info(&tx.get_recent_blockhash().to_string())
        else {
            bail!("Blockhash not found in block store".to_string());
        };

        let raw_tx_clone = raw_tx.clone();
        let max_replay = max_retries.map_or(self.max_retries, |x| x as usize);
        if let Err(e) = self
            .transaction_channel
            .send(TransactionInfo {
                signature: signature.to_string(),
                last_valid_block_height: last_valid_blockheight,
                slot,
                transaction: raw_tx,
            })
            .await
        {
            bail!(
                "Internal error sending transaction on send channel error {}",
                e
            );
        }
        let replay_at = Instant::now() + self.replay_after;
        // ignore error for replay service
        if self
            .replay_channel
            .send(TransactionReplay {
                signature: signature.to_string(),
                tx: raw_tx_clone,
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
