// This class will manage the lifecycle for a transaction
// It will send, replay if necessary and confirm by listening to blocks

use crate::tx_service::tx_replayer::MESSAGES_IN_REPLAY_QUEUE;
use std::time::Duration;

use anyhow::bail;
use solana_lite_rpc_core::{block_store::BlockMeta, ledger::Ledger};
use solana_rpc_client::rpc_client::SerializableTransaction;
use solana_sdk::transaction::VersionedTransaction;
use tokio::{
    sync::mpsc::{Sender, UnboundedSender},
    time::Instant,
};

use super::{tx_batch_fwd::TxInfo, tx_replayer::TxReplay};

#[derive(Clone)]
pub struct TxSender {
    pub tx_channel: Sender<TxInfo>,
    pub replay_channel: UnboundedSender<TxReplay>,
    pub ledger: Ledger,
    pub max_retries: u16,
    pub retry_after: Duration,
}

impl TxSender {
    pub async fn send_transaction(
        &self,
        // TODO: make this arc to avoid cloning
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
        let blockhash = tx.get_recent_blockhash().to_string();

        let Some(BlockMeta { slot, last_valid_blockheight, .. }) = self.ledger.block_store.get_block_info(&blockhash) else {
            bail!("Blockhash not found in block store");
        };

        let raw_tx_clone = raw_tx.clone();
        let max_replay = max_retries.unwrap_or(self.max_retries);

        if let Err(e) = self
            .tx_channel
            .send(TxInfo {
                signature: signature.to_string(),
                last_valid_blockheight,
                slot,
                tx: raw_tx,
            })
            .await
        {
            bail!("Internal error sending transaction on send channel error {e}",);
        }

        let replay_at = Instant::now() + self.retry_after;

        // ignore error for replay service
        if self
            .replay_channel
            .send(TxReplay {
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
