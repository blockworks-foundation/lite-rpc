use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use itertools::Itertools;
use solana_lite_rpc_core::{
    structures::{epoch::EpochCache, produced_block::ProducedBlock},
    traits::block_storage_interface::BlockStorageInterface,
};
use solana_rpc_client_api::config::RpcBlockConfig;
use solana_sdk::{slot_history::Slot, stake_history::Epoch};
use tokio::sync::RwLock;

use crate::postgres::{
    postgres_block::PostgresBlock, postgres_session::PostgresSessionCache,
    postgres_transaction::PostgresTransaction,
};

#[derive(Default, Clone, Copy)]
pub struct PostgresData {
    from_slot: Slot,
    to_slot: Slot,
    current_epoch: Epoch,
}

pub struct PostgresBlockStore {
    session_cache: PostgresSessionCache,
    epoch_cache: EpochCache,
    postgres_data: Arc<RwLock<PostgresData>>,
}

impl PostgresBlockStore {
    pub async fn start_new_epoch(&self, schema: &String) -> Result<()> {
        // create schema for new epoch
        let session = self
            .session_cache
            .get_session()
            .await
            .expect("should get new postgres session");

        let statement = format!("CREATE SCHEMA {};", schema);
        // note: requires GRANT CREATE ON DATABASE xyz
        session.execute(&statement, &[]).await
            .context("create schema for new epoch")?;

        // Create blocks table
        let statement = PostgresBlock::create_statement(schema);
        session.execute(&statement, &[]).await
            .context("create blocks table for new epoch")?;

        // create transaction table
        let statement = PostgresTransaction::create_statement(schema);
        session.execute(&statement, &[]).await
            .context("create transaction table for new epoch")?;
        Ok(())

    }
}

#[async_trait]
impl BlockStorageInterface for PostgresBlockStore {
    async fn save(&self, block: ProducedBlock) -> Result<()> {
        let PostgresData { current_epoch, .. } = { *self.postgres_data.read().await };

        let slot = block.slot;
        let transactions = block
            .transactions
            .iter()
            .map(|x| PostgresTransaction::new(x, slot))
            .collect_vec();
        let postgres_block = PostgresBlock::from(&block);

        let epoch = self.epoch_cache.get_epoch_at_slot(slot);
        let schema = format!("EPOCH_{}", epoch.epoch);
        if current_epoch == 0 || current_epoch < epoch.epoch {
            self.postgres_data.write().await.current_epoch = epoch.epoch;
            self.start_new_epoch(&schema).await?;
        }

        const NUMBER_OF_TRANSACTION: usize = 20;

        // save transaction
        let chunks = transactions.chunks(NUMBER_OF_TRANSACTION);
        let session = self
            .session_cache
            .get_session()
            .await
            .expect("should get new postgres session");
        for chunk in chunks {
            PostgresTransaction::save_transactions(&session, &schema, chunk).await?;
        }
        postgres_block.save(&session, &schema).await?;
        Ok(())
    }

    async fn get(&self, slot: Slot, _config: RpcBlockConfig) -> Result<ProducedBlock> {
        let range = self.get_slot_range().await;
        if range.contains(&slot) {}
        todo!()
    }

    async fn get_slot_range(&self) -> std::ops::Range<Slot> {
        let lk = self.postgres_data.read().await;
        lk.from_slot..lk.to_slot + 1
    }
}
