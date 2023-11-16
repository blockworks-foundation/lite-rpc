use std::ops::RangeInclusive;
use std::time::Instant;

use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::Duration;
use itertools::Itertools;
use log::{debug, info, trace, warn};
use solana_lite_rpc_core::structures::epoch::EpochRef;
use solana_lite_rpc_core::{
    structures::{epoch::EpochCache, produced_block::ProducedBlock},
    traits::block_storage_interface::BlockStorageInterface,
};
use solana_sdk::slot_history::Slot;
use tokio_postgres::error::SqlState;

use crate::postgres::postgres_epoch::{PostgresEpoch, EPOCH_SCHEMA_PREFIX};
use crate::postgres::postgres_session::PostgresSession;
use crate::postgres::{
    postgres_block::PostgresBlock, postgres_session::PostgresSessionCache,
    postgres_transaction::PostgresTransaction,
};

const LITERPC_ROLE: &str = "r_literpc";

#[derive(Default, Clone, Copy)]
pub struct PostgresData {
    // from_slot: Slot,
    // to_slot: Slot,
    // current_epoch: Epoch,
}

#[derive(Clone)]
pub struct PostgresBlockStore {
    session_cache: PostgresSessionCache,
    epoch_cache: EpochCache,
    // postgres_data: Arc<RwLock<PostgresData>>,
}

impl PostgresBlockStore {
    pub async fn new(epoch_cache: EpochCache) -> Self {
        let session_cache = PostgresSessionCache::new().await.unwrap();
        // let postgres_data = Arc::new(RwLock::new(PostgresData::default()));

        Self::check_role(&session_cache).await;

        Self {
            session_cache,
            epoch_cache,
            // postgres_data,
        }
    }

    async fn check_role(session_cache: &PostgresSessionCache) {
        let role = LITERPC_ROLE;
        let statement = format!("SELECT 1 FROM pg_roles WHERE rolname='{role}'");
        let count = session_cache
            .get_session()
            .await
            .expect("must get session")
            .execute(&statement, &[])
            .await
            .expect("must execute query to check for role");

        if count == 0 {
            panic!(
                "Missing mandatory postgres role '{}' for Lite RPC - see permissions.sql",
                role
            );
        } else {
            info!("Self check - found postgres role '{}'", role);
        }
    }

    async fn start_new_epoch_if_necessary(&self, epoch: EpochRef) -> Result<()> {
        // create schema for new epoch
        let schema_name = PostgresEpoch::build_schema_name(epoch);
        let session = self.get_session().await;

        let statement = PostgresEpoch::build_create_schema_statement(epoch);
        // note: requires GRANT CREATE ON DATABASE xyz
        let result_create_schema = session.execute_simple(&statement).await;
        if let Err(err) = result_create_schema {
            if err
                .code()
                .map(|sqlstate| sqlstate == &SqlState::DUPLICATE_SCHEMA)
                .unwrap_or(false)
            {
                // TODO: do we want to allow this; continuing with existing epoch schema might lead to inconsistent data in blocks and transactions table
                info!(
                    "Schema {} for epoch {} already exists - data will be appended",
                    schema_name, epoch
                );
                return Ok(());
            } else {
                return Err(err).context("create schema for new epoch");
            }
        }

        // set permissions for new schema
        let statement = build_assign_permissions_statements(epoch);
        session
            .execute_simple(&statement)
            .await
            .context("Set postgres permissions for new schema")?;

        // Create blocks table
        let statement = PostgresBlock::build_create_table_statement(epoch);
        session
            .execute_simple(&statement)
            .await
            .context("create blocks table for new epoch")?;

        // create transaction table
        let statement = PostgresTransaction::build_create_table_statement(epoch);
        session
            .execute_simple(&statement)
            .await
            .context("create transaction table for new epoch")?;

        // add foreign key constraint between transactions and blocks
        let statement = PostgresTransaction::build_foreign_key_statement(epoch);
        session
            .execute_simple(&statement)
            .await
            .context("create foreign key constraint between transactions and blocks")?;

        info!("Start new epoch in postgres schema {}", schema_name);
        Ok(())
    }

    async fn get_session(&self) -> PostgresSession {
        self.session_cache
            .get_session()
            .await
            .expect("should get new postgres session")
    }

    pub async fn query(&self, slot: Slot) -> Result<ProducedBlock> {
        let range = self.get_slot_range().await;
        if range.contains(&slot) {}
        todo!()
    }

    pub async fn save(&self, block: &ProducedBlock) -> Result<()> {
        let started = Instant::now();
        trace!("Saving block {} to postgres storage...", block.slot);

        // let PostgresData { current_epoch, .. } = { *self.postgres_data.read().await };

        let slot = block.slot;
        let transactions = block
            .transactions
            .iter()
            .map(|x| PostgresTransaction::new(x, slot))
            .collect_vec();
        let postgres_block = PostgresBlock::from(block);

        let epoch = self.epoch_cache.get_epoch_at_slot(slot);
        self.start_new_epoch_if_necessary(epoch.into()).await?;

        let session = self.get_session().await;

        postgres_block.save(&session, epoch.into()).await?;

        // NOTE: this controls the number of rows in VALUES clause of INSERT statement
        const NUM_TX_PER_CHUNK: usize = 20;

        // save transaction
        let chunks = transactions.chunks(NUM_TX_PER_CHUNK);
        for chunk in chunks {
            PostgresTransaction::save_transaction_batch(&session, epoch.into(), slot, chunk)
                .await?;
        }
        debug!(
            "Saving block {} with {} txs to postgres took {:.2}ms",
            slot,
            transactions.len(),
            started.elapsed().as_secs_f64() * 1000.0
        );
        Ok(())
    }
}

fn build_assign_permissions_statements(epoch: EpochRef) -> String {
    let role = LITERPC_ROLE;
    let schema = PostgresEpoch::build_schema_name(epoch);

    format!(
        r#"
        GRANT USAGE ON SCHEMA {schema} TO {role};
        GRANT ALL ON ALL TABLES IN SCHEMA {schema} TO {role};
        ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT ALL ON TABLES TO {role};
    "#
    )
}

#[async_trait]
impl BlockStorageInterface for PostgresBlockStore {

    async fn get_slot_range(&self) -> RangeInclusive<Slot> {
        let started = Instant::now();
        let session = self.get_session().await;
        // e.g. "rpc2a_epoch_552"
        let query = format!(
            r#"
                SELECT
                 schema_name
                FROM information_schema.schemata
                WHERE schema_name ~ '^{schema_prefix}[0-9]+$'
            "#,
            schema_prefix = EPOCH_SCHEMA_PREFIX
        );
        let result = session.query_list(&query, &[]).await.unwrap();

        let epoch_schemas = result
            .iter().map(|row| row.get::<&str, &str>("schema_name"))
            .collect_vec();

        if epoch_schemas.is_empty() {
            return RangeInclusive::new(1, 0);
        }

        let inner =
            epoch_schemas.iter()
            .map(|schema| format!("SELECT slot FROM {schema}.blocks", schema = schema))
            .join(" UNION ALL ");

        let query = format!(
            r#"
                SELECT min(slot) as slot_min, max(slot) as slot_max FROM (
                    {inner}
                ) AS all_slots
            "#,
            inner = inner
        );


        let row_minmax = session.query_one(&query, &[]).await.unwrap();
        let slot_min = row_minmax.get::<&str, i64>("slot_min");
        let slot_max = row_minmax.get::<&str, i64>("slot_max");

        if started.elapsed().as_millis() > 10 {
            warn!("Slow slot range check in postgres - took {:2}sec", started.elapsed().as_secs_f64());
        }

        RangeInclusive::new(slot_min as u64, slot_max as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_lite_rpc_core::structures::produced_block::TransactionInfo;
    use solana_sdk::commitment_config::CommitmentConfig;
    use solana_sdk::signature::Signature;
    use std::str::FromStr;

    #[tokio::test]
    #[ignore]
    async fn test_save_block() {
        tracing_subscriber::fmt::init();

        std::env::set_var("PG_CONFIG", "host=localhost dbname=literpc3 user=literpc_app password=litelitesecret sslmode=disable");
        let _postgres_session_cache = PostgresSessionCache::new().await.unwrap();
        let epoch_cache = EpochCache::new_for_tests();

        let postgres_block_store = PostgresBlockStore::new(epoch_cache.clone()).await;

        postgres_block_store
            .save(&create_test_block())
            .await
            .unwrap();
    }

    fn create_test_block() -> ProducedBlock {
        let sig1 = Signature::from_str("5VBroA4MxsbZdZmaSEb618WRRwhWYW9weKhh3md1asGRx7nXDVFLua9c98voeiWdBE7A9isEoLL7buKyaVRSK1pV").unwrap();
        let sig2 = Signature::from_str("3d9x3rkVQEoza37MLJqXyadeTbEJGUB6unywK4pjeRLJc16wPsgw3dxPryRWw3UaLcRyuxEp1AXKGECvroYxAEf2").unwrap();

        ProducedBlock {
            block_height: 42,
            blockhash: "blockhash".to_string(),
            previous_blockhash: "previous_blockhash".to_string(),
            parent_slot: 666,
            slot: 223555999,
            transactions: vec![create_test_tx(sig1), create_test_tx(sig2)],
            // TODO double if this is unix millis or seconds
            block_time: 1699260872000,
            commitment_config: CommitmentConfig::finalized(),
            leader_id: None,
            rewards: None,
        }
    }

    fn create_test_tx(signature: Signature) -> TransactionInfo {
        TransactionInfo {
            signature: signature.to_string(),
            err: None,
            cu_requested: Some(40000),
            prioritization_fees: Some(5000),
            cu_consumed: Some(32000),
            recent_blockhash: "recent_blockhash".to_string(),
            message: "some message".to_string(),
        }
    }
}
