use std::collections::HashMap;
use std::ops::RangeInclusive;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use chrono::Duration;
use itertools::Itertools;
use log::{debug, error, info, trace, warn};
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_lite_rpc_core::structures::epoch::{Epoch, EpochRef};
use solana_lite_rpc_core::{
    structures::{epoch::EpochCache, produced_block::ProducedBlock},
    traits::block_storage_interface::BlockStorageInterface,
};
use solana_sdk::slot_history::Slot;
use solana_transaction_status::Reward;
use tokio::sync::Mutex;
use tokio_postgres::Error;
use tokio_postgres::error::SqlState;
use solana_lite_rpc_core::iterutils::Uniqueness;

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
    write_session: PostgresWriteSession,
    epoch_schedule: EpochCache,
    // postgres_data: Arc<RwLock<PostgresData>>,
}

#[derive(Clone)]
struct PostgresWriteSession {
    session: Arc<RwLock<PostgresSession>>,
}

impl PostgresWriteSession {
    pub async fn new_from_env() -> Result<Self> {
        let session = PostgresSession::new_from_env().await?;
        Ok(Self {
            session: Arc::new(RwLock::new(session))
        })
    }

    pub async fn get_write_session(&self) -> PostgresSession {
        let session = self.session.read().unwrap();

        if session.client.is_closed() || session.client.execute(";", &[]).await.is_err() {
            drop(session);
            info!("Reconnecting to postgres after detecting closed/broken connection!")
            let session = PostgresSession::new_from_env().await
                .expect("should have created new postgres session");
            let mut lock = self.session.write().unwrap();
            *lock = session.clone();
            session
        } else {
            session.clone()
        }

    }

}


impl PostgresBlockStore {
    pub async fn new(epoch_schedule: EpochCache) -> Self {
        let session_cache = PostgresSessionCache::new().await.unwrap();
        let write_session = PostgresWriteSession::new_from_env().await.unwrap();

        Self::check_role(&session_cache).await;

        Self {
            session_cache,
            write_session,
            epoch_schedule,
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

    // return true if schema was actually created
    async fn start_new_epoch_if_necessary(&self, epoch: EpochRef) -> Result<bool> {
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
                return Ok(false);
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
        Ok(true)
    }

    async fn get_session(&self) -> PostgresSession {
        self.session_cache
            .get_session()
            .await
            .expect("should get new postgres session")
    }


    pub async fn is_block_in_range(&self, slot: Slot) -> bool {
        let epoch = self.epoch_schedule.get_epoch_at_slot(slot);
        let ranges = self.get_slot_range_by_epoch().await;
        let matching_range: Option<&RangeInclusive<Slot>> = ranges.get(&epoch.into());

        matching_range.map(|slot_range| slot_range.contains(&slot)).is_some()
    }

    pub async fn query(&self, slot: Slot) -> Result<ProducedBlock> {
        let started = Instant::now();
        let epoch: EpochRef = self.epoch_schedule.get_epoch_at_slot(slot).into();

        let query = PostgresBlock::build_query_statement(epoch, slot);
        let block_row = self.get_session().await.query_opt(
            &query,
            &[])
        .await.unwrap();

        if block_row.is_none() {
            bail!("Block {} in epoch {} not found in postgres", slot, epoch);
        }

        let row = block_row.unwrap();
        // meta data
        let _epoch: i64 = row.get("_epoch");
        let epoch_schema: String = row.get("_epoch_schema");

        let blockhash: String = row.get("blockhash");
        let block_height: i64 = row.get("block_height");
        let slot: i64 = row.get("slot");
        let parent_slot: i64 = row.get("parent_slot");
        let block_time: i64 = row.get("block_time");
        let previous_blockhash: String = row.get("previous_blockhash");
        let rewards: Option<String> = row.get("rewards");
        let leader_id: Option<String> = row.get("leader_id");

        let postgres_block = PostgresBlock {
            slot,
            blockhash,
            block_height,
            parent_slot,
            block_time,
            previous_blockhash,
            rewards: rewards,
            leader_id: leader_id,
        };

        let produced_block = postgres_block.into_produced_block(
            // TODO what to do
            vec![],
            CommitmentConfig::confirmed(),
        );

        debug!("Querying produced block {} from postgres in epoch schema {} took {:.2}ms: {}/{}",
            produced_block.slot, epoch_schema,
            started.elapsed().as_secs_f64() * 1000.0,
            produced_block.blockhash, produced_block.commitment_config.commitment);

        return Ok(produced_block);

    }

    // optimistically try to progress commitment level for a block that is already stored
    pub async fn progress_block_commitment_level(&self, block: &ProducedBlock) -> Result<()> {
        // ATM we only support updating confirmed block to finalized
        if block.commitment_config.commitment == CommitmentLevel::Finalized {
            debug!("Checking block {} if we can progress it to finalized ...", block.slot);

            // TODO model commitment levels in new table

        }

        Ok(())
    }

    pub async fn write_block(&self, block: &ProducedBlock) -> Result<()> {

        self.progress_block_commitment_level(block).await?;

        // let PostgresData { current_epoch, .. } = { *self.postgres_data.read().await };

        trace!("Saving block {} to postgres storage...", block.slot);
        let slot = block.slot;
        let transactions = block
            .transactions
            .iter()
            .map(|x| PostgresTransaction::new(x, slot))
            .collect_vec();
        let postgres_block = PostgresBlock::from(block);

        let epoch = self.epoch_schedule.get_epoch_at_slot(slot);

        // TODO use write_session
        let session = self.get_session().await;

        let started_block = Instant::now();
        let inserted = postgres_block.save(&session, epoch.into()).await?;

        if !inserted {
            debug!("Block {} already exists - skip update", slot);
            return Ok(());
        }
        let elapsed_block_insert = started_block.elapsed();

        // NOTE: this controls the number of rows in VALUES clause of INSERT statement
        const NUM_TX_PER_CHUNK: usize = 20;

        // save transaction
        let chunks = transactions.chunks(NUM_TX_PER_CHUNK);
        let started_txs = Instant::now();
        for chunk in chunks {
            PostgresTransaction::save_transaction_batch(&session, epoch.into(), slot, chunk)
                .await?;
        }
        let elapsed_txs_insert = started_txs.elapsed();

        debug!(
            "Saving block {} with {} txs to postgres took {:.2}ms for block and {:.2}ms for transactions",
            slot,
            transactions.len(),
            elapsed_block_insert.as_secs_f64() * 1000.0,
            elapsed_txs_insert.as_secs_f64() * 1000.0,
        );
        Ok(())
    }

    // create current + next epoch
    // true if anything was created; false if a NOOP
    pub async fn prepare_epoch_schema(&self, slot: Slot) -> anyhow::Result<bool> {
        let epoch = self.epoch_schedule.get_epoch_at_slot(slot);
        let current_epoch = epoch.into();
        let created_current = self.start_new_epoch_if_necessary(current_epoch).await?;
        let next_epoch = current_epoch.get_next_epoch();
        let created_next = self.start_new_epoch_if_necessary(next_epoch).await?;
        Ok(created_current || created_next)
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
        let map_epoch_to_slot_range = self.get_slot_range_by_epoch().await;

        let rows_minmax: Vec<&RangeInclusive<Slot>> = map_epoch_to_slot_range.iter().map(|(_, range)| range).collect_vec();

        let slot_min = rows_minmax.iter().map(|range| range.start()).min().expect("non-empty result");
        let slot_max = rows_minmax.iter().map(|range| range.end()).max().expect("non-empty result");

        RangeInclusive::new(*slot_min, *slot_max)
    }
}


impl PostgresBlockStore {
    pub async fn get_slot_range_by_epoch(&self) -> HashMap<EpochRef, RangeInclusive<Slot>> {
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
            .iter()
            .map(|row| row.get::<&str, &str>("schema_name"))
            .map(|schema_name| (schema_name, PostgresEpoch::parse_epoch_from_schema_name(schema_name)))
            .collect_vec();

        if epoch_schemas.is_empty() {
            return HashMap::new();
        }

        let inner =
            epoch_schemas.iter()
            .map(|(schema, epoch)|
                format!("SELECT slot,{epoch}::bigint as epoch FROM {schema}.blocks",
                                  schema = schema, epoch = epoch))
            .join(" UNION ALL ");

        let query = format!(
            r#"
                SELECT epoch, min(slot) as slot_min, max(slot) as slot_max FROM (
                    {inner}
                ) AS all_slots
                GROUP BY epoch
            "#,
            inner = inner
        );


        let rows_minmax = session.query_list(&query, &[]).await.unwrap();

        if rows_minmax.is_empty() {
            return HashMap::new();
        }

        let mut map_epoch_to_slot_range =
            rows_minmax.iter()
                .map(|row| (
                    row.get::<&str, i64>("epoch"),
                    RangeInclusive::new(
                        row.get::<&str, i64>("slot_min") as Slot,
                        row.get::<&str, i64>("slot_max") as Slot
                    )
                ))
                .into_grouping_map()
                .fold(None, |acc, _key, val| {
                    assert!(acc.is_none(), "epoch must be unique");
                    Some(val)
                });

        let final_range: HashMap<EpochRef, RangeInclusive<Slot>> =
            map_epoch_to_slot_range.iter_mut().map(|(epoch, range)| {
                let epoch = EpochRef::new(*epoch as u64);
                (epoch, range.clone().expect("range must be returned from SQL"))
            }).collect();


        debug!("Slot range check in postgres found {} ranges, took {:2}sec: {:?}",
            rows_minmax.len(),
            started.elapsed().as_secs_f64(), final_range);

        final_range
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
    async fn postgres_write_session() {
        let write_session = PostgresWriteSession::new_from_env().await.unwrap();

        let row_role = write_session.get_write_session().await.query_one("SELECT current_role", &[]).await.unwrap();

        info!("row: {:?}", row_role);


    }

    #[tokio::test]
    #[ignore]
    async fn test_save_block() {
        tracing_subscriber::fmt::init();

        std::env::set_var("PG_CONFIG", "host=localhost dbname=literpc3 user=literpc_app password=litelitesecret sslmode=disable");
        let _postgres_session_cache = PostgresSessionCache::new().await.unwrap();
        let epoch_cache = EpochCache::new_for_tests();

        let postgres_block_store = PostgresBlockStore::new(epoch_cache.clone()).await;

        postgres_block_store
            .write_block(&create_test_block())
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
