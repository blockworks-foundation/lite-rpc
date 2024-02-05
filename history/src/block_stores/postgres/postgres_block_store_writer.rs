use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use itertools::Itertools;
use log::{debug, info, trace, warn};
use solana_lite_rpc_core::structures::epoch::EpochRef;
use solana_lite_rpc_core::structures::{epoch::EpochCache, produced_block::ProducedBlock};
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_sdk::slot_history::Slot;
use tokio_postgres::Error;
use tokio_postgres::error::SqlState;
use crate::block_stores::postgres::{LITERPC_QUERY_ROLE, LITERPC_ROLE};

use super::postgres_config::*;
use super::postgres_epoch::*;
use super::postgres_transaction::*;
use super::postgres_block::*;
use super::postgres_session::*;

const PARALLEL_WRITE_SESSIONS: usize = 4;
const MIN_WRITE_CHUNK_SIZE: usize = 500;


#[derive(Clone)]
pub struct PostgresBlockStore {
    session_cache: PostgresSessionCache,
    // use this session only for the write path!
    write_sessions: Vec<PostgresWriteSession>,
    epoch_schedule: EpochCache,
}

impl PostgresBlockStore {
    pub async fn new(epoch_schedule: EpochCache, pg_session_config: PostgresSessionConfig) -> Self {
        let session_cache = PostgresSessionCache::new(pg_session_config.clone())
            .await
            .unwrap();
        let mut write_sessions = Vec::new();
        for _i in 0..PARALLEL_WRITE_SESSIONS {
            write_sessions.push(
                PostgresWriteSession::new(pg_session_config.clone())
                    .await
                    .unwrap(),
            );
        }
        assert!(
            !write_sessions.is_empty(),
            "must have at least one write session"
        );

        Self::check_write_role(&session_cache).await;

        Self {
            session_cache,
            write_sessions,
            epoch_schedule,
        }
    }

    async fn check_write_role(session_cache: &PostgresSessionCache) {
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
                "Missing mandatory postgres write/ownership role '{}' for Lite RPC - see permissions.sql",
                role
            );
        } else {
            info!("Self check - found postgres write role/ownership '{}'", role);
        }
    }

    // return true if schema was actually created
    async fn start_new_epoch_if_necessary(&self, epoch: EpochRef) -> Result<bool> {
        // create schema for new epoch
        let schema_name = PostgresEpoch::build_schema_name(epoch);
        let session = self.get_session().await;

        let statement = PostgresEpoch::build_create_schema_statement(epoch);
        // note: requires GRANT CREATE ON DATABASE xyz
        let result_create_schema = session.execute_multiple(&statement).await;
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
            .execute_multiple(&statement)
            .await
            .context("Set postgres permissions for new schema")?;

        // Create blocks table
        let statement = PostgresBlock::build_create_table_statement(epoch);
        session
            .execute_multiple(&statement)
            .await
            .context("create blocks table for new epoch")?;

        // create transaction table
        let statement = PostgresTransaction::build_create_table_statement(epoch);
        session
            .execute_multiple(&statement)
            .await
            .context("create transaction table for new epoch")?;

        // add foreign key constraint between transactions and blocks
        let statement = PostgresTransaction::build_foreign_key_statement(epoch);
        session
            .execute_multiple(&statement)
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

    // optimistically try to progress commitment level for a block that is already stored
    pub async fn progress_block_commitment_level(&self, block: &ProducedBlock) -> Result<()> {
        // ATM we only support updating confirmed block to finalized
        if block.commitment_config.commitment == CommitmentLevel::Finalized {
            debug!(
                "Checking block {} if we can progress it to finalized ...",
                block.slot
            );

            // TODO model commitment levels in new table
        }
        Ok(())
    }

    pub async fn save_block(&self, block: &ProducedBlock) -> Result<()> {
        self.progress_block_commitment_level(block).await?;

        // let PostgresData { current_epoch, .. } = { *self.postgres_data.read().await };

        trace!("Saving block {}@{} to postgres storage...", block.slot, block.commitment_config.commitment);
        let slot = block.slot;
        let transactions = block
            .transactions
            .iter()
            .map(|x| PostgresTransaction::new(x, slot))
            .collect_vec();
        let postgres_block = PostgresBlock::from(block);

        let epoch = self.epoch_schedule.get_epoch_at_slot(slot);

        let write_session_single = self.write_sessions[0].get_write_session().await;

        let started_block = Instant::now();
        let inserted = postgres_block
            .save(&write_session_single, epoch.into())
            .await?;

        if !inserted {
            debug!("Block {} already exists - skip update", slot);
            return Ok(());
        }
        let elapsed_block_insert = started_block.elapsed();

        let started_txs = Instant::now();

        let mut queries_fut = Vec::new();
        let chunk_size =
            div_ceil(transactions.len(), self.write_sessions.len()).max(MIN_WRITE_CHUNK_SIZE);
        let chunks = transactions.chunks(chunk_size).collect_vec();
        assert!(
            chunks.len() <= self.write_sessions.len(),
            "cannot have more chunks than session"
        );
        for (i, chunk) in chunks.iter().enumerate() {
            let session = self.write_sessions[i].get_write_session().await.clone();
            let future = PostgresTransaction::save_transactions_from_block(session, epoch.into(), chunk);
            queries_fut.push(future);
        }
        let all_results: Vec<Result<()>> = futures_util::future::join_all(queries_fut).await;
        for result in all_results {
            result.expect("Save query must succeed");
        }

        let elapsed_txs_insert = started_txs.elapsed();

        info!(
            "Saving block {}@{} to postgres took {:.2}ms for block and {:.2}ms for {} transactions ({}x{} chunks)",
            slot, block.commitment_config.commitment,
            elapsed_block_insert.as_secs_f64() * 1000.0,
            elapsed_txs_insert.as_secs_f64() * 1000.0,
            transactions.len(),
            chunks.len(),
            chunk_size,
        );

        Ok(())
    }

    // ATM we focus on blocks as this table gets INSERTS and does deduplication checks (i.e. heavy reads on index pk_block_slot)
    pub async fn optimize_blocks_table(&self, slot: Slot) -> Result<()> {
        let started = Instant::now();
        let epoch: EpochRef = self.epoch_schedule.get_epoch_at_slot(slot).into();
        let random_session = slot as usize % self.write_sessions.len();
        let write_session_single = self.write_sessions[random_session]
            .get_write_session()
            .await;
        let statement = format!(
            r#"
                ANALYZE (SKIP_LOCKED) {schema}.blocks;
            "#,
            schema = PostgresEpoch::build_schema_name(epoch),
        );

        tokio::spawn(async move {
            write_session_single
                .execute_multiple(&statement)
                .await
                .unwrap();
            let elapsed = started.elapsed();
            debug!(
                "Postgres analyze of blocks table took {:.2}ms",
                elapsed.as_secs_f64() * 1000.0
            );
            if elapsed > Duration::from_millis(500) {
                warn!(
                    "Very slow postgres ANALYZE on slot {} - took {:.2}ms",
                    slot,
                    elapsed.as_secs_f64() * 1000.0
                );
            }
        });
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

    // used for testing only ATM
    pub async fn drop_epoch_schema(&self, epoch: EpochRef) -> anyhow::Result<()> {
        // create schema for new epoch
        let schema_name = PostgresEpoch::build_schema_name(epoch);
        let session = self.get_session().await;

        let statement = PostgresEpoch::build_drop_schema_statement(epoch);
        let result_drop_schema = session.execute_multiple(&statement).await;
        match result_drop_schema {
            Ok(_) => {
                warn!("Dropped schema {}", schema_name);
                Ok(())
            }
            Err(e) => {
                bail!("Error dropping schema {}", schema_name)
            }
        }
    }

}

fn build_assign_permissions_statements(epoch: EpochRef) -> String {
    let schema = PostgresEpoch::build_schema_name(epoch);
    format!(
        r#"
            GRANT USAGE ON SCHEMA {schema} TO {role};
            GRANT ALL ON ALL TABLES IN SCHEMA {schema} TO {role};
            ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT ALL ON TABLES TO {role};

            GRANT USAGE ON SCHEMA {schema} TO {query_role};
            ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT SELECT ON TABLES TO {query_role};
        "#,
        role = LITERPC_ROLE,
        query_role = LITERPC_QUERY_ROLE,
    )
}

fn div_ceil(a: usize, b: usize) -> usize {
    (a.saturating_add(b).saturating_sub(1)).saturating_div(b)
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

        let row_role = write_session
            .get_write_session()
            .await
            .query_one("SELECT current_role", &[])
            .await
            .unwrap();
        info!("row: {:?}", row_role);
    }

    #[tokio::test]
    #[ignore]
    async fn test_save_block() {
        tracing_subscriber::fmt::init();

        let pg_session_config = PostgresSessionConfig {
            pg_config: "host=localhost dbname=literpc3 user=literpc_app password=litelitesecret"
                .to_string(),
            ssl: None,
        };

        let _postgres_session_cache = PostgresSessionCache::new(pg_session_config.clone())
            .await
            .unwrap();
        let epoch_cache = EpochCache::new_for_tests();

        let postgres_block_store =
            PostgresBlockStore::new(epoch_cache.clone(), pg_session_config.clone()).await;

        postgres_block_store
            .save_block(&create_test_block())
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
            is_vote: false,
            err: None,
            cu_requested: Some(40000),
            prioritization_fees: Some(5000),
            cu_consumed: Some(32000),
            recent_blockhash: "recent_blockhash".to_string(),
            message: "some message".to_string(),
            writable_accounts: vec![],
            readable_accounts: vec![],
        }
    }
}
