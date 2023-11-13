use crate::postgres::postgres_epoch::PostgresEpoch;
use log::{debug, warn};
use solana_lite_rpc_core::structures::epoch::EpochRef;
use solana_lite_rpc_core::{encoding::BASE64, structures::produced_block::ProducedBlock};
use std::time::Instant;
use tokio_postgres::types::ToSql;

use super::postgres_session::PostgresSession;

#[derive(Debug)]
pub struct PostgresBlock {
    pub slot: i64,
    pub blockhash: String,
    pub block_height: i64,
    pub parent_slot: i64,
    pub block_time: i64,
    pub previous_blockhash: String,
    pub rewards: Option<String>,
}

const NB_ARUMENTS: usize = 7;

impl From<&ProducedBlock> for PostgresBlock {
    fn from(value: &ProducedBlock) -> Self {
        let rewards = value
            .rewards
            .as_ref()
            .map(|x| BASE64.serialize(x).ok())
            .unwrap_or(None);

        Self {
            blockhash: value.blockhash.clone(),
            block_height: value.block_height as i64,
            slot: value.slot as i64,
            parent_slot: value.parent_slot as i64,
            block_time: value.block_time as i64,
            previous_blockhash: value.previous_blockhash.clone(),
            rewards,
        }
    }
}

impl PostgresBlock {
    pub fn build_create_table_statement(epoch: EpochRef) -> String {
        let schema = PostgresEpoch::build_schema_name(epoch);
        format!(
            r#"
            CREATE TABLE IF NOT EXISTS {schema}.blocks (
                slot BIGINT NOT NULL,
                blockhash TEXT NOT NULL,
                leader_id TEXT,
                block_height BIGINT NOT NULL,
                parent_slot BIGINT NOT NULL,
                block_time BIGINT NOT NULL,
                previous_blockhash TEXT NOT NULL,
                rewards TEXT,
                CONSTRAINT pk_block_slot PRIMARY KEY(slot)
            );
            CLUSTER {schema}.blocks USING pk_block_slot;
        "#,
            schema = schema
        )
    }

    pub async fn save(
        &self,
        postgres_session: &PostgresSession,
        epoch: EpochRef,
    ) -> anyhow::Result<()> {
        let started = Instant::now();
        let schema = PostgresEpoch::build_schema_name(epoch);
        let values = PostgresSession::values_vecvec(NB_ARUMENTS, 1, &[]);

        let statement = format!(
            r#"
                INSERT INTO {schema}.blocks (slot, blockhash, block_height, parent_slot, block_time, previous_blockhash, rewards)
                VALUES {}
                -- prevent updates
                ON CONFLICT DO NOTHING
                RETURNING (
                    -- get previous max slot
                    SELECT max(all_blocks.slot) as prev_max_slot
                    FROM {schema}.blocks AS all_blocks
                    WHERE all_blocks.slot!={schema}.blocks.slot
                )
            "#,
            values,
            schema = schema,
        );

        let mut args: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(NB_ARUMENTS);
        args.push(&self.slot);
        args.push(&self.blockhash);
        args.push(&self.block_height);
        args.push(&self.parent_slot);
        args.push(&self.block_time);
        args.push(&self.previous_blockhash);
        args.push(&self.rewards);

        let returning = postgres_session
            .execute_and_return(&statement, &args)
            .await?;

        // TODO: decide what to do if block already exists
        match returning {
            Some(row) => {
                // check if monotonic
                let prev_max_slot = row.get::<&str, Option<i64>>("prev_max_slot");
                // None -> no previous rows
                debug!(
                    "Inserted block {} with prev highest slot being {}, parent={}",
                    self.slot,
                    prev_max_slot.unwrap_or(-1),
                    self.parent_slot
                );
                if let Some(prev_max_slot) = prev_max_slot {
                    if prev_max_slot > self.slot {
                        // note: unclear if this is desired behavior!
                        warn!(
                            "Block {} was inserted behind tip of highest slot number {} (epoch {})",
                            self.slot, prev_max_slot, epoch
                        );
                    }
                }
            }
            None => {
                // database detected conflict
                warn!("Block {} already exists - not updated", self.slot);
            }
        }

        debug!(
            "Inserting block row to postgres took {:.2}ms",
            started.elapsed().as_secs_f64() * 1000.0
        );

        Ok(())
    }
}
