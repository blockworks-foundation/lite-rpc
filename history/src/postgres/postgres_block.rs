use log::warn;
use solana_lite_rpc_core::{encoding::BASE64, structures::produced_block::ProducedBlock};
use tokio_postgres::types::ToSql;
use solana_lite_rpc_core::structures::epoch::EpochRef;
use crate::postgres::postgres_epoch::PostgresEpoch;

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
            "
            CREATE TABLE IF NOT EXISTS {}.blocks (
                slot BIGINT PRIMARY KEY,
                blockhash TEXT NOT NULL,
                leader_id TEXT,
                block_height BIGINT NOT NULL,
                parent_slot BIGINT NOT NULL,
                block_time BIGINT NOT NULL,
                previous_blockhash TEXT NOT NULL,
                rewards TEXT
            );
        ",
            schema
        )
    }

    pub async fn save(
        &self,
        postgres_session: &PostgresSession,
        epoch: EpochRef,
    ) -> anyhow::Result<()> {
        let schema = PostgresEpoch::build_schema_name(epoch);
        let values = PostgresSession::values_vecvec(NB_ARUMENTS, 1, &[]);
        let statement = format!(
            r#"
                INSERT INTO {}.blocks (slot, blockhash, block_height, parent_slot, block_time, previous_blockhash, rewards)
                VALUES {} ON CONFLICT DO NOTHING;
            "#,
            schema,
            values
        );

        let mut args: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(NB_ARUMENTS);
        args.push(&self.slot);
        args.push(&self.blockhash);
        args.push(&self.block_height);
        args.push(&self.parent_slot);
        args.push(&self.block_time);
        args.push(&self.previous_blockhash);
        args.push(&self.rewards);

        let inserted = postgres_session.execute(&statement, &args).await?;

        // TODO: decide what to do if block already exists
        if inserted == 0 {
            warn!("Block {} already exists - not updated", self.slot);
        }

        Ok(())
    }
}
