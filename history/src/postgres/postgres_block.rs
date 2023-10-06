use solana_lite_rpc_core::{encoding::BASE64, structures::produced_block::ProducedBlock};
use solana_sdk::slot_history::Slot;
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
    pub fn create_statement(schema: &String) -> String {
        format!(
            "
            CREATE TABLE {}.BLOCKS (
                slot BIGINT PRIMARY KEY,
                blockhash STRING NOT NULL,
                leader_id STRING,
                block_height BIGINT NOT NULL,
                parent_slot BIGINT NOT NULL,
                block_time BIGINT NOT NULL,
                previous_blockhash STRING NOT NULL,
                rewards STRING,
            );
        ",
            schema
        )
    }

    pub async fn save(
        &self,
        postgres_session: &PostgresSession,
        schema: &String,
    ) -> anyhow::Result<()> {
        let mut query = format!(
            r#"
            INSERT INTO {}.BLOCKS (slot, blockhash, block_height, parent_slot, block_time, previous_blockhash, rewards) VALUES 
        "#,
            schema
        );

        let mut args: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(NB_ARUMENTS);
        args.push(&self.slot);
        args.push(&self.blockhash);
        args.push(&self.block_height);
        args.push(&self.parent_slot);
        args.push(&self.block_time);
        args.push(&self.previous_blockhash);
        args.push(&self.rewards);

        PostgresSession::multiline_query(&mut query, NB_ARUMENTS, 1, &[]);
        postgres_session.execute(&query, &args).await?;
        Ok(())
    }

    pub async fn get(
        postgres_session: &PostgresSession,
        schema: &String,
        slot: Slot,
    ) -> anyhow::Result<PostgresBlock> {
        let statement = format!("SELECT  blockhash, block_height, parent_slot, block_time, previous_blockhash, rewards FROM {}.BLOCKS WHERE SLOT = {};", schema, slot);
        let row = postgres_session.client.query_one(&statement, &[]).await?;
        Ok(Self {
            slot: slot as i64,
            blockhash: row.get(0),
            block_height: row.get(1),
            parent_slot: row.get(2),
            block_time: row.get(3),
            previous_blockhash: row.get(4),
            rewards: row.get(5),
        })
    }
}
