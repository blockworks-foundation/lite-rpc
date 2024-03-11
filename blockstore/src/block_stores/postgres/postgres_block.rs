use super::postgres_epoch::PostgresEpoch;
use super::postgres_session::PostgresSession;
use log::{debug, warn};
use solana_lite_rpc_core::solana_utils::hash_from_str;
use solana_lite_rpc_core::structures::epoch::EpochRef;
use solana_lite_rpc_core::structures::produced_block::{ProducedBlockInner, TransactionInfo};
use solana_lite_rpc_core::{encoding::BASE64, structures::produced_block::ProducedBlock};
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_transaction_status::Reward;
use std::time::Instant;
use itertools::Itertools;
use serde_json::Value;
use tokio_postgres::types::ToSql;
use crate::block_stores::postgres::{json_deserialize, json_serialize};

#[derive(Debug)]
pub struct PostgresBlock {
    pub slot: i64,
    pub blockhash: String,
    pub block_height: i64,
    pub parent_slot: i64,
    pub block_time: i64,
    pub previous_blockhash: String,
    pub rewards: Option<Vec<Value>>,
    pub leader_id: Option<String>,
}

impl From<&ProducedBlock> for PostgresBlock {
    fn from(value: &ProducedBlock) -> Self {
        let rewards_vec = value.rewards.as_ref()
            .map(|list| list.iter().map(|r| json_serialize::<Reward>(r)).collect_vec());

        Self {
            blockhash: value.blockhash.to_string(),
            block_height: value.block_height as i64,
            slot: value.slot as i64,
            parent_slot: value.parent_slot as i64,
            block_time: value.block_time as i64,
            previous_blockhash: value.previous_blockhash.to_string(),
            rewards: rewards_vec,
            leader_id: value.leader_id.clone(),
        }
    }
}

impl PostgresBlock {
    pub fn to_produced_block(
        &self,
        transaction_infos: Vec<TransactionInfo>,
        commitment_config: CommitmentConfig,
    ) -> ProducedBlock {
        let rewards_vec: Option<Vec<Reward>> = self
            .rewards.clone()
            .map(|list | list.into_iter().map(|r| json_deserialize(r)).collect_vec());

        let inner = ProducedBlockInner {
            // TODO implement
            transactions: transaction_infos,
            leader_id: None,
            blockhash: hash_from_str(&self.blockhash).expect("valid blockhash"),
            block_height: self.block_height as u64,
            slot: self.slot as Slot,
            parent_slot: self.parent_slot as Slot,
            block_time: self.block_time as u64,
            previous_blockhash: hash_from_str(&self.previous_blockhash).expect("valid blockhash"),
            rewards: rewards_vec,
        };
        ProducedBlock::new(inner, commitment_config)
    }
}

impl PostgresBlock {
    pub fn build_create_table_statement(epoch: EpochRef) -> String {
        let schema = PostgresEpoch::build_schema_name(epoch);
        format!(
            r#"
            CREATE TABLE IF NOT EXISTS {schema}.blocks (
                slot BIGINT NOT NULL,
                block_height BIGINT NOT NULL,
                parent_slot BIGINT NOT NULL,
                block_time BIGINT NOT NULL,
                blockhash varchar(44) COMPRESSION lz4 NOT NULL,
                previous_blockhash varchar(44) COMPRESSION lz4 NOT NULL,
                leader_id TEXT COMPRESSION lz4,
                rewards jsonb[] COMPRESSION lz4,
                CONSTRAINT pk_block_slot PRIMARY KEY(slot)
            ) WITH (FILLFACTOR=90,TOAST_TUPLE_TARGET=128);
            ALTER TABLE {schema}.blocks ALTER COLUMN blockhash SET STORAGE MAIN;
            ALTER TABLE {schema}.blocks ALTER COLUMN previous_blockhash SET STORAGE MAIN;
            ALTER TABLE {schema}.blocks
                SET (
                    autovacuum_vacuum_scale_factor=0,
                    autovacuum_vacuum_threshold=1000,
                    autovacuum_vacuum_insert_scale_factor=0,
                    autovacuum_vacuum_insert_threshold=100,
                    autovacuum_analyze_scale_factor=0,
                    autovacuum_analyze_threshold=100
                    );
        "#,
            schema = schema
        )
    }

    pub fn build_query_statement(epoch: EpochRef, slot: Slot) -> String {
        format!(
            r#"
                SELECT
                    slot, blockhash, block_height, parent_slot, block_time, previous_blockhash, rewards, leader_id,
                    {epoch}::bigint as _epoch, '{schema}'::text as _epoch_schema FROM {schema}.blocks
                WHERE slot = {slot}
            "#,
            schema = PostgresEpoch::build_schema_name(epoch),
            epoch = epoch,
            slot = slot
        )
    }

    // true is actually inserted; false if operation was noop
    pub async fn save(
        &self,
        postgres_session: &PostgresSession,
        epoch: EpochRef,
    ) -> anyhow::Result<bool> {
        const NB_ARGUMENTS: usize = 8;

        let started = Instant::now();
        let schema = PostgresEpoch::build_schema_name(epoch);
        let values = PostgresSession::values_vec(NB_ARGUMENTS, &[]);

        let statement = format!(
            r#"
                INSERT INTO {schema}.blocks (slot, block_height, parent_slot, block_time, blockhash, previous_blockhash, leader_id, rewards)
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

        let mut args: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(NB_ARGUMENTS);
        args.push(&self.slot);
        args.push(&self.block_height);
        args.push(&self.parent_slot);
        args.push(&self.block_time);
        args.push(&self.blockhash);
        args.push(&self.previous_blockhash);
        args.push(&self.leader_id);
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
                debug!("Inserted block {}, epoch={}", self.slot, epoch);
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
                return Ok(false);
            }
        }

        debug!(
            "Inserting block {} to schema {} postgres took {:.2}ms",
            self.slot,
            schema,
            started.elapsed().as_secs_f64() * 1000.0
        );

        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::hash::Hash;
    use solana_sdk::message::{v0, MessageHeader, VersionedMessage};
    use solana_sdk::pubkey::Pubkey;
    use solana_sdk::{commitment_config::CommitmentConfig, signature::Signature};

    #[test]
    fn map_postgresblock_to_produced_block() {
        let block = PostgresBlock {
            slot: 5050505,
            blockhash: Hash::new_unique().to_string(),
            block_height: 4040404,
            parent_slot: 5050500,
            block_time: 12121212,
            previous_blockhash: Hash::new_unique().to_string(),
            rewards: None,
            leader_id: None,
        };

        let transaction_infos = vec![create_tx_info(), create_tx_info()];

        let produced_block =
            block.to_produced_block(transaction_infos, CommitmentConfig::confirmed());

        assert_eq!(produced_block.slot, 5050505);
        assert_eq!(produced_block.transactions.len(), 2);
    }

    fn create_tx_info() -> TransactionInfo {
        TransactionInfo {
            signature: Signature::new_unique(),
            index: 0,
            is_vote: false,
            err: None,
            cu_requested: None,
            prioritization_fees: None,
            cu_consumed: None,
            recent_blockhash: solana_sdk::hash::Hash::new_unique(),
            message: create_test_message(),
            writable_accounts: vec![],
            readable_accounts: vec![],
            address_lookup_tables: vec![],
            fee: 0,
            pre_balances: vec![],
            post_balances: vec![],
            inner_instructions: None,
            log_messages: None,
        }
    }

    fn create_test_message() -> VersionedMessage {
        VersionedMessage::V0(v0::Message {
            header: MessageHeader {
                num_required_signatures: 1,
                ..MessageHeader::default()
            },
            account_keys: vec![Pubkey::new_unique()],
            ..v0::Message::default()
        })
    }
}
