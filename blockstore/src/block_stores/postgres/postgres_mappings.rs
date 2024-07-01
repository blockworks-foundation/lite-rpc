use bimap::BiMap;
use itertools::Itertools;
use log::{debug, info, trace};
use tracing::field::debug;
use solana_lite_rpc_core::structures::epoch::EpochRef;
use crate::block_stores::postgres::postgres_epoch::PostgresEpoch;
use crate::block_stores::postgres::PostgresSession;

pub fn build_create_transaction_mapping_table_statement(epoch: EpochRef) -> String {
    let schema = PostgresEpoch::build_schema_name(epoch);
    format!(
        r#"
                -- lookup table; maps signatures to generated int8 transaction ids
                -- no updates or deletes, only INSERTs
                CREATE TABLE {schema}.transaction_ids(
                    transaction_id bigserial NOT NULL,
                    signature varchar(88) NOT NULL,
                    PRIMARY KEY (transaction_id) INCLUDE(signature) WITH (FILLFACTOR=80),
	                UNIQUE(signature) INCLUDE (transaction_id) WITH (FILLFACTOR=80)
                ) WITH (FILLFACTOR=100, toast_tuple_target=128);
                -- signature might end up on TOAST which is okey because the data gets pulled from index
                ALTER TABLE {schema}.transaction_ids
                    SET (
                        autovacuum_vacuum_scale_factor=0,
                        autovacuum_vacuum_threshold=10000,
                        autovacuum_vacuum_insert_scale_factor=0,
                        autovacuum_vacuum_insert_threshold=50000,
                        autovacuum_analyze_scale_factor=0,
                        autovacuum_analyze_threshold=50000
                        );
            "#,
        schema = schema
    )
}

// note: sigantures might contain duplicates but that's quite rare and can be ignored for transactions
pub async fn perform_transaction_mapping(postgres_session: &PostgresSession, epoch: EpochRef, signatures: &[&str]) -> anyhow::Result<BiMap<String, i64>> {
    let schema = PostgresEpoch::build_schema_name(epoch);
    let statement = format!(
        r#"
            WITH
            sigs AS (
                SELECT signature from unnest($1::text[]) tx_sig(signature)
            ),
            inserted AS
            (
                INSERT INTO {schema}.transaction_ids(signature)
                    SELECT signature from sigs
                ON CONFLICT DO NOTHING
                RETURNING *
            ),
            existed AS
            (
                SELECT * FROM {schema}.transaction_ids WHERE transaction_id not in (SELECT transaction_id FROM inserted)
            )
            SELECT transaction_id, signature FROM inserted
            UNION ALL
            SELECT transaction_id, signature FROM existed
            "#,
        schema = schema
    );

    let mappings = postgres_session.query_list(statement.as_str(), &[&signatures]).await?;

    let mapping_pairs = mappings.iter()
        .map(|row| {
            let tx_id: i64 = row.get(0);
            let tx_sig: String = row.get(1);
            (tx_sig, tx_id)
        });

    // sig <-> tx_id
    let map = BiMap::from_iter(mapping_pairs);

    trace!("Transaction mapping from database: {:?}", map);
    Ok(map)
}


pub fn build_create_account_mapping_table_statement(epoch: EpochRef) -> String {
    let schema = PostgresEpoch::build_schema_name(epoch);
    format!(
        r#"
                -- lookup table; maps account pubkey to generated int8 acc_ids
                -- no updates or deletes, only INSERTs
                CREATE TABLE {schema}.account_ids(
                    acc_id bigserial NOT NULL,
                    account_key varchar(44) NOT NULL,
                    PRIMARY KEY (acc_id) INCLUDE(account_key) WITH (FILLFACTOR=80),
	                UNIQUE(account_key) INCLUDE (acc_id) WITH (FILLFACTOR=80)
                ) WITH (FILLFACTOR=100, toast_tuple_target=128);
                -- pubkey might end up on TOAST which is okey because the data gets pulled from index
                ALTER TABLE {schema}.account_ids
                    SET (
                        autovacuum_vacuum_scale_factor=0,
                        autovacuum_vacuum_threshold=10000,
                        autovacuum_vacuum_insert_scale_factor=0,
                        autovacuum_vacuum_insert_threshold=50000,
                        autovacuum_analyze_scale_factor=0,
                        autovacuum_analyze_threshold=50000
                        );
            "#,
        schema = schema
    )
}

// account_keys is deduped
pub async fn perform_account_mapping(postgres_session: &PostgresSession, epoch: EpochRef, account_keys: &[&str]) -> anyhow::Result<BiMap<String, i64>> {
    let schema = PostgresEpoch::build_schema_name(epoch);
    let statement = format!(
        r#"
           WITH
            account_keys AS (
                SELECT account_key from unnest($1::text[]) acc_sig(account_key)
            ),
            inserted AS
            (
                INSERT INTO {schema}.account_ids(account_key)
                    SELECT account_key from account_keys
                ON CONFLICT DO NOTHING
                RETURNING *
            ),
            existed AS
            (
                SELECT * FROM {schema}.account_ids WHERE acc_id not in (SELECT acc_id FROM inserted)
            )
            SELECT acc_id, account_key FROM inserted
            UNION ALL
            SELECT acc_id, account_key FROM existed
            "#,
        schema = schema
    );

    let mappings = postgres_session.query_list(statement.as_str(), &[&account_keys]).await?;

    let mapping_pairs = mappings.iter()
        .map(|row| {
            let acc_id: i64 = row.get(0);
            let account_key: String = row.get(1);
            (account_key, acc_id)
        });

    // pubkey <-> tx_id
    let map = BiMap::from_iter(mapping_pairs);

    trace!("Accounts mapping from database: {:?}", map);
    Ok(map)
}
