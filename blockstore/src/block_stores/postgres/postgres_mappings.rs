use solana_lite_rpc_core::structures::epoch::EpochRef;
use crate::block_stores::postgres::postgres_epoch::PostgresEpoch;

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
