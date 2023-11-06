use solana_lite_rpc_core::structures::epoch::EpochRef;

pub struct PostgresEpoch {}

impl PostgresEpoch {
    // e.g. lite_rpc_epoch_644
    pub fn build_schema_name(epoch: EpochRef) -> String {
        format!("lite_rpc_epoch_{}", epoch.get_epoch())
    }

    pub fn build_create_table_statement(epoch: EpochRef) -> String {
        let schema = PostgresEpoch::build_schema_name(epoch);
        format!(
            "
            CREATE SCHEMA {} AUTHORIZATION CURRENT_ROLE;
            ",
            schema
        )
    }
}
