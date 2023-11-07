use solana_lite_rpc_core::structures::epoch::EpochRef;

pub struct PostgresEpoch {}

impl PostgresEpoch {
    // e.g. rpc2a_epoch_644 - rpc2a = RPCv2 alpha
    pub fn build_schema_name(epoch: EpochRef) -> String {
        format!("rpc2a_epoch_{}", epoch.get_epoch())
    }

    pub fn build_create_schema_statement(epoch: EpochRef) -> String {
        let schema = PostgresEpoch::build_schema_name(epoch);
        format!(
            "
            CREATE SCHEMA {};
            ",
            schema
        )
    }

}
