use solana_lite_rpc_core::structures::epoch::EpochRef;

pub struct PostgresEpoch {}

pub const EPOCH_SCHEMA_PREFIX: &str = "rpc2a_epoch_";

impl PostgresEpoch {
    // e.g. rpc2a_epoch_644 - rpc2a = RPCv2 alpha
    pub fn build_schema_name(epoch: EpochRef) -> String {
        format!("{}{}", EPOCH_SCHEMA_PREFIX, epoch.get_epoch())
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

    pub fn build_drop_schema_statement(epoch: EpochRef) -> String {
        let schema = PostgresEpoch::build_schema_name(epoch);
        format!(
            "
            DROP SCHEMA IF EXISTS {} CASCADE;
            ",
            schema
        )
    }

    pub fn parse_epoch_from_schema_name(schema_name: &str) -> EpochRef {
        let epoch_number_str = schema_name.trim_start_matches(EPOCH_SCHEMA_PREFIX);
        let epoch = epoch_number_str.parse::<u64>().unwrap();
        EpochRef::new(epoch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_schema_name() {
        let epoch = EpochRef::new(644);
        let schema = PostgresEpoch::build_schema_name(epoch);
        assert_eq!("rpc2a_epoch_644", schema);
    }

    #[test]
    fn test_parse_epoch_from_schema_name() {
        let schema = "rpc2a_epoch_644";
        let epoch = PostgresEpoch::parse_epoch_from_schema_name(schema);
        assert_eq!(644, epoch.get_epoch());
    }
}
