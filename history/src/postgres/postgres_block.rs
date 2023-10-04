use super::postgres_session::SchemaSize;
use solana_lite_rpc_core::{
    commitment_utils::Commitment, encoding::BASE64, structures::produced_block::ProducedBlock,
};

#[derive(Debug)]
pub struct PostgresBlock {
    pub leader_id: Option<String>,
    pub blockhash: String,
    pub block_height: i64,
    pub slot: i64,
    pub parent_slot: i64,
    pub block_time: i64,
    pub commitment_config: i8,
    pub previous_blockhash: String,
    pub rewards: Option<String>,
}

impl SchemaSize for PostgresBlock {
    const DEFAULT_SIZE: usize = 4 * 8;
    const MAX_SIZE: usize = Self::DEFAULT_SIZE + 8;
}

impl From<ProducedBlock> for PostgresBlock {
    fn from(value: ProducedBlock) -> Self {
        let commitment = Commitment::from(&value.commitment_config);
        let rewards = value
            .rewards
            .as_ref()
            .map(|x| BASE64.serialize(x).ok())
            .unwrap_or(None);

        Self {
            leader_id: value.leader_id,
            blockhash: value.blockhash,
            block_height: value.block_height as i64,
            slot: value.slot as i64,
            parent_slot: value.parent_slot as i64,
            block_time: value.block_time as i64,
            commitment_config: commitment as i8,
            previous_blockhash: value.previous_blockhash,
            rewards,
        }
    }
}
