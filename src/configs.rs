use crate::encoding::BinaryEncoding;
use serde::{Deserialize, Serialize};
use solana_sdk::commitment_config::CommitmentLevel;
use solana_sdk::slot_history::Slot;

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SendTransactionConfig {
    #[serde(default)]
    pub skip_preflight: bool,
    #[serde(default)]
    pub preflight_commitment: CommitmentLevel,
    #[serde(default)]
    pub encoding: BinaryEncoding,
    pub max_retries: Option<u16>,
    pub min_context_slot: Option<Slot>,
}
