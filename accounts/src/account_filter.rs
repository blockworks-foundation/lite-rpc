use serde::{Deserialize, Serialize};
use solana_rpc_client_api::filter::RpcFilterType;
use solana_sdk::pubkey::Pubkey;

#[derive(Serialize, Deserialize, Clone)]
pub struct AccountFilter {
    pub program_id: Pubkey,
    pub filters: Option<Vec<RpcFilterType>>,
}

pub type AccountFilters = Vec<AccountFilter>;
