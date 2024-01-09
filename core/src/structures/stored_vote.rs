use serde::{Deserialize, Serialize};
use solana_rpc_client_api::response::RpcVoteAccountInfo;
use solana_sdk::{pubkey::Pubkey, slot_history::Slot, vote::state::VoteState};

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct StoredVote {
    pub pubkey: Pubkey,
    pub vote_data: VoteState,
    pub last_update_slot: Slot,
    pub write_version: u64,
}

impl StoredVote {
    pub fn convert_to_rpc_vote_account_info(
        &self,
        activated_stake: u64,
        epoch_vote_account: bool,
    ) -> RpcVoteAccountInfo {
        let last_vote = self
            .vote_data
            .votes
            .iter()
            .last()
            .map(|vote| vote.slot())
            .unwrap_or_default();

        RpcVoteAccountInfo {
            vote_pubkey: self.pubkey.to_string(),
            node_pubkey: self.vote_data.node_pubkey.to_string(),
            activated_stake,
            commission: self.vote_data.commission,
            epoch_vote_account,
            epoch_credits: self.vote_data.epoch_credits.clone(),
            last_vote,
            root_slot: self.vote_data.root_slot.unwrap_or_default(),
        }
    }
}
