use crate::Slot;
use dashmap::DashMap;
use solana_lite_rpc_core::stores::vote_store::EpochVoteStakes;
use solana_lite_rpc_core::stores::vote_store::VoteMap;
use solana_lite_rpc_core::stores::vote_store::VoteStore;
use solana_lite_rpc_core::structures::leaderschedule::GetVoteAccountsConfig;
use solana_lite_rpc_core::structures::stored_vote::StoredVote;
use solana_rpc_client_api::response::RpcVoteAccountInfo;
use solana_rpc_client_api::response::RpcVoteAccountStatus;
use solana_sdk::account::Account;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::vote::state::VoteState;

pub fn merge_program_account_in_vote_map(
    vote_map: &mut VoteMap,
    pa_list: Vec<(Pubkey, Account)>,
    last_update_slot: Slot,
) {
    pa_list
        .into_iter()
        .filter_map(
            |(pk, account)| match VoteState::deserialize(&account.data) {
                Ok(vote) => Some((pk, vote)),
                Err(err) => {
                    log::warn!("Error during vote account data deserialisation:{err}");
                    None
                }
            },
        )
        .for_each(|(pk, vote)| {
            //log::info!("Vote init {pk} :{vote:?}");
            let vote = StoredVote {
                pubkey: pk,
                vote_data: vote,
                last_update_slot,
                write_version: 0,
            };
            VoteStore::vote_map_insert_vote(vote_map, vote);
        });
}

// Validators that are this number of slots behind are considered delinquent
pub fn get_rpc_vote_accounts_info(
    current_slot: Slot,
    votes: &VoteMap,
    vote_accounts: &DashMap<Pubkey, (u64, StoredVote)>,
    config: GetVoteAccountsConfig,
) -> RpcVoteAccountStatus {
    pub const DELINQUENT_VALIDATOR_SLOT_DISTANCE: u64 =
        solana_rpc_client_api::request::DELINQUENT_VALIDATOR_SLOT_DISTANCE;
    let delinquent_validator_slot_distance = config
        .delinquent_slot_distance
        .unwrap_or(DELINQUENT_VALIDATOR_SLOT_DISTANCE);
    //From Solana rpc::rpc::metaz::get_vote_accounts() code.
    let (current_vote_accounts, delinquent_vote_accounts): (
        Vec<RpcVoteAccountInfo>,
        Vec<RpcVoteAccountInfo>,
    ) = votes
        .iter()
        .map(|iter| {
            let vote = iter.value();
            let (stake, epoch_vote_account) = vote_accounts
                .get(&vote.pubkey)
                .map(|iter| (iter.0, true))
                .unwrap_or((0, false));
            vote.convert_to_rpc_vote_account_info(stake, epoch_vote_account)
        })
        .partition(|vote_account_info| {
            if current_slot >= delinquent_validator_slot_distance {
                vote_account_info.last_vote > current_slot - delinquent_validator_slot_distance
            } else {
                vote_account_info.last_vote > 0
            }
        });
    let keep_unstaked_delinquents = config.keep_unstaked_delinquents.unwrap_or_default();
    let delinquent_vote_accounts = if !keep_unstaked_delinquents {
        delinquent_vote_accounts
            .into_iter()
            .filter(|vote_account_info| vote_account_info.activated_stake > 0)
            .collect::<Vec<_>>()
    } else {
        delinquent_vote_accounts
    };

    RpcVoteAccountStatus {
        current: current_vote_accounts,
        delinquent: delinquent_vote_accounts,
    }
}

pub fn get_rpc_vote_account_info_from_current_epoch_stakes(
    current_epoch_stakes: &EpochVoteStakes,
) -> RpcVoteAccountStatus {
    let current_vote_accounts: Vec<RpcVoteAccountInfo> = current_epoch_stakes
        .vote_stakes
        .iter()
        .map(|iter| iter.1.convert_to_rpc_vote_account_info(iter.0, true))
        .collect();
    RpcVoteAccountStatus {
        current: current_vote_accounts,
        delinquent: vec![], //no info about delinquent at startup.
    }
}
