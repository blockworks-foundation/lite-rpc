use crate::utils::wait_for_merge_or_get_content;
use crate::utils::Takable;
use crate::vote::EpochVoteStakesCache;
use crate::vote::VoteMap;
use crate::vote::VoteStore;
use crate::Slot;
use futures_util::stream::FuturesUnordered;
use solana_lite_rpc_core::structures::leaderschedule::GetVoteAccountsConfig;
use solana_rpc_client_api::response::RpcVoteAccountStatus;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

pub struct RpcRequestData {
    pub rpc_notify_task: FuturesUnordered<JoinHandle<(u64, GetVoteAccountsConfig)>>,
    pub rpc_exec_task:
        FuturesUnordered<JoinHandle<(VoteMap, EpochVoteStakesCache, RpcVoteAccountStatus)>>,
    pending_rpc_request: Option<Vec<oneshot::Sender<RpcVoteAccountStatus>>>,
}

impl RpcRequestData {
    pub fn new() -> Self {
        RpcRequestData {
            rpc_notify_task: FuturesUnordered::new(),
            rpc_exec_task: FuturesUnordered::new(),
            pending_rpc_request: None,
        }
    }

    pub async fn process_get_vote_accounts(
        &mut self,
        current_slot: Slot,
        config: GetVoteAccountsConfig,
        return_channel: oneshot::Sender<RpcVoteAccountStatus>,
        votestore: &mut VoteStore,
    ) {
        match self.pending_rpc_request {
            Some(ref mut pending) => pending.push(return_channel),
            None => {
                self.pending_rpc_request = Some(vec![return_channel]);
            }
        }
        self.take_vote_accounts_and_process(votestore, current_slot, config)
            .await;
    }
    pub async fn notify_end_rpc_get_vote_accounts(
        &mut self,
        votes: VoteMap,
        vote_accounts: EpochVoteStakesCache,
        rpc_vote_accounts: RpcVoteAccountStatus,
        votestore: &mut VoteStore,
    ) {
        if let Err(err) = votestore.votes.merge((votes, vote_accounts)) {
            log::info!("Error during  RPC get vote account merge:{err}");
        }

        //avoid clone on the first request
        if let Some(ref mut pending_rpc_request) = self.pending_rpc_request {
            for return_channel in pending_rpc_request.drain(0..pending_rpc_request.len() - 1) {
                if return_channel.send(rpc_vote_accounts.clone()).is_err() {
                    log::error!("Vote accounts RPC channel send closed.");
                }
            }
            if pending_rpc_request
                .pop()
                .unwrap()
                .send(rpc_vote_accounts)
                .is_err()
            {
                log::error!("Vote accounts RPC channel send closed.");
            }
        }
    }

    pub async fn take_vote_accounts_and_process(
        &mut self,
        votestore: &mut VoteStore,
        current_slot: Slot,
        config: GetVoteAccountsConfig,
    ) {
        if let Some(((votes, vote_accounts), (current_slot, config))) =
            wait_for_merge_or_get_content(
                &mut votestore.votes,
                (current_slot, config),
                &mut self.rpc_notify_task,
            )
            .await
        {
            let jh = tokio::task::spawn_blocking({
                move || {
                    let rpc_vote_accounts = crate::vote::get_rpc_vote_accounts_info(
                        current_slot,
                        &votes,
                        //TODO manage missing epoch and return error.
                        &vote_accounts
                            .vote_stakes_for_epoch(0)
                            .as_ref()
                            .unwrap()
                            .vote_stakes,
                        config,
                    );
                    (votes, vote_accounts, rpc_vote_accounts)
                }
            });
            self.rpc_exec_task.push(jh);
        }
    }
}
