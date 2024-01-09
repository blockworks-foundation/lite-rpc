use anyhow::bail;
use borsh::BorshDeserialize;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::stake::state::Delegation;
use solana_sdk::stake::state::StakeState;
use solana_sdk::stake_history::StakeHistory;
use solana_sdk::vote::state::VoteState;

#[derive(Debug)]
#[allow(dead_code)]
pub struct AccountPretty {
    pub is_startup: bool,
    pub slot: u64,
    pub pubkey: Pubkey,
    pub lamports: u64,
    pub owner: Pubkey,
    pub executable: bool,
    pub rent_epoch: u64,
    pub data: Vec<u8>,
    pub write_version: u64,
    pub txn_signature: String,
}

impl AccountPretty {
    pub fn read_stake(&self) -> anyhow::Result<Option<Delegation>> {
        read_stake_from_account_data(self.data.as_slice())
    }

    // pub fn read_stake_history(&self) -> Option<StakeHistory> {
    //     read_historystake_from_account(self.data.as_slice())
    // }

    pub fn read_vote(&self) -> anyhow::Result<VoteState> {
        if self.data.is_empty() {
            log::warn!("Vote account with empty data. Can't read vote.");
            bail!("Error: read Vote account with empty data");
        }
        Ok(VoteState::deserialize(&self.data)?)
    }
}

impl std::fmt::Display for AccountPretty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} at slot:{} lpt:{}",
            self.pubkey, self.slot, self.lamports
        )
    }
}

pub fn read_stake_from_account_data(mut data: &[u8]) -> anyhow::Result<Option<Delegation>> {
    if data.is_empty() {
        log::warn!("Stake account with empty data. Can't read stake.");
        bail!("Error: read Stake account with empty data");
    }
    match BorshDeserialize::deserialize(&mut data)? {
        StakeState::Stake(_, stake) => Ok(Some(stake.delegation)),
        StakeState::Initialized(_) => Ok(None),
        StakeState::Uninitialized => Ok(None),
        StakeState::RewardsPool => Ok(None),
    }
}

pub fn read_historystake_from_account(account_data: &[u8]) -> Option<StakeHistory> {
    //solana_sdk::account::from_account::<StakeHistory, _>(&AccountSharedData::from(account))
    bincode::deserialize(account_data).ok()
}
