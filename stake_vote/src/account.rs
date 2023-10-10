use anyhow::bail;
use borsh::BorshDeserialize;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::stake::state::Delegation;
use solana_sdk::stake::state::StakeState;
use solana_sdk::vote::state::VoteState;
use yellowstone_grpc_proto::prelude::SubscribeUpdateAccount;

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
    pub fn new_from_geyzer(
        geyser_account: SubscribeUpdateAccount,
        current_slot: u64,
    ) -> Option<AccountPretty> {
        let Some(inner_account) = geyser_account.account else {
        log::warn!("Receive a SubscribeUpdateAccount without account.");
        return None;
    };

        if geyser_account.slot != current_slot {
            log::trace!(
                "Get geyser account on a different slot:{} of the current:{current_slot}",
                geyser_account.slot
            );
        }

        Some(AccountPretty {
            is_startup: geyser_account.is_startup,
            slot: geyser_account.slot,
            pubkey: Pubkey::try_from(inner_account.pubkey).expect("valid pubkey"),
            lamports: inner_account.lamports,
            owner: Pubkey::try_from(inner_account.owner).expect("valid pubkey"),
            executable: inner_account.executable,
            rent_epoch: inner_account.rent_epoch,
            data: inner_account.data,
            write_version: inner_account.write_version,
            txn_signature: bs58::encode(inner_account.txn_signature.unwrap_or_default())
                .into_string(),
        })
    }

    pub fn read_stake(&self) -> anyhow::Result<Option<Delegation>> {
        if self.data.is_empty() {
            log::warn!("Stake account with empty data. Can't read vote.");
            bail!("Error: read Stake account with empty data");
        }

        if self.data.is_empty() {
            log::warn!("Stake account with empty data. Can't read stake.");
            bail!("Error: read Stake account with empty data");
        }
        match BorshDeserialize::deserialize(&mut self.data.as_slice())? {
            StakeState::Stake(_, stake) => Ok(Some(stake.delegation)),
            StakeState::Initialized(_) => Ok(None),
            StakeState::Uninitialized => Ok(None),
            StakeState::RewardsPool => Ok(None),
        }
    }

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
            self.pubkey.to_string(),
            self.slot,
            self.lamports
        )
    }
}
