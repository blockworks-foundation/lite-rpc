use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(C)]
pub enum Commitment {
    Processed = 0,
    Confirmed = 1,
    Finalized = 2,
}

impl From<&CommitmentLevel> for Commitment {
    #[allow(deprecated)]
    fn from(value: &CommitmentLevel) -> Self {
        match value {
            CommitmentLevel::Finalized | CommitmentLevel::Root | CommitmentLevel::Max => {
                Commitment::Finalized
            }
            CommitmentLevel::Confirmed
            | CommitmentLevel::Single
            | CommitmentLevel::SingleGossip => Commitment::Confirmed,
            CommitmentLevel::Processed | CommitmentLevel::Recent => Commitment::Processed,
        }
    }
}

impl From<CommitmentLevel> for Commitment {
    #[allow(deprecated)]
    fn from(value: CommitmentLevel) -> Self {
        match value {
            CommitmentLevel::Finalized | CommitmentLevel::Root | CommitmentLevel::Max => {
                Commitment::Finalized
            }
            CommitmentLevel::Confirmed
            | CommitmentLevel::Single
            | CommitmentLevel::SingleGossip => Commitment::Confirmed,
            CommitmentLevel::Processed | CommitmentLevel::Recent => Commitment::Processed,
        }
    }
}

impl From<CommitmentConfig> for Commitment {
    fn from(value: CommitmentConfig) -> Self {
        value.commitment.into()
    }
}

impl From<&CommitmentConfig> for Commitment {
    fn from(value: &CommitmentConfig) -> Self {
        value.commitment.into()
    }
}

impl Commitment {
    pub fn into_commitment_level(&self) -> CommitmentLevel {
        match self {
            Commitment::Confirmed => CommitmentLevel::Confirmed,
            Commitment::Processed => CommitmentLevel::Processed,
            Commitment::Finalized => CommitmentLevel::Finalized,
        }
    }

    pub fn into_commiment_config(&self) -> CommitmentConfig {
        CommitmentConfig {
            commitment: self.into_commitment_level(),
        }
    }
}
