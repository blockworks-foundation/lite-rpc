use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::hash::Hash;
use solana_sdk::message::v0::{LoadedAddresses, MessageAddressTableLookup};
use solana_sdk::message::VersionedMessage;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_sdk::transaction::VersionedTransaction;
use solana_sdk::transaction_context::TransactionReturnData;
use solana_sdk::{slot_history::Slot, transaction::TransactionError};
use solana_transaction_status::{
    InnerInstructions, Reward, Rewards, TransactionStatusMeta, TransactionTokenBalance,
    UiTransactionTokenBalance,
};
use std::fmt::Debug;
use std::ops::Deref;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct TransactionInfo {
    pub signature: Signature,
    // index sent by yellowstone
    pub index: i32,
    pub is_vote: bool,
    pub err: Option<TransactionError>,
    pub cu_requested: Option<u32>,
    pub prioritization_fees: Option<u64>,
    pub cu_consumed: Option<u64>,
    pub recent_blockhash: Hash,
    pub message: VersionedMessage,
    pub writable_accounts: Vec<Pubkey>,
    pub readable_accounts: Vec<Pubkey>,
    pub address_lookup_tables: Vec<MessageAddressTableLookup>,
    pub fee: i64,
    pub pre_balances: Vec<i64>,
    pub post_balances: Vec<i64>,
    pub inner_instructions: Option<Vec<InnerInstructions>>,
    pub log_messages: Option<Vec<String>>,
    pub pre_token_balances: Vec<UiTransactionTokenBalance>,
    pub post_token_balances: Vec<UiTransactionTokenBalance>,
    // from TransactionStatusMeta

    // pub pre_token_balances: Option<Vec<TransactionTokenBalance>>,
    // pub post_token_balances: Option<Vec<TransactionTokenBalance>>,
    // pub rewards: Option<Rewards>,
    // pub return_data: Option<TransactionReturnData>,
}

#[derive(Clone)]
pub struct ProducedBlock {
    // Arc is required for channels
    inner: Arc<ProducedBlockInner>,
    pub commitment_config: CommitmentConfig,
}

impl ProducedBlock {
    pub fn new(inner: ProducedBlockInner, commitment_config: CommitmentConfig) -> Self {
        ProducedBlock {
            inner: Arc::new(inner),
            commitment_config,
        }
    }
}

/// # Example
/// ```text
/// ProducedBlock { slot: 254169151, commitment_config: processed, blockhash: BULfZwLswkDbHhTrHGDASUtmNAG8gk6TV2njnobjYLyd, transactions_count: 806 }
/// ```
impl Debug for ProducedBlock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ProducedBlock {{ slot: {}, commitment_config: {}, blockhash: {}, transactions_count: {} }}",
               self.slot, self.commitment_config.commitment, self.blockhash, self.transactions.len())
    }
}

impl Deref for ProducedBlock {
    type Target = ProducedBlockInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

pub struct ProducedBlockInner {
    pub transactions: Vec<TransactionInfo>,
    pub leader_id: Option<String>,
    pub blockhash: Hash,
    pub block_height: u64,
    pub slot: Slot,
    pub parent_slot: Slot,
    // seconds since epoch
    pub block_time: u64,
    pub previous_blockhash: Hash,
    pub rewards: Option<Vec<Reward>>,
}

impl ProducedBlock {
    /// moving commitment level to confirmed
    pub fn to_confirmed_block(&self) -> Self {
        ProducedBlock {
            inner: self.inner.clone(),
            commitment_config: CommitmentConfig::confirmed(),
        }
    }

    /// moving commitment level to finalized
    pub fn to_finalized_block(&self) -> Self {
        ProducedBlock {
            inner: self.inner.clone(),
            commitment_config: CommitmentConfig::finalized(),
        }
    }
}

impl From<&TransactionInfo> for VersionedTransaction {
    fn from(ti: &TransactionInfo) -> Self {
        let tx: VersionedTransaction = VersionedTransaction {
            signatures: vec![ti.signature], // TODO check if it's correct to map only one signature
            message: ti.message.clone(),
        };
        tx
    }
}
