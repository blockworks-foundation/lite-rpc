use log::info;
use rust_debugging_locks::stacktrace_util::{backtrack_frame, BacktrackError, Stracktrace};
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::{slot_history::Slot, transaction::TransactionError};
use solana_transaction_status::Reward;

#[derive(Debug, Clone)]
pub struct TransactionInfo {
    pub signature: String,
    pub is_vote: bool,
    pub err: Option<TransactionError>,
    pub cu_requested: Option<u32>,
    pub prioritization_fees: Option<u64>,
    pub cu_consumed: Option<u64>,
    pub recent_blockhash: String,
    pub message: String,
}

// TODO try to remove Clone
#[derive(Debug, Clone)]
pub struct ProducedBlock {
    pub transactions: Vec<TransactionInfo>,
    pub leader_id: Option<String>,
    pub blockhash: String,
    pub block_height: u64,
    pub slot: Slot,
    pub parent_slot: Slot,
    pub block_time: u64,
    pub commitment_config: CommitmentConfig,
    pub previous_blockhash: String,
    pub rewards: Option<Vec<Reward>>,
    pub bloat: Bloat,
}
pub const BLOAT_SIZE: usize = 100;

#[derive(Debug, Clone)]
pub struct Bloat {
    bloat: [u8; BLOAT_SIZE],
}

impl Bloat {
    pub fn new() -> Self {
        let stracktrace = get_stacktrace().unwrap();
        // log_frames("Bloat::new", &stracktrace);
        Bloat {
            bloat: [0; BLOAT_SIZE],
        }
    }

}

fn log_frames(msg: &str, stacktrace: &Stracktrace) {
    info!(" |>\t{}:", msg);
    for frame in &stacktrace.frames {
        info!(" |>\t  {}!{}:{}", frame.filename, frame.method, frame.line_no);
    }
}

impl ProducedBlock {
    /// moving commitment level to finalized
    pub fn to_finalized_block(&self) -> Box<ProducedBlock> {
        Box::new(ProducedBlock {
            commitment_config: CommitmentConfig::finalized(),
            ..self.clone()
        })
    }

    /// moving commitment level to confirmed
    pub fn to_confirmed_block(&self) -> Box<ProducedBlock> {
        Box::new(ProducedBlock {
            commitment_config: CommitmentConfig::confirmed(),
            ..self.clone()
        })
    }
}


pub fn get_stacktrace() -> Result<Stracktrace, BacktrackError> {
    const OMIT_FRAME_SUFFIX1: &str = "rust_debugging_locks:";
    // <rust_debugging_locks::debugging_locks::RwLockWrapped<T> as core::default::Default>::default::haed7701ba5f48aa2:97
    const OMIT_FRAME_SUFFIX2: &str = "<rust_debugging_locks:";
    // produced_block.rs!solana_lite_rpc_core::structures::produced_block::get_stacktrace::h5a3e40b7d013ff2f:82
    backtrack_frame(|symbol_name| symbol_name.starts_with(OMIT_FRAME_SUFFIX1) || symbol_name.starts_with(OMIT_FRAME_SUFFIX2))
}
