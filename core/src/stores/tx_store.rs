use std::sync::Arc;

use dashmap::DashMap;
use solana_transaction_status::TransactionStatus;
/// Transaction Properties

#[derive(Debug, Clone)]
pub struct TxProps {
    pub status: Option<TransactionStatus>,
    pub last_valid_blockheight: u64,
}

impl TxProps {
    pub fn new(last_valid_blockheight: u64) -> Self {
        Self {
            status: Default::default(),
            last_valid_blockheight,
        }
    }
}

#[derive(Default, Clone, Debug)]
pub struct TxStore {
    pub store: Arc<DashMap<String, TxProps>>,
}

impl TxStore {
    pub fn update_status(&self, signature: &str, status: TransactionStatus) -> bool {
        if let Some(mut meta) = self.store.get_mut(signature) {
            meta.status = Some(status);
            true
        } else {
            false
        }
    }

    pub fn insert(&self, signature: String, props: TxProps) -> Option<TxProps> {
        self.store.insert(signature, props)
    }

    pub fn len(&self) -> usize {
        self.store.len()
    }

    pub fn is_empty(&self) -> bool {
        self.store.is_empty()
    }

    pub fn contains_key(&self, signature: &String) -> bool {
        self.store.contains_key(signature)
    }

    pub fn get(&self, signature: &String) -> Option<TxProps> {
        self.store.get(signature).map(|x| x.value().clone())
    }

    pub fn clean(&self, current_finalized_blochash: u64) {
        let length_before = self.store.len();
        self.store.retain(|_k, v| {
            let retain = v.last_valid_blockheight >= current_finalized_blochash;
            if !retain && v.status.is_none() {
                // TODO: TX_TIMED_OUT.inc();
            }
            retain
        });
        log::info!("Cleaned {} transactions", length_before - self.store.len());
    }

    pub fn is_transaction_confirmed(&self, signature: &String) -> bool {
        match self.store.get(signature) {
            Some(props) => props.status.is_some(),
            None => false,
        }
    }
}

pub fn empty_tx_store() -> TxStore {
    TxStore {
        store: Arc::new(DashMap::new()),
    }
}
