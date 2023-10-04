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

#[derive(Clone, Debug)]
pub struct TxStore {
    pub store: Arc<DashMap<String, TxProps>>,
    pub save_for_additional_slots: u64,
}

impl TxStore {
    pub fn update_status(
        &self,
        signature: &str,
        status: TransactionStatus,
        last_valid_blockheight: u64,
    ) -> bool {
        if let Some(mut meta) = self.store.get_mut(signature) {
            meta.status = Some(status);
        } else {
            self.store.insert(
                signature.to_string(),
                TxProps {
                    status: Some(status),
                    last_valid_blockheight,
                },
            );
        }
        true
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
            v.last_valid_blockheight >= current_finalized_blochash + self.save_for_additional_slots
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
