use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use dashmap::DashSet;
use tokio::sync::Mutex;

use super::transaction_sent_info::SentTransactionInfo;

#[derive(Default, Clone)]
pub struct PrioritizationFeesHeap {
    signatures: DashSet<String>,
    map: Arc<Mutex<BTreeMap<u64, VecDeque<SentTransactionInfo>>>>,
    min_prioritization_fees: Arc<AtomicU64>,
    max_number_of_transactions: usize,
}

impl PrioritizationFeesHeap {
    pub fn new(max_number_of_transactions: usize) -> Self {
        Self {
            signatures: DashSet::new(),
            map: Arc::new(Mutex::new(BTreeMap::new())),
            min_prioritization_fees: Arc::new(AtomicU64::new(0)),
            max_number_of_transactions,
        }
    }

    pub async fn pop(&self) -> Option<SentTransactionInfo> {
        let mut write_lock = self.map.lock().await;
        if let Some(mut entry) = write_lock.last_entry() {
            if let Some(element) = entry.get_mut().pop_front() {
                if entry.get().is_empty() {
                    entry.remove();
                }
                self.signatures.remove(&element.signature);
                return Some(element);
            }
        }
        None
    }

    pub async fn insert(&self, tx: SentTransactionInfo) {
        if self.signatures.len() >= self.max_number_of_transactions {
            // check if prioritization is more than prioritization in the map
            if tx.prioritization_fee <= self.min_prioritization_fees.load(Ordering::Relaxed) {
                return;
            }
        }
        if self.signatures.contains(&tx.signature) {
            // signature already in the list
            return;
        }

        self.signatures.insert(tx.signature.clone());
        let mut write_lock = self.map.lock().await;
        match write_lock.get_mut(&tx.prioritization_fee) {
            Some(value) => {
                value.push_back(tx);
            }
            None => {
                let mut vec_d = VecDeque::new();
                let prioritization_fee = tx.prioritization_fee;
                vec_d.push_back(tx);
                write_lock.insert(prioritization_fee, vec_d);
            }
        }
        if self.signatures.len() > self.max_number_of_transactions {
            match write_lock.first_entry() {
                Some(mut first_entry) => {
                    let tx_info = first_entry.get_mut().pop_front();
                    if let Some(tx_info) = tx_info {
                        self.signatures.remove(&tx_info.signature);
                    }
                    if first_entry.get().is_empty() {
                        first_entry.remove_entry();
                    }
                }
                None => {
                    panic!("Should not happen");
                }
            }
        }
    }

    pub async fn remove_expired_transactions(&self, current_blockheight: u64) {
        let mut write_lock = self.map.lock().await;
        for (_, entry) in write_lock.iter_mut() {
            entry.retain(|x| {
                let retain = x.last_valid_block_height > current_blockheight;
                if !retain {
                    self.signatures.remove(&x.signature);
                }
                retain
            });
        }
    }
}

#[tokio::test]
pub async fn test_prioritization_heap() {
    let p_heap = PrioritizationFeesHeap::new(4);
    let tx_creator = |signature, prioritization_fee| SentTransactionInfo {
        signature,
        slot: 0,
        transaction: vec![],
        last_valid_block_height: 0,
        prioritization_fee,
    };

    let tx_0 = tx_creator("0".to_string(), 0);
    let tx_1 = tx_creator("1".to_string(), 10);
    let tx_2 = tx_creator("2".to_string(), 100);
    let tx_3 = tx_creator("3".to_string(), 0);
    p_heap.insert(tx_0.clone()).await;
    p_heap.insert(tx_0.clone()).await;
    p_heap.insert(tx_0.clone()).await;
    p_heap.insert(tx_0.clone()).await;
    p_heap.insert(tx_0.clone()).await;
    p_heap.insert(tx_1.clone()).await;
    p_heap.insert(tx_2.clone()).await;
    p_heap.insert(tx_2.clone()).await;
    p_heap.insert(tx_2.clone()).await;
    p_heap.insert(tx_2.clone()).await;
    p_heap.insert(tx_3.clone()).await;

    assert_eq!(p_heap.pop().await, Some(tx_2));
    assert_eq!(p_heap.pop().await, Some(tx_1));
    assert_eq!(p_heap.pop().await, Some(tx_0));
    assert_eq!(p_heap.pop().await, Some(tx_3));
    assert_eq!(p_heap.pop().await, None);

    let tx_0 = tx_creator("0".to_string(), 0);
    let tx_1 = tx_creator("1".to_string(), 10);
    let tx_2 = tx_creator("2".to_string(), 100);
    let tx_3 = tx_creator("3".to_string(), 0);
    let tx_4 = tx_creator("4".to_string(), 0);
    let tx_5 = tx_creator("5".to_string(), 1000);
    let tx_6 = tx_creator("6".to_string(), 10);
    p_heap.insert(tx_0.clone()).await;
    p_heap.insert(tx_1.clone()).await;
    p_heap.insert(tx_2.clone()).await;
    p_heap.insert(tx_3.clone()).await;
    p_heap.insert(tx_4.clone()).await;
    p_heap.insert(tx_5.clone()).await;
    p_heap.insert(tx_6.clone()).await;

    assert_eq!(p_heap.pop().await, Some(tx_5));
    assert_eq!(p_heap.pop().await, Some(tx_2));
    assert_eq!(p_heap.pop().await, Some(tx_1));
    assert_eq!(p_heap.pop().await, Some(tx_6));
    assert_eq!(p_heap.pop().await, None);
}
