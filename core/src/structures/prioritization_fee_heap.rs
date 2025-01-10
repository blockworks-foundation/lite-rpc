use std::collections::{BTreeMap, HashSet, VecDeque};

use solana_sdk::signature::Signature;

use super::transaction_sent_info::SentTransactionInfo;

#[derive(Default)]
struct PrioFeeHeapData {
    map: BTreeMap<u64, VecDeque<SentTransactionInfo>>,
    signatures: HashSet<Signature>,
}

#[derive(Default)]
pub struct PrioritizationFeesHeap {
    fee_data: PrioFeeHeapData,
    max_number_of_transactions: usize,
}

impl PrioritizationFeesHeap {
    pub fn new(max_number_of_transactions: usize) -> Self {
        Self {
            fee_data: PrioFeeHeapData::default(),
            max_number_of_transactions,
        }
    }

    pub fn pop(&mut self) -> Option<SentTransactionInfo> {
        if let Some(mut entry) = self.fee_data.map.last_entry() {
            let element = entry.get_mut().pop_front().unwrap();
            if entry.get().is_empty() {
                entry.remove();
            }
            self.fee_data.signatures.remove(&element.signature);
            return Some(element);
        }
        None
    }

    pub fn insert(&mut self, tx: SentTransactionInfo) {
        if self.fee_data.signatures.contains(&tx.signature) {
            // signature already in the list
            return;
        }

        if self.fee_data.signatures.len() >= self.max_number_of_transactions {
            // check if prioritization is more than prioritization in the map
            if tx.prioritization_fee <= *self.fee_data.map.first_entry().unwrap().key() {
                return;
            }
        }

        self.fee_data.signatures.insert(tx.signature);
        match self.fee_data.map.get_mut(&tx.prioritization_fee) {
            Some(value) => {
                value.push_back(tx);
            }
            None => {
                let mut vec_d = VecDeque::new();
                let prioritization_fee = tx.prioritization_fee;
                vec_d.push_back(tx);
                self.fee_data.map.insert(prioritization_fee, vec_d);
            }
        }
        if self.fee_data.signatures.len() > self.max_number_of_transactions {
            match self.fee_data.map.first_entry() {
                Some(mut first_entry) => {
                    let tx_info = first_entry.get_mut().pop_front().unwrap();
                    if first_entry.get().is_empty() {
                        first_entry.remove();
                    }

                    self.fee_data.signatures.remove(&tx_info.signature);
                }
                None => {
                    panic!("Should not happen");
                }
            }
        }
    }

    pub fn remove_expired_transactions(&mut self, current_blockheight: u64) -> usize {
        let mut cells_to_remove = vec![];
        let mut signatures_to_remove = vec![];
        for (p, entry) in self.fee_data.map.iter_mut() {
            entry.retain(|x| {
                let retain = x.last_valid_block_height > current_blockheight;
                if !retain {
                    signatures_to_remove.push(x.signature);
                }
                retain
            });
            if entry.is_empty() {
                cells_to_remove.push(*p);
            }
        }
        for p in cells_to_remove {
            self.fee_data.map.remove(&p);
        }
        let signatures_len = signatures_to_remove.len();
        for sig in signatures_to_remove {
            self.fee_data.signatures.remove(&sig);
        }
        signatures_len
    }

    pub async fn size(&self) -> usize {
        self.fee_data.signatures.len()
    }

    pub async fn clear(&mut self) -> usize {
        self.fee_data.map.clear();
        let size = self.fee_data.signatures.len();
        self.fee_data.signatures.clear();
        size
    }
}

#[cfg(test)]
mod tests {
    use solana_sdk::signature::Signature;
    use std::sync::Arc;

    use crate::structures::{
        prioritization_fee_heap::PrioritizationFeesHeap, transaction_sent_info::SentTransactionInfo,
    };

    #[tokio::test]
    pub async fn test_prioritization_heap() {
        let mut p_heap = PrioritizationFeesHeap::new(4);
        let tx_creator = |signature, prioritization_fee| SentTransactionInfo {
            signature,
            slot: 0,
            transaction: Arc::new(vec![]),
            last_valid_block_height: 0,
            prioritization_fee,
        };

        let tx_0 = tx_creator(Signature::new_unique(), 0);
        let tx_1 = tx_creator(Signature::new_unique(), 10);
        let tx_2 = tx_creator(Signature::new_unique(), 100);
        let tx_3 = tx_creator(Signature::new_unique(), 0);
        p_heap.insert(tx_0.clone());
        p_heap.insert(tx_0.clone());
        p_heap.insert(tx_0.clone());
        p_heap.insert(tx_0.clone());
        p_heap.insert(tx_0.clone());
        p_heap.insert(tx_1.clone());
        p_heap.insert(tx_2.clone());
        p_heap.insert(tx_2.clone());
        p_heap.insert(tx_2.clone());
        p_heap.insert(tx_2.clone());
        p_heap.insert(tx_3.clone());

        assert_eq!(p_heap.pop(), Some(tx_2));
        assert_eq!(p_heap.pop(), Some(tx_1));
        assert_eq!(p_heap.pop(), Some(tx_0));
        assert_eq!(p_heap.pop(), Some(tx_3));
        assert_eq!(p_heap.pop(), None);

        let tx_0 = tx_creator(Signature::new_unique(), 0);
        let tx_1 = tx_creator(Signature::new_unique(), 10);
        let tx_2 = tx_creator(Signature::new_unique(), 100);
        let tx_3 = tx_creator(Signature::new_unique(), 0);
        let tx_4 = tx_creator(Signature::new_unique(), 0);
        let tx_5 = tx_creator(Signature::new_unique(), 1000);
        let tx_6 = tx_creator(Signature::new_unique(), 10);
        p_heap.insert(tx_0.clone());
        p_heap.insert(tx_1.clone());
        p_heap.insert(tx_2.clone());
        p_heap.insert(tx_3.clone());
        p_heap.insert(tx_4.clone());
        p_heap.insert(tx_5.clone());
        p_heap.insert(tx_6.clone());

        assert_eq!(p_heap.pop(), Some(tx_5));
        assert_eq!(p_heap.pop(), Some(tx_2));
        assert_eq!(p_heap.pop(), Some(tx_1));
        assert_eq!(p_heap.pop(), Some(tx_6));
        assert_eq!(p_heap.pop(), None);
    }
}
