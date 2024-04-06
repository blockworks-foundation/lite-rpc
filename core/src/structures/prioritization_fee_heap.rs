use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    sync::Arc,
};

use solana_sdk::signature::Signature;
use tokio::sync::Mutex;

use super::transaction_sent_info::SentTransactionInfo;

#[derive(Default)]
struct PrioFeeHeapData {
    map: BTreeMap<u64, VecDeque<SentTransactionInfo>>,
    signatures: HashSet<Signature>,
}

#[derive(Default, Clone)]
pub struct PrioritizationFeesHeap {
    map: Arc<Mutex<PrioFeeHeapData>>,
    max_number_of_transactions: usize,
}

impl PrioritizationFeesHeap {
    pub fn new(max_number_of_transactions: usize) -> Self {
        Self {
            map: Arc::new(Mutex::new(PrioFeeHeapData::default())),
            max_number_of_transactions,
        }
    }

    pub async fn pop(&self) -> Option<SentTransactionInfo> {
        let mut write_lock = self.map.lock().await;
        if let Some(mut entry) = write_lock.map.last_entry() {
            let element = entry.get_mut().pop_front().unwrap();
            if entry.get().is_empty() {
                entry.remove();
            }
            write_lock.signatures.remove(&element.signature);
            return Some(element);
        }
        None
    }

    pub async fn insert(&self, tx: SentTransactionInfo) {
        let mut write_lock = self.map.lock().await;

        if write_lock.signatures.contains(&tx.signature) {
            // signature already in the list
            return;
        }

        if write_lock.signatures.len() >= self.max_number_of_transactions {
            // check if prioritization is more than prioritization in the map
            if tx.prioritization_fee <= *write_lock.map.first_entry().unwrap().key() {
                return;
            }
        }

        write_lock.signatures.insert(tx.signature);
        match write_lock.map.get_mut(&tx.prioritization_fee) {
            Some(value) => {
                value.push_back(tx);
            }
            None => {
                let mut vec_d = VecDeque::new();
                let prioritization_fee = tx.prioritization_fee;
                vec_d.push_back(tx);
                write_lock.map.insert(prioritization_fee, vec_d);
            }
        }
        if write_lock.signatures.len() > self.max_number_of_transactions {
            match write_lock.map.first_entry() {
                Some(mut first_entry) => {
                    let tx_info = first_entry.get_mut().pop_front().unwrap();
                    if first_entry.get().is_empty() {
                        first_entry.remove();
                    }

                    write_lock.signatures.remove(&tx_info.signature);
                }
                None => {
                    panic!("Should not happen");
                }
            }
        }
    }

    pub async fn remove_expired_transactions(&self, current_blockheight: u64) -> usize {
        let mut write_lock = self.map.lock().await;
        let mut cells_to_remove = vec![];
        let mut signatures_to_remove = vec![];
        for (p, entry) in write_lock.map.iter_mut() {
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
            write_lock.map.remove(&p);
        }
        let signatures_len = signatures_to_remove.len();
        for sig in signatures_to_remove {
            write_lock.signatures.remove(&sig);
        }
        signatures_len
    }

    pub async fn size(&self) -> usize {
        self.map.lock().await.signatures.len()
    }

    pub async fn clear(&self) -> usize {
        let mut lk = self.map.lock().await;
        lk.map.clear();
        let size = lk.signatures.len();
        lk.signatures.clear();
        size
    }
}

#[cfg(test)]
mod tests {
    use solana_sdk::signature::Signature;
    use std::{sync::Arc, time::Duration};

    use crate::structures::{
        prioritization_fee_heap::PrioritizationFeesHeap, transaction_sent_info::SentTransactionInfo,
    };

    #[tokio::test]
    pub async fn test_prioritization_heap() {
        let p_heap = PrioritizationFeesHeap::new(4);
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

        let tx_0 = tx_creator(Signature::new_unique(), 0);
        let tx_1 = tx_creator(Signature::new_unique(), 10);
        let tx_2 = tx_creator(Signature::new_unique(), 100);
        let tx_3 = tx_creator(Signature::new_unique(), 0);
        let tx_4 = tx_creator(Signature::new_unique(), 0);
        let tx_5 = tx_creator(Signature::new_unique(), 1000);
        let tx_6 = tx_creator(Signature::new_unique(), 10);
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    pub async fn test_prioritization_bench() {
        let p_heap = PrioritizationFeesHeap::new(4096);

        let jh = {
            let p_heap = p_heap.clone();
            tokio::spawn(async move {
                let instant = tokio::time::Instant::now();

                let mut height = 0;
                while instant.elapsed() < Duration::from_secs(45) {
                    let burst_count = rand::random::<u64>() % 128 + 1;
                    for _c in 0..burst_count {
                        let prioritization_fee = rand::random::<u64>() % 100000;
                        let info = SentTransactionInfo {
                            signature: Signature::new_unique(),
                            slot: height + 1,
                            transaction: Arc::new(vec![]),
                            last_valid_block_height: height + 10,
                            prioritization_fee,
                        };
                        p_heap.insert(info).await;
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    p_heap.remove_expired_transactions(height).await;
                    height += 1;
                }
            })
        };

        let mut pop_count = 0;
        while !jh.is_finished() {
            if p_heap.pop().await.is_some() {
                pop_count += 1;
            }
        }
        println!("pop_count : {pop_count}");
    }
}
