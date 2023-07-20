use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct RotatingQueue<T> {
    deque: Arc<Mutex<VecDeque<T>>>,
    count: Arc<AtomicU64>,
}

impl<T: Clone> RotatingQueue<T> {
    pub async fn new<F>(size: usize, creator_functor: F) -> Self
    where
        F: Fn() -> T,
    {
        let item = Self {
            deque: Arc::new(Mutex::new(VecDeque::<T>::new())),
            count: Arc::new(AtomicU64::new(0)),
        };
        {
            let mut deque = item.deque.lock().await;
            for _i in 0..size {
                deque.push_back(creator_functor());
            }
            item.count.store(size as u64, Ordering::Relaxed);
        }
        item
    }

    pub fn new_empty() -> Self {
        Self {
            deque: Arc::new(Mutex::new(VecDeque::<T>::new())),
            count: Arc::new(AtomicU64::new(0)),
        }
    }

    pub async fn get(&self) -> Option<T> {
        let mut deque = self.deque.lock().await;
        if !deque.is_empty() {
            let current = deque.pop_front().unwrap();
            deque.push_back(current.clone());
            Some(current)
        } else {
            None
        }
    }

    pub async fn add(&self, instance: T) {
        let mut queue = self.deque.lock().await;
        queue.push_front(instance);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    pub async fn remove(&self) {
        if !self.is_empty() {
            let mut queue = self.deque.lock().await;
            queue.pop_front();
            self.count.fetch_sub(1, Ordering::Relaxed);
        }
    }

    pub fn len(&self) -> usize {
        self.count.load(Ordering::Relaxed) as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
