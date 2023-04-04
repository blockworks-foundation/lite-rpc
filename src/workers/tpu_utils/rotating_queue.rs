use std::{
    collections::VecDeque,
    sync::{Arc, RwLock},
};

#[derive(Clone)]
pub struct RotatingQueue<T> {
    deque: Arc<RwLock<VecDeque<T>>>,
}

impl<T: Clone> RotatingQueue<T> {
    pub fn new<F>(size: usize, creator_functor: F) -> Self
    where
        F: Fn() -> T,
    {
        let item = Self {
            deque: Arc::new(RwLock::new(VecDeque::<T>::new())),
        };
        {
            let mut deque = item.deque.write().unwrap();
            for _i in 0..size {
                deque.push_back(creator_functor());
            }
        }
        item
    }

    pub fn get(&self) -> T {
        let mut deque = self.deque.write().unwrap();
        let current = deque.pop_front().unwrap();
        deque.push_back(current.clone());
        current
    }
}
