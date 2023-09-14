use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

#[derive(Clone)]
pub struct RotatingQueue<T: Clone> {
    elements: Vec<T>,
    current: Arc<AtomicU64>,
}

impl<T: Clone> RotatingQueue<T> {
    pub fn new<F>(size: usize, creator_functor: F) -> Self
    where
        F: Fn() -> T,
    {
        let mut item = Self {
            elements: Vec::<T>::new(),
            current: Arc::new(AtomicU64::new(0)),
        };
        {
            for _i in 0..size {
                item.elements.push(creator_functor());
            }
        }
        item
    }

    pub fn get(&self) -> Option<T> {
        if !self.elements.is_empty() {
            let current = self.current.fetch_add(1, Ordering::Relaxed);
            let index = current % (self.elements.len() as u64);
            Some(self.elements[index as usize].clone())
        } else {
            None
        }
    }

    pub fn len(&self) -> usize {
        self.elements.len()
    }

    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }
}
