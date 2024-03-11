use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[derive(Clone)]
pub struct RotatingQueue<T: Clone> {
    elements: Vec<T>,
    current: Arc<AtomicUsize>,
}

impl<T: Clone> RotatingQueue<T> {
    pub fn new<F>(size: usize, creator_functor: F) -> Self
    where
        F: Fn() -> T,
    {
        Self {
            elements: std::iter::repeat_with(creator_functor).take(size).collect(),
            current: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn get(&self) -> Option<T> {
        if !self.elements.is_empty() {
            let current = self.current.fetch_add(1, Ordering::Relaxed);
            let index = current % (self.elements.len());
            Some(self.elements[index].clone())
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
