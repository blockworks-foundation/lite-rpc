/// Maintain a virtual len, returning slices according to batch_size
/// Removes batched elements on drop
///
/// Prevents re-sizing of vector after every batch
pub struct Batcher<'a, T> {
    batch_from: &'a mut Vec<T>,
    batch_size: usize,
    pointer: usize,
    strategy: BatcherStrategy
}

pub enum BatcherStrategy {
    Start, 
    End 
}


impl<'a, T> Batcher<'a, T> {
    pub fn new(batch_from: &'a mut Vec<T>, batch_size: usize, strategy: BatcherStrategy) -> Self {
        Self {
            pointer: match strategy {
                BatcherStrategy::Start => 0,
                BatcherStrategy::End => batch_from.len()
            },
            batch_from,
            batch_size,
            strategy
        }
    }

    pub fn next_batch(&mut self) -> Option<&[T]> {
        let range = match self.strategy {
            BatcherStrategy::Start => {
                let new_start = self.pointer + self.batch_size;
                
                if new_start >= self.batch_from.len() {
                    return None;
                }

                let range = self.pointer .. new_start;
                self.pointer = new_start;
                range
            },
            BatcherStrategy::End => {
                let Some(new_len) = self.pointer.checked_sub(self.batch_size) else {
                    return None;
                };
        
                let range = new_len.. self.pointer;
                self.pointer = new_len;
                range

            },
        };

        Some(&self.batch_from[range])
    }
}


impl<'a, T> Drop for Batcher<'a, T> {
    fn drop(&mut self) {
        let range = match self.strategy {
            BatcherStrategy::Start => 0..self.pointer,
            BatcherStrategy::End => self.pointer..self.batch_from.len(),
        };

        self.batch_from.drain(range);
    }
}

#[cfg(test)]
mod tests {
    use super::{BatcherStrategy, Batcher};

    #[test]
    fn start() {
        let mut elements = vec![1, 2, 3, 4, 5, 6, 7];
        let mut batcher = Batcher::new(&mut elements, 2, BatcherStrategy::Start);
        assert_eq!(Some(&[1, 2][..]), batcher.next_batch());
        assert_eq!(Some(&[3, 4][..]), batcher.next_batch());
        assert_eq!(Some(&[5, 6][..]), batcher.next_batch());
        assert_eq!(None, batcher.next_batch());
        drop(batcher);
        assert_eq!(&[7][..], &elements);
    }

    #[test]
    fn end() {
        let mut elements = vec![1, 2, 3, 4, 5, 6, 7];
        let mut batcher = Batcher::new(&mut elements, 2, BatcherStrategy::End);
        assert_eq!(Some(&[6, 7][..]), batcher.next_batch());
        assert_eq!(Some(&[4, 5][..]), batcher.next_batch());
        assert_eq!(Some(&[2, 3][..]), batcher.next_batch());
        assert_eq!(None, batcher.next_batch());
        drop(batcher);
        assert_eq!(&[1][..], &elements);
    }

}
