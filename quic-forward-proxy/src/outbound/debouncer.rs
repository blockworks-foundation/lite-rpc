use log::trace;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub struct Debouncer {
    cooldown_ms: i64,
    last: AtomicI64,
}

impl Debouncer {
    pub fn new(cooldown: Duration) -> Self {
        Self {
            cooldown_ms: cooldown.as_millis() as i64,
            last: AtomicI64::new(-1),
        }
    }
    pub fn can_fire(&self) -> bool {
        let now = SystemTime::now();
        let epoch_now = now.duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;

        let results = self
            .last
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |last| {
                let elapsed = epoch_now - last;
                if elapsed > self.cooldown_ms {
                    trace!("trigger it!");
                    Some(epoch_now)
                } else {
                    trace!("have to wait - not yet .. (elapsed {})", elapsed);
                    None
                }
            }); // -- compare+swap

        results.is_ok()
    }
}

#[cfg(test)]
mod tests {
    use crate::outbound::debouncer::Debouncer;
    use std::sync::Arc;
    use std::thread;
    use std::thread::sleep;
    use std::time::Duration;

    #[test]
    fn fire() {
        let debouncer = Debouncer::new(Duration::from_millis(500));

        assert!(debouncer.can_fire());
        assert!(!debouncer.can_fire());
        sleep(Duration::from_millis(200));
        assert!(!debouncer.can_fire());
        sleep(Duration::from_millis(400));
        assert!(debouncer.can_fire());
    }

    #[test]
    fn threading() {
        let debouncer = Debouncer::new(Duration::from_millis(500));

        thread::spawn(move || {
            debouncer.can_fire();
        });
    }

    #[test]
    fn shared() {
        let debouncer = Arc::new(Debouncer::new(Duration::from_millis(500)));

        let debouncer_copy = debouncer.clone();
        thread::spawn(move || {
            debouncer_copy.can_fire();
        });

        thread::spawn(move || {
            debouncer.can_fire();
        });
    }
}
