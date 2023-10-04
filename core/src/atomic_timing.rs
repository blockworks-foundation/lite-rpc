
// ported from solana_sdk timing.rs

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use solana_sdk::unchecked_div_by_const;

pub fn duration_as_ms(d: &Duration) -> u64 {
    d.as_secs()
        .saturating_mul(1000)
        .saturating_add(unchecked_div_by_const!(
            u64::from(d.subsec_nanos()),
            1_000_000
        ))
}

// milliseconds since unix epoch start
fn epoch_now_ms() -> u64 {
    let since_epoch_start = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("create timestamp in timing");
    duration_as_ms(&since_epoch_start)
}

#[derive(Debug)]
pub struct AtomicTiming {
    // note: 0 is interpreted as "not updated yet"
    last_update: AtomicU64,
}

impl Default for AtomicTiming {
    /// initialize with "0" (start of unix epoch)
    // 0 is magic value
    fn default() -> Self {
        Self {
            last_update: AtomicU64::new(0),
        }
    }
}

impl AtomicTiming {
    // initialize with "fired now"
    pub fn new() -> Self {
        Self {
            last_update: AtomicU64::new(epoch_now_ms()),
        }
    }
}

impl AtomicTiming {
    /// true if 'interval_time_ms' has elapsed since last time we returned true as long as it has been 'interval_time_ms' since this struct was created
    pub fn should_update(&self, interval_time_ms: u64) -> bool {
        self.should_update_ext(interval_time_ms, true)
    }

    pub fn update(&self) {
        let now = epoch_now_ms();
        self.last_update.store(now, Ordering::Relaxed);
    }

    /// a primary use case is periodic metric reporting, potentially from different threads
    /// true if 'interval_time_ms' has elapsed since last time we returned true
    /// except, if skip_first=false, false until 'interval_time_ms' has elapsed since this struct was created
    pub fn should_update_ext(&self, interval_time_ms: u64, skip_first: bool) -> bool {
        let now = epoch_now_ms();
        let last = self.last_update.load(Ordering::Relaxed);

        if now.saturating_sub(last) <= interval_time_ms {
            return false;
        }

        if skip_first && last == 0 {
            return false;
        }

        if self
            .last_update
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            != Ok(last)
        {
            // concurrent update
            return false;
        }

        return true;
    }

    /// return ms elapsed since the last time the time was set
    pub fn elapsed_ms(&self) -> u64 {
        let now = epoch_now_ms();
        let last = self.last_update.load(Ordering::Relaxed);
        now.saturating_sub(last)
    }

    /// return ms elapsed since the last time the time was set
    pub fn elapsed(&self) -> Duration {
        let elapsed_ms = self.elapsed_ms();
        Duration::from_millis(elapsed_ms)
    }

    /// return ms until the interval_time will have elapsed
    pub fn remaining_until_next_interval(&self, interval_time_ms: u64) -> u64 {
        interval_time_ms.saturating_sub(self.elapsed_ms())
    }
}


#[test]
fn default() {
    // note: race condition - this calls now() twice
    assert_eq!(AtomicTiming::default().elapsed_ms(), epoch_now_ms());
}