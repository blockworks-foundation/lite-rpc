use std::future::Future;
use std::time::Duration;

use tokio::time::Timeout;

pub type AnyhowJoinHandle = tokio::task::JoinHandle<anyhow::Result<()>>;
pub const FALLBACK_TIMEOUT: Duration = Duration::from_secs(5);

pub fn timeout_fallback<F>(future: F) -> Timeout<F>
where
    F: Future,
{
    tokio::time::timeout(FALLBACK_TIMEOUT, future)
}
