use std::time::Duration;
use tokio_cron_scheduler::{Job, JobScheduler, JobSchedulerError};

#[tokio::main]
async fn main() -> Result<(), JobSchedulerError> {
    tracing_subscriber::fmt::init();

    let mut sched = JobScheduler::new().await?;

    // Add basic cron job
    sched.add(
        Job::new("1/2 * * * * *", |_uuid, _l| {
            println!("I run every 2 seconds");
        })?
    ).await?;


    sched.start().await?;

    // Wait while the jobs run
    tokio::time::sleep(Duration::from_secs(100)).await;


    Ok(())
}
