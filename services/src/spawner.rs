use std::time::Duration;

use crate::cleaner::Cleaner;

pub struct SpawnerConfig {}

pub struct Spawner;

impl Spawner {
    pub async fn spawn() -> anyhow::Result<()> {
        // transactions get invalid in around 1 mins, because the block hash expires in 150 blocks so 150 * 400ms = 60s
        // Setting it to two to give some margin of error / as not all the blocks are filled.
        let cleaner = Cleaner::new(tx_sender.clone(), block_listner.clone(), block_store_t)
            .start(Duration::from_secs(120));

        Ok(())
    }
}
