use anyhow::bail;
use solana_account_decoder::parse_sysvar::SysvarAccountType;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::epoch_info::EpochInfo;
use solana_sdk::slot_history::Slot;
use solana_sdk::sysvar::epoch_schedule::EpochSchedule;
use std::sync::Arc;

#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Eq, Ord)]
pub struct Epoch {
    pub epoch: u64,
    pub slot_index: u64,
    pub slots_in_epoch: u64,
    pub absolute_slot: Slot,
}

impl Epoch {
    pub fn into_epoch_info(&self, block_height: u64, transaction_count: Option<u64>) -> EpochInfo {
        EpochInfo {
            epoch: self.epoch,
            slot_index: self.slot_index,
            slots_in_epoch: self.slots_in_epoch,
            absolute_slot: self.absolute_slot,
            block_height,
            transaction_count,
        }
    }
}

#[derive(Clone)]
pub struct EpochCache {
    epoch_schedule: Arc<EpochSchedule>,
}

impl EpochCache {
    pub fn get_epoch_at_slot(&self, slot: Slot) -> Epoch {
        let (epoch, slot_index) = self.epoch_schedule.get_epoch_and_slot_index(slot);
        let slots_in_epoch = self.epoch_schedule.get_slots_in_epoch(epoch);
        Epoch {
            epoch,
            slot_index,
            slots_in_epoch,
            absolute_slot: slot,
        }
    }

    pub fn get_slots_in_epoch(&self, epoch: u64) -> u64 {
        self.epoch_schedule.get_slots_in_epoch(epoch)
    }

    pub fn get_first_slot_in_epoch(&self, epoch: u64) -> u64 {
        self.epoch_schedule.get_first_slot_in_epoch(epoch)
    }

    pub fn get_last_slot_in_epoch(&self, epoch: u64) -> u64 {
        self.epoch_schedule.get_last_slot_in_epoch(epoch)
    }

    pub async fn bootstrap_epoch(rpc_client: &RpcClient) -> anyhow::Result<EpochCache> {
        let res_epoch = rpc_client
            .get_account(&solana_sdk::sysvar::epoch_schedule::id())
            .await?;
        let Some(SysvarAccountType::EpochSchedule(epoch_schedule)) = bincode::deserialize(&res_epoch.data[..])
        .ok()
        .map(SysvarAccountType::EpochSchedule) else {
            bail!("Error during bootstrap epoch. SysvarAccountType::EpochSchedule can't be deserilized. Epoch can't be calculated.");
        };

        Ok(EpochCache {
            epoch_schedule: Arc::new(epoch_schedule),
        })
    }
}

impl EpochCache {
    ///Use only for test.
    pub fn new_for_tests() -> Self {
        Self {
            epoch_schedule: Arc::new(EpochSchedule {
                slots_per_epoch: 1000,
                leader_schedule_slot_offset: 0,
                warmup: false,
                first_normal_epoch: 0,
                first_normal_slot: 0,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_epoch_calculus() {
        let epoch_cache = EpochCache::new_for_tests();

        let slot_epoch = epoch_cache.get_epoch_at_slot(1);
        assert_eq!(
            Epoch {
                epoch: 0,
                slot_index: 1,
                slots_in_epoch: 1000,
                absolute_slot: 1,
            },
            slot_epoch
        );

        let slot_epoch = epoch_cache.get_epoch_at_slot(999);
        assert_eq!(
            Epoch {
                epoch: 0,
                slot_index: 999,
                slots_in_epoch: 1000,
                absolute_slot: 999,
            },
            slot_epoch
        );

        let slot_epoch = epoch_cache.get_epoch_at_slot(1001);
        assert_eq!(
            Epoch {
                epoch: 1,
                slot_index: 1,
                slots_in_epoch: 1000,
                absolute_slot: 1001,
            },
            slot_epoch
        );
        let slot_epoch = epoch_cache.get_epoch_at_slot(14031);
        assert_eq!(
            Epoch {
                epoch: 14,
                slot_index: 31,
                slots_in_epoch: 1000,
                absolute_slot: 14031,
            },
            slot_epoch
        );
    }
}
