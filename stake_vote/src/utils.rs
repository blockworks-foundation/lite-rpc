use anyhow::bail;
use solana_lite_rpc_core::stores::block_information_store::BlockInformation;
use solana_lite_rpc_core::stores::data_cache::DataCache;
use solana_lite_rpc_core::structures::epoch::Epoch as LiteRpcEpoch;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use std::default::Default;

pub async fn get_current_confirmed_slot(data_cache: &DataCache) -> u64 {
    let commitment = CommitmentConfig::confirmed();
    let BlockInformation { slot, .. } = data_cache
        .block_information_store
        .get_latest_block(commitment)
        .await;
    slot
}

pub async fn get_current_epoch(data_cache: &DataCache) -> LiteRpcEpoch {
    let commitment = CommitmentConfig::confirmed();
    data_cache.get_current_epoch(commitment).await
}

//Takable struct code
pub trait TakableContent<T>: Default {
    fn add_value(&mut self, val: T);
}

///A struct that hold a collection call content that can be taken during some time and merged after.
///During the time the content is taken, new added values are cached and added to the content after the merge.
///It allow to process struct content while allowing to still update it without lock.
#[derive(Default, Debug)]
pub struct TakableMap<T, C: TakableContent<T>> {
    pub content: C,
    pub updates: Vec<T>,
    taken: bool,
}

impl<T: Default, C: TakableContent<T> + Default> TakableMap<T, C> {
    pub fn new(content: C) -> Self {
        TakableMap {
            content,
            updates: vec![],
            taken: false,
        }
    }

    //add a value to the content if not taken or put it in the update waiting list.
    //Use force_in_update to force the insert in update waiting list.
    pub fn add_value(&mut self, val: T, force_in_update: bool) {
        //during extract push the new update or
        //don't insert now account change that has been done in next epoch.
        //put in update pool to be merged next epoch change.
        match self.taken || force_in_update {
            true => self.updates.push(val),
            false => self.content.add_value(val),
        }
    }

    pub fn take(self) -> (Self, C) {
        let takenmap = TakableMap {
            content: C::default(),
            updates: self.updates,
            taken: true,
        };
        (takenmap, self.content)
    }

    pub fn merge(self, content: C) -> Self {
        let mut mergedstore = TakableMap {
            content,
            updates: vec![],
            taken: false,
        };

        //apply stake added during extraction.
        for val in self.updates {
            mergedstore.content.add_value(val);
        }
        mergedstore
    }

    pub fn is_taken(&self) -> bool {
        self.taken
    }
}

pub fn take<T: Default, C: TakableContent<T> + Default>(
    map: &mut TakableMap<T, C>,
) -> anyhow::Result<C> {
    if map.is_taken() {
        bail!("TakableMap already taken. Try later");
    }
    let new_store = std::mem::take(map);
    let (new_store, content) = new_store.take();
    *map = new_store;
    Ok(content)
}

pub fn merge<T: Default, C: TakableContent<T> + Default>(
    map: &mut TakableMap<T, C>,
    content: C,
) -> anyhow::Result<()> {
    if !map.is_taken() {
        bail!("TakableMap merge of non taken map. Try later");
    }
    let new_store = std::mem::take(map);
    let new_store = new_store.merge(content);
    *map = new_store;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_takable_struct() {
        impl TakableContent<u64> for Vec<u64> {
            fn add_value(&mut self, val: u64) {
                self.push(val)
            }
        }

        let content: Vec<u64> = vec![];
        let mut takable = TakableMap::new(content);
        takable.add_value(23, false);
        assert_eq!(takable.content.len(), 1);

        takable.add_value(24, true);
        assert_eq!(takable.content.len(), 1);
        assert_eq!(takable.updates.len(), 1);

        let content = take(&mut takable).unwrap();
        assert_eq!(content.len(), 1);
        assert_eq!(takable.content.len(), 0);
        assert_eq!(takable.updates.len(), 1);
        let err_content = take(&mut takable);
        assert!(err_content.is_err());
        assert_eq!(content.len(), 1);
        assert_eq!(takable.content.len(), 0);
        assert_eq!(takable.updates.len(), 1);
        takable.add_value(25, false);
        assert_eq!(takable.content.len(), 0);
        assert_eq!(takable.updates.len(), 2);
        merge(&mut takable, content).unwrap();
        assert_eq!(takable.content.len(), 3);
        assert_eq!(takable.updates.len(), 0);

        let err = merge(&mut takable, vec![]);
        assert!(err.is_err());
    }
}
