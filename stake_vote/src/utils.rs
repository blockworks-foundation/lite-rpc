use crate::vote::StoredVote;
use crate::Slot;
use anyhow::bail;
use serde::{Deserialize, Serialize};
use solana_lite_rpc_core::stores::block_information_store::BlockInformation;
use solana_lite_rpc_core::stores::data_cache::DataCache;
use solana_lite_rpc_core::structures::epoch::Epoch as LiteRpcEpoch;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use std::default::Default;
use std::fs::File;
use std::io::Write;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Notify;

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

//Read save epoch vote stake to bootstrap current leader shedule and get_vote_account.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct StringSavedStake {
    epoch: u64,
    stake_vote_map: HashMap<String, (u64, Arc<StoredVote>)>,
}

#[allow(clippy::type_complexity)]
pub fn read_schedule_vote_stakes(
    file_path: &str,
) -> anyhow::Result<(u64, HashMap<Pubkey, (u64, Arc<StoredVote>)>)> {
    let content = std::fs::read_to_string(file_path)?;
    let stakes_str: StringSavedStake = serde_json::from_str(&content)?;
    //convert to EpochStake because json hashmap parser can only have String key.
    let ret_stakes = stakes_str
        .stake_vote_map
        .into_iter()
        .map(|(pk, st)| (Pubkey::from_str(&pk).unwrap(), (st.0, st.1)))
        .collect();
    Ok((stakes_str.epoch, ret_stakes))
}

pub fn save_schedule_vote_stakes(
    base_file_path: &str,
    stake_vote_map: &HashMap<Pubkey, (u64, Arc<StoredVote>)>,
    epoch: u64,
) -> anyhow::Result<()> {
    //save new schedule for restart.
    //need to convert hahsmap key to String because json aloow only string
    //key for dictionnary.
    //it's better to use json because the file can use to very some stake by hand.
    //in the end it will be removed with the bootstrap process.
    let save_stakes = StringSavedStake {
        epoch,
        stake_vote_map: stake_vote_map
            .iter()
            .map(|(pk, st)| (pk.to_string(), (st.0, Arc::clone(&st.1))))
            .collect(),
    };
    let serialized_stakes = serde_json::to_string(&save_stakes).unwrap();
    let mut file = File::create(base_file_path).unwrap();
    file.write_all(serialized_stakes.as_bytes()).unwrap();
    file.flush().unwrap();
    Ok(())
}

#[derive(Debug)]
pub enum UpdateAction<Account> {
    Notify(Slot, Account),
    Remove(Pubkey, Slot),
}

// impl<Account> UpdateAction<Account> {
//     pub fn get_update_slot(&self) -> u64 {
//         match self {
//             UpdateAction::Notify(slot, _) | UpdateAction::Remove(_, slot) => *slot,
//         }
//     }
// }

pub enum TakeResult<C> {
    //Vec because can wait on several collection to be merged
    Taken(Vec<Arc<Notify>>),
    Map(C),
}

impl<C1> TakeResult<C1> {
    pub fn and_then<C2>(self, action: TakeResult<C2>) -> TakeResult<(C1, C2)> {
        match (self, action) {
            (TakeResult::Taken(mut notif1), TakeResult::Taken(mut notif2)) => {
                notif1.append(&mut notif2);
                TakeResult::Taken(notif1)
            }
            (TakeResult::Map(content1), TakeResult::Map(content2)) => {
                TakeResult::Map((content1, content2))
            }
            _ => unreachable!("Bad take result association."), //TODO add mix result.
        }
    }

    // pub fn get_content(self) -> Option<C1> {
    //     match self {
    //         TakeResult::Taken(_) => None,
    //         TakeResult::Map(content) => Some(content),
    //     }
    // }
}

//Takable struct code
pub trait TakableContent<T>: Default {
    fn add_value(&mut self, val: UpdateAction<T>);
}

//Takable struct code
pub trait Takable<C> {
    fn take(self) -> TakeResult<C>;
    fn merge(self, content: C) -> anyhow::Result<()>;
    fn is_taken(&self) -> bool;
}

impl<'a, T, C: TakableContent<T>> Takable<C> for &'a mut TakableMap<T, C> {
    fn take(self) -> TakeResult<C> {
        match self.content.take() {
            Some(content) => TakeResult::Map(content),
            None => TakeResult::Taken(vec![Arc::clone(&self.notifier)]),
        }
    }

    fn merge(mut self, mut content: C) -> anyhow::Result<()> {
        if self.content.is_none() {
            //apply stake added during extraction.
            for val in self.updates.drain(..) {
                content.add_value(val);
            }
            self.content = Some(content);
            self.notifier.notify_waiters();
            Ok(())
        } else {
            bail!("TakableMap with a existing content".to_string())
        }
    }

    fn is_taken(&self) -> bool {
        self.content.is_none()
    }
}

impl<'a, T1, T2, C1: TakableContent<T1>, C2: TakableContent<T2>> Takable<(C1, C2)>
    for (&'a mut TakableMap<T1, C1>, &'a mut TakableMap<T2, C2>)
{
    fn take(self) -> TakeResult<(C1, C2)> {
        let first = self.0;
        let second = self.1;

        match (first.is_taken(), second.is_taken()) {
            (true, true) | (false, false) => first.take().and_then(second.take()),
            (true, false) => {
                match first.take() {
                    TakeResult::Taken(notif) => TakeResult::Taken(notif),
                    TakeResult::Map(_) => unreachable!(), //tested before.
                }
            }
            (false, true) => {
                match second.take() {
                    TakeResult::Taken(notif) => TakeResult::Taken(notif),
                    TakeResult::Map(_) => unreachable!(), //tested before.
                }
            }
        }
    }

    fn merge(self, content: (C1, C2)) -> anyhow::Result<()> {
        self.0
            .merge(content.0)
            .and_then(|_| self.1.merge(content.1))
    }

    fn is_taken(&self) -> bool {
        self.0.is_taken() && self.1.is_taken()
    }
}

///A struct that hold a collection call content that can be taken during some time and merged after.
///During the time the content is taken, new added values are cached and added to the content after the merge.
///It allow to process struct content while allowing to still update it without lock.
#[derive(Default, Debug)]
pub struct TakableMap<T, C: TakableContent<T>> {
    pub content: Option<C>,
    pub updates: Vec<UpdateAction<T>>,
    notifier: Arc<Notify>,
}

impl<T: Default, C: TakableContent<T> + Default> TakableMap<T, C> {
    pub fn new(content: C) -> Self {
        TakableMap {
            content: Some(content),
            updates: vec![],
            notifier: Arc::new(Notify::new()),
        }
    }

    //add a value to the content if not taken or put it in the update waiting list.
    //Use force_in_update to force the insert in update waiting list.
    pub fn add_value(&mut self, val: UpdateAction<T>, force_in_update: bool) {
        //during extract push the new update or
        //don't insert now account change that has been done in next epoch.
        //put in update pool to be merged next epoch change.
        match self.content.is_none() || force_in_update {
            true => self.updates.push(val),
            false => {
                let content = self.content.as_mut().unwrap(); //unwrap tested
                content.add_value(val);
            }
        }
    }
}

// pub fn take<T: Default, C: TakableContent<T> + Default>(
//     map: &mut TakableMap<T, C>,
// ) -> anyhow::Result<C> {
//     if map.is_taken() {
//         bail!("TakableMap already taken. Try later");
//     }
//     let new_store = std::mem::take(map);
//     let (new_store, content) = new_store.take();
//     *map = new_store;
//     Ok(content)
// }

// pub fn merge<T: Default, C: TakableContent<T> + Default>(
//     map: &mut TakableMap<T, C>,
//     content: C,
// ) -> anyhow::Result<()> {
//     if !map.is_taken() {
//         bail!("TakableMap merge of non taken map. Try later");
//     }
//     let new_store = std::mem::take(map);
//     let new_store = new_store.merge(content);
//     *map = new_store;
//     Ok(())
// }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_takable_struct() {
        impl TakableContent<u64> for Vec<u64> {
            fn add_value(&mut self, val: UpdateAction<u64>) {
                match val {
                    UpdateAction::Notify(account, _) => self.push(account),
                    UpdateAction::Remove(_, _) => (),
                    UpdateAction::None => (),
                }
            }
        }

        let content: Vec<u64> = vec![];
        let mut takable = TakableMap::new(content);
        takable.add_value(UpdateAction::Notify(23, 0), false);
        assert_eq!(takable.content.as_ref().unwrap().len(), 1);

        takable.add_value(UpdateAction::Notify(24, 0), true);
        assert_eq!(takable.content.as_ref().unwrap().len(), 1);
        assert_eq!(takable.updates.len(), 1);

        let take_content = (&mut takable).take();
        assert_take_content_map(&take_content, 1);
        assert_eq!(takable.updates.len(), 1);
        let take_content = (&mut takable).take();
        assert_take_content_taken(&take_content);
        assert!(takable.content.is_none());
        assert_eq!(takable.updates.len(), 1);
        takable.add_value(UpdateAction::Notify(25, 0), false);
        assert_eq!(takable.updates.len(), 2);
        let content = match take_content {
            TakeResult::Taken(_) => panic!("not a content"),
            TakeResult::Map(content) => content,
        };
        takable.merge(content);
        assert_eq!(takable.content.as_ref().unwrap().len(), 3);
        assert_eq!(takable.updates.len(), 0);

        //merge(&mut takable, vec![]);
        //assert!(err.is_err());
    }

    fn assert_take_content_map(take_content: &TakeResult<Vec<u64>>, len: usize) {
        match take_content {
            TakeResult::Taken(_) => assert!(false),
            TakeResult::Map(content) => assert_eq!(content.len(), len),
        }
    }
    fn assert_take_content_taken(take_content: &TakeResult<Vec<u64>>) {
        match take_content {
            TakeResult::Taken(_) => (),
            TakeResult::Map(_) => assert!(false),
        }
    }
}
