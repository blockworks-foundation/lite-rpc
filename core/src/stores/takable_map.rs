use anyhow::bail;
use futures::{future::join_all, stream::FuturesUnordered};
use solana_sdk::{pubkey::Pubkey, slot_history::Slot};
use std::sync::Arc;
use tokio::{sync::Notify, task::JoinHandle};

#[derive(Debug, Clone)]
pub enum UpdateAction<Account> {
    Notify(Slot, Account),
    Remove(Pubkey, Slot),
}

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
}

//Takable struct code
pub trait TakableContent<T>: Default + Clone {
    fn add_value(&mut self, val: UpdateAction<T>);
}

//Takable struct code
pub trait Takable<C> {
    fn take(self) -> TakeResult<C>;
    fn merge(self, content: C) -> anyhow::Result<()>;
    fn is_taken(&self) -> bool;
}

impl<'a, T: Clone, C: TakableContent<T>> Takable<C> for &'a mut TakableMap<T, C> {
    fn take(self) -> TakeResult<C> {
        match self.content.take() {
            Some(content) => TakeResult::Map(content),
            None => TakeResult::Taken(vec![Arc::clone(&self.notifier)]),
        }
    }

    fn merge(self, mut content: C) -> anyhow::Result<()> {
        if self.content.is_none() {
            //apply stake added during extraction.
            for val in self.updates.drain(..) {
                content.add_value(val);
            }
            self.content = Some(content);
            self.notifier.notify_one();
            Ok(())
        } else {
            bail!("TakableMap with a existing content".to_string())
        }
    }

    fn is_taken(&self) -> bool {
        self.content.is_none()
    }
}

impl<'a, T1: Clone, T2: Clone, C1: TakableContent<T1>, C2: TakableContent<T2>> Takable<(C1, C2)>
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

pub async fn wait_for_merge_or_get_content<NotifyContent: std::marker::Send + 'static, C>(
    take_map: impl Takable<C>,
    notify_content: NotifyContent,
    waiter_futures: &mut FuturesUnordered<JoinHandle<NotifyContent>>,
) -> Option<(C, NotifyContent)> {
    match take_map.take() {
        TakeResult::Map(content) => Some((content, notify_content)),
        TakeResult::Taken(stake_notify) => {
            let notif_jh = tokio::spawn({
                async move {
                    let notifs = stake_notify
                        .iter()
                        .map(|n| n.notified())
                        .collect::<Vec<tokio::sync::futures::Notified>>();
                    join_all(notifs).await;
                    notify_content
                }
            });
            waiter_futures.push(notif_jh);
            None
        }
    }
}

///A struct that hold a collection call content that can be taken during some time and merged after.
///During the time the content is taken, new added values are cached and added to the content after the merge.
///It allow to process struct content while allowing to still update it without lock.
#[derive(Default, Debug, Clone)]
pub struct TakableMap<T: Clone, C: TakableContent<T>> {
    pub content: Option<C>,
    pub updates: Vec<UpdateAction<T>>,
    notifier: Arc<Notify>,
}

impl<T: Default + Clone, C: TakableContent<T> + Default> TakableMap<T, C> {
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
        //log::info!("tm u:{} c:{} f:{}", self.updates.len(), self.content.is_none(), force_in_update);
        match self.content.is_none() || force_in_update {
            true => self.updates.push(val),
            false => {
                let content = self.content.as_mut().unwrap(); //unwrap tested
                content.add_value(val);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_takable_struct() {
        impl TakableContent<u64> for Vec<u64> {
            fn add_value(&mut self, val: UpdateAction<u64>) {
                match val {
                    UpdateAction::Notify(account, _) => self.push(account),
                    UpdateAction::Remove(_, _) => (),
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
        let content = match take_content {
            TakeResult::Taken(_) => panic!("not a content"),
            TakeResult::Map(content) => content,
        };
        assert_eq!(takable.updates.len(), 1);
        let take_content = (&mut takable).take();
        assert_take_content_taken(&take_content);
        let notifier = match take_content {
            TakeResult::Taken(notifier) => notifier,
            TakeResult::Map(_) => panic!("not a notifier"),
        };
        assert_eq!(notifier.len(), 1);
        let notif_jh = tokio::spawn(async move {
            notifier[0].as_ref().notified().await;
        });

        assert!(takable.content.is_none());
        assert_eq!(takable.updates.len(), 1);
        takable.add_value(UpdateAction::Notify(25, 0), false);
        assert_eq!(takable.updates.len(), 2);
        takable.merge(content).unwrap();
        assert_eq!(takable.content.as_ref().unwrap().len(), 3);
        assert_eq!(takable.updates.len(), 0);

        //wait for notifier
        if tokio::time::timeout(std::time::Duration::from_millis(1000), notif_jh)
            .await
            .is_err()
        {
            panic!("take notifier timeout");
        }
    }

    fn assert_take_content_map(take_content: &TakeResult<Vec<u64>>, len: usize) {
        match take_content {
            TakeResult::Taken(_) => unreachable!(),
            TakeResult::Map(content) => assert_eq!(content.len(), len),
        }
    }
    fn assert_take_content_taken(take_content: &TakeResult<Vec<u64>>) {
        match take_content {
            TakeResult::Taken(_) => (),
            TakeResult::Map(_) => unreachable!(),
        }
    }
}
