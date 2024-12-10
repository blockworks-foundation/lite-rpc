use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;
use std::str::FromStr;

use itertools::Itertools;
use solana_sdk::clock::Slot;
use solana_sdk::pubkey::Pubkey;

use accounts_reader::{append_vec_iter, SnapshotExtractor};
use accounts_reader::archived::ArchiveSnapshotExtractor;

fn main() {
    let archive_path = PathBuf::from_str("/tmp/snapshot.tar.zst").unwrap();
    let mut loader: ArchiveSnapshotExtractor<File> = ArchiveSnapshotExtractor::open(&archive_path).unwrap();

    let mut accounts_per_slot: HashMap<Slot, u64> = HashMap::new();
    let mut updates: HashMap<Pubkey, Vec<Slot>> = HashMap::new();

    for vec in loader.iter() {
        let append_vec = vec.unwrap();
        println!("size: {:?}", append_vec.len());
        for handle in append_vec_iter(&append_vec) {
            let stored = handle.access().unwrap();
            // println!("account {:?}: {}", stored.meta.pubkey, stored.account_meta.lamports);
            let zzz = accounts_per_slot.entry(append_vec.slot()).or_default();
            *zzz += 1;
            updates.entry(stored.meta.pubkey).or_default().push(append_vec.slot());
        }
    }

    for (slot, count) in accounts_per_slot.iter().sorted_by_key(|(slot, _)| *slot).take(100) {
        println!("slot: {:?} count: {:?}", slot, count);
    }

    for (pubkey, slots) in updates.iter().filter(|(_, slots)| slots.len() > 1) {
        println!("pubkey: {:?} slots: {:?}", pubkey, slots);
    }

    for (count, group) in &updates.into_iter().map(|(pubkey, slots)| (pubkey, slots.len()))
        .sorted_by_key(|(_, count)| *count)
        .group_by(|(pubkey, count)| *count) {
        println!("count: {:?} groupsize: {}", count, group.count());
    }
}