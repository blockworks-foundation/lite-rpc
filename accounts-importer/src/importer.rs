use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use solana_sdk::account::{Account, ReadableAccount};
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;

use solana_lite_rpc_core::structures::account_data::AccountData;

use crate::archived::ArchiveSnapshotExtractor;
use crate::{append_vec_iter, SnapshotExtractor};

pub async fn import(archive_path: PathBuf) -> (Receiver<AccountData>, JoinHandle<()>) {
    let (tx, rx) = mpsc::channel::<AccountData>(10_000);

    let handle: JoinHandle<()> = tokio::task::spawn_blocking(move || {
        let mut extractor: ArchiveSnapshotExtractor<File> =
            ArchiveSnapshotExtractor::open(&archive_path).expect(
                format!(
                    "Unable to load archive file: {}",
                    archive_path.to_str().unwrap()
                )
                .as_str(),
            );

        for append_vec in extractor.iter() {
            let tx = tx.clone();

            tokio::task::spawn(async move {
                let append_vec = append_vec.unwrap();

                for handle in append_vec_iter(&append_vec) {
                    if let Some((account_meta, _offset)) = append_vec.get_account(handle.offset) {
                        let shared_data = account_meta.clone_account();

                        tx.send(AccountData {
                            pubkey: account_meta.meta.pubkey,
                            account: Arc::new(Account {
                                lamports: shared_data.lamports(),
                                data: Vec::from(shared_data.data()),
                                owner: shared_data.owner().clone(),
                                executable: shared_data.executable(),
                                rent_epoch: shared_data.rent_epoch(),
                            }),
                            updated_slot: append_vec.slot(),
                        })
                        .await
                        .expect("Failed to send account data");
                    }
                }
            });
        }
    });

    (rx, handle)
}
