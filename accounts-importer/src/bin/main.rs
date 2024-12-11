use std::fs;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use log::info;
use reqwest::blocking::{Client, get};
use reqwest::redirect::Policy;
use solana_accounts_db::accounts_hash::AccountsHashKind;
use solana_download_utils::{download_file, download_snapshot_archive};
use solana_runtime::snapshot_archive_info::SnapshotArchiveInfoGetter;
use solana_runtime::snapshot_hash::SnapshotHash;
use solana_runtime::snapshot_package::SnapshotKind;
use solana_runtime::snapshot_utils;
use solana_runtime::snapshot_utils::ArchiveFormat;
use solana_sdk::clock::Slot;
use solana_sdk::hash::Hash;

use accounts_importer::import;

pub struct TestConsumer {}

// #[tokio::main]
fn main() {
    let full_path = PathBuf::from_str("/tmp/lite-full").unwrap();

    let url = "https://api.testnet.solana.com/incremental-snapshot.tar.bz2";

    let client = Client::builder()
        .redirect(Policy::none()) // Disable automatic redirects
        .build().unwrap();

    let response = client.get(url).send().unwrap();

    // Save the response content
    let content = response.bytes().unwrap();
    let text = String::from_utf8(content.to_vec()).unwrap();
    println!("{text}");

    //
    // // https://api.mainnet-beta.solana.com/incremental-snapshot.tar.bz2 - 303 -> /incremental-snapshot-306231258-306254131-B4aDg39Wyemk2VV9K2Mgq7zNq6Vk11hWbqut4KprSfLt.tar.zst
    //
    // let text = "/incremental-snapshot-306231258-306254131-B4aDg39Wyemk2VV9K2Mgq7zNq6Vk11hWbqut4KprSfLt.tar.zst";
    //
    if let Some(snapshot_data) = text.strip_prefix("/incremental-snapshot-")
        .and_then(|s| s.strip_suffix(".tar.zst")) {
        let parts: Vec<&str> = snapshot_data.split('-').collect();

        // incremental_snap_slot = int(snap_location_.split("-")[2])
        // snap_slot_ = int(snap_location_.split("-")[3])
        // slots_diff = current_slot - snap_slot_


        if parts.len() == 3 {
            let incremental_snap_slot = parts[0].parse::<u64>().unwrap();
            let snap_slot = parts[1].parse::<u64>().unwrap();
            // slot_diff = current_slot - snap_slot

            let hash = parts[2];

            println!("incremental 1: {}", incremental_snap_slot);
            println!("full 2: {}", snap_slot);
            println!("Hash: {}", hash);

            // let address = "api.mainnet-beta.solana.com:80";
            let address = "api.testnet.solana.com:443";

            let rpc = match address.to_socket_addrs() {
                Ok(mut socket_addrs) => {
                    if let Some(socket_addr) = socket_addrs.next() {
                        println!("Resolved SocketAddr: {}", &socket_addr);
                        socket_addr
                    } else {
                        panic!("No SocketAddr found for the hostname.");
                    }
                }
                Err(e) => {
                    panic!("Failed to resolve hostname: {}", e);
                }
            };

            download_snapshot(
                "https://api.testnet.solana.com",
                &*full_path,
                Path::new("/tmp/lite-incr"),
                (
                    Slot::from(snap_slot),
                    SnapshotHash(Hash::from_str(hash).unwrap()),
                ),
                SnapshotKind::IncrementalSnapshot(Slot::from(incremental_snap_slot)),
                NonZeroUsize::new(10000000).unwrap(),
                NonZeroUsize::new(10000000).unwrap(),
                true,
            ).unwrap();
        } else {
            println!("Unexpected format: {:?}", parts);
        }
    } else {
        println!("String does not match expected format.");
    }

    // let full_snapshot_archive_info = snapshot_utils::get_highest_full_snapshot_archive_info(&full_path).unwrap();
    //
    // download_snapshot_archive(
    //     &SocketAddr::from_str("mango.rpcpool.com/1099647b-733f-4d0d-b1c1-2353a4f60cf8").unwrap(),
    //     &*full_path,
    //     Path::new("/tmp/lite-incr"),
    //     (
    //         full_snapshot_archive_info.slot(),
    //         *full_snapshot_archive_info.hash(),
    //     ),
    //     SnapshotKind::FullSnapshot,
    //     NonZeroUsize::new(1).unwrap(),
    //     NonZeroUsize::new(1).unwrap(),
    //     false,
    //     &mut None,
    // ).unwrap();

    // let archive_path = PathBuf::from_str("/tmp/snapshot.tar.zst").unwrap();
    //
    // let (mut rx, jh) = import(archive_path).await;
    // while let Some(r) = rx.recv().await {
    //     println!("{r:#?}")
    // }
    //
    // let _ = tokio::join!(jh);
}

pub fn download_snapshot(
    host: &str,
    full_snapshot_archives_dir: &Path,
    incremental_snapshot_archives_dir: &Path,
    desired_snapshot_hash: (Slot, SnapshotHash),
    snapshot_kind: SnapshotKind,
    maximum_full_snapshot_archives_to_retain: NonZeroUsize,
    maximum_incremental_snapshot_archives_to_retain: NonZeroUsize,
    use_progress_bar: bool,
) -> Result<(), String> {
    snapshot_utils::purge_old_snapshot_archives(
        full_snapshot_archives_dir,
        incremental_snapshot_archives_dir,
        maximum_full_snapshot_archives_to_retain,
        maximum_incremental_snapshot_archives_to_retain,
    );

    let snapshot_archives_remote_dir =
        snapshot_utils::build_snapshot_archives_remote_dir(match snapshot_kind {
            SnapshotKind::FullSnapshot => full_snapshot_archives_dir,
            SnapshotKind::IncrementalSnapshot(_) => incremental_snapshot_archives_dir,
        });
    fs::create_dir_all(&snapshot_archives_remote_dir).unwrap();

    for archive_format in [
        ArchiveFormat::TarZstd,
    ] {
        let destination_path = match snapshot_kind {
            SnapshotKind::FullSnapshot => snapshot_utils::build_full_snapshot_archive_path(
                &snapshot_archives_remote_dir,
                desired_snapshot_hash.0,
                &desired_snapshot_hash.1,
                archive_format,
            ),
            SnapshotKind::IncrementalSnapshot(base_slot) => {
                snapshot_utils::build_incremental_snapshot_archive_path(
                    &snapshot_archives_remote_dir,
                    base_slot,
                    desired_snapshot_hash.0,
                    &desired_snapshot_hash.1,
                    archive_format,
                )
            }
        };

        if destination_path.is_file() {
            return Ok(());
        }

        match download_file(
            &format!(
                "{}/{}",
                host,
                destination_path.file_name().unwrap().to_str().unwrap()
            ),
            &destination_path,
            use_progress_bar,
            &mut None,
        ) {
            Ok(()) => return Ok(()),
            Err(err) => info!("{}", err),
        }
    }
    Err(format!(
        "Failed to download a snapshot archive for slot {} from {}",
        desired_snapshot_hash.0, host
    ))
}
