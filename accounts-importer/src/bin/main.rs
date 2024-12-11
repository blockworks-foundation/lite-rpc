use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::str::FromStr;

use solana_runtime::snapshot_archive_info::SnapshotArchiveInfoGetter;

use accounts_importer::snapshot::{Config, HostUrl, Loader};

pub struct TestConsumer {}

#[tokio::main]
async fn main() {
    let loader = Loader::new(Config {
        hosts: Box::new([
            HostUrl::from_str("https://api.testnet.solana.com").unwrap()
        ]),
        full_snapshot_path: PathBuf::from_str("/tmp/lite-full").unwrap(),
        incremental_snapshot_path: PathBuf::from_str("/tmp/lite-incr").unwrap(),
        maximum_full_snapshot_archives_to_retain: NonZeroUsize::new(100).unwrap(),
        maximum_incremental_snapshot_archives_to_retain: NonZeroUsize::new(100).unwrap(),
    });

    let file = loader.load_latest_snapshot().await.unwrap();
    print!("{file:#?}");

    // let result = latest_incremental_snapshot([
    //     // HostUrl::from_str("https://api.testnet.solana.com").unwrap(),
    //     HostUrl::from_str("https://api.mainnet-beta.solana.com").unwrap(),
    // ]).await.unwrap();

    // println!("{result:#?}")
    // let full_path = PathBuf::from_str("/tmp/lite-full").unwrap();
    //
    // let url = "https://api.testnet.solana.com/snapshot.tar.bz2";
    //
    // let client = Client::builder()
    //     .redirect(Policy::none()) // Disable automatic redirects
    //     .build().unwrap();
    //
    // let response = client.get(url).send().unwrap();
    //
    // // Save the response content
    // let content = response.bytes().unwrap();
    // let text = String::from_utf8(content.to_vec()).unwrap();
    // println!("{text}");
    //
    // //
    // // // https://api.mainnet-beta.solana.com/snapshot.tar.bz2 - 303 -> /snapshot-306254131-B4aDg39Wyemk2VV9K2Mgq7zNq6Vk11hWbqut4KprSfLt.tar.zst
    // //
    // // let text = "/incremental-snapshot-306231258-306254131-B4aDg39Wyemk2VV9K2Mgq7zNq6Vk11hWbqut4KprSfLt.tar.zst";
    // //
    // if let Some(snapshot_data) = text.strip_prefix("/snapshot-")
    //     .and_then(|s| s.strip_suffix(".tar.zst")) {
    //     let parts: Vec<&str> = snapshot_data.split('-').collect();
    //
    //     // incremental_snap_slot = int(snap_location_.split("-")[2])
    //     // snap_slot_ = int(snap_location_.split("-")[3])
    //     // slots_diff = current_slot - snap_slot_
    //
    //
    //     if parts.len() == 2 {
    //         let snap_slot = parts[0].parse::<u64>().unwrap();
    //         // slot_diff = current_slot - snap_slot
    //
    //         let hash = parts[1];
    //
    //         println!("full 2: {}", snap_slot);
    //         println!("Hash: {}", hash);
    //
    //         // let address = "api.mainnet-beta.solana.com:80";
    //         let address = "api.testnet.solana.com:443";
    //
    //         download_snapshot(
    //             "https://api.testnet.solana.com",
    //             &*full_path,
    //             Path::new("/tmp/lite-incr"),
    //             (
    //                 Slot::from(snap_slot),
    //                 SnapshotHash(Hash::from_str(hash).unwrap()),
    //             ),
    //             SnapshotKind::FullSnapshot,
    //             NonZeroUsize::new(10000000).unwrap(),
    //             NonZeroUsize::new(10000000).unwrap(),
    //             true,
    //         ).unwrap();
    //     } else {
    //         println!("Unexpected format: {:?}", parts);
    //     }
    // } else {
    //     println!("String does not match expected format.");
    // }


// incremental

    // let full_path = PathBuf::from_str("/tmp/lite-full").unwrap();
    //
    // let url = "https://api.testnet.solana.com/incremental-snapshot.tar.bz2";
    //
    // let client = Client::builder()
    //     .redirect(Policy::none()) // Disable automatic redirects
    //     .build().unwrap();
    //
    // let response = client.get(url).send().unwrap();
    //
    // // Save the response content
    // let content = response.bytes().unwrap();
    // let text = String::from_utf8(content.to_vec()).unwrap();
    // println!("{text}");
    //
    // //
    // // // https://api.mainnet-beta.solana.com/incremental-snapshot.tar.bz2 - 303 -> /incremental-snapshot-306231258-306254131-B4aDg39Wyemk2VV9K2Mgq7zNq6Vk11hWbqut4KprSfLt.tar.zst
    // //
    // // let text = "/incremental-snapshot-306231258-306254131-B4aDg39Wyemk2VV9K2Mgq7zNq6Vk11hWbqut4KprSfLt.tar.zst";
    // //
    // if let Some(snapshot_data) = text.strip_prefix("/incremental-snapshot-")
    //     .and_then(|s| s.strip_suffix(".tar.zst")) {
    //     let parts: Vec<&str> = snapshot_data.split('-').collect();
    //
    //     // incremental_snap_slot = int(snap_location_.split("-")[2])
    //     // snap_slot_ = int(snap_location_.split("-")[3])
    //     // slots_diff = current_slot - snap_slot_
    //
    //
    //     if parts.len() == 3 {
    //         let incremental_snap_slot = parts[0].parse::<u64>().unwrap();
    //         let snap_slot = parts[1].parse::<u64>().unwrap();
    //         // slot_diff = current_slot - snap_slot
    //
    //         let hash = parts[2];
    //
    //         println!("incremental 1: {}", incremental_snap_slot);
    //         println!("full 2: {}", snap_slot);
    //         println!("Hash: {}", hash);
    //
    //         // let address = "api.mainnet-beta.solana.com:80";
    //         let address = "api.testnet.solana.com:443";
    //
    //         let rpc = match address.to_socket_addrs() {
    //             Ok(mut socket_addrs) => {
    //                 if let Some(socket_addr) = socket_addrs.next() {
    //                     println!("Resolved SocketAddr: {}", &socket_addr);
    //                     socket_addr
    //                 } else {
    //                     panic!("No SocketAddr found for the hostname.");
    //                 }
    //             }
    //             Err(e) => {
    //                 panic!("Failed to resolve hostname: {}", e);
    //             }
    //         };
    //
    //         download_snapshot(
    //             "https://api.testnet.solana.com",
    //             &*full_path,
    //             Path::new("/tmp/lite-incr"),
    //             (
    //                 Slot::from(snap_slot),
    //                 SnapshotHash(Hash::from_str(hash).unwrap()),
    //             ),
    //             SnapshotKind::IncrementalSnapshot(Slot::from(incremental_snap_slot)),
    //             NonZeroUsize::new(10000000).unwrap(),
    //             NonZeroUsize::new(10000000).unwrap(),
    //             true,
    //         ).unwrap();
    //     } else {
    //         println!("Unexpected format: {:?}", parts);
    //     }
    // } else {
    //     println!("String does not match expected format.");
    // }
}
