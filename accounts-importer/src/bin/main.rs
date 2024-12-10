use std::path::PathBuf;
use std::str::FromStr;
use accounts_importer::import;


pub struct TestConsumer {}

#[tokio::main]
async fn main() {
    let archive_path = PathBuf::from_str("/tmp/snapshot.tar.zst").unwrap();

    let (mut rx, jh) = import(archive_path).await;
    while let Some(r) = rx.recv().await {
        println!("{r:#?}")
    }

    let _ = tokio::join!(jh);
}
