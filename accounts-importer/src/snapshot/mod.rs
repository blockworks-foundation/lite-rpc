use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::str::FromStr;

use solana_runtime::snapshot_package::SnapshotKind;

use crate::snapshot::download::download_snapshot;
use crate::snapshot::find::{latest_full_snapshot, latest_incremental_snapshot};

pub(crate) mod download;
pub(crate) mod find;

#[derive(Clone, Debug)]
pub struct HostUrl(String);

impl FromStr for HostUrl {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(HostUrl(s.to_string()))
    }
}

pub struct Config {
    pub hosts: Box<[HostUrl]>,
    pub full_snapshot_path: PathBuf,
    pub incremental_snapshot_path: PathBuf,
    pub maximum_full_snapshot_archives_to_retain: NonZeroUsize,
    pub maximum_incremental_snapshot_archives_to_retain: NonZeroUsize,
}

pub struct Loader {
    cfg: Config,
}

impl Loader {
    pub fn new(cfg: Config) -> Self {
        Self {
            cfg
        }
    }

    pub async fn load_latest_snapshot(&self) -> anyhow::Result<PathBuf> {
        let snapshot = latest_full_snapshot(self.cfg.hosts.clone()).await.unwrap();

        download_snapshot(
            snapshot.host,
            self.cfg.full_snapshot_path.clone(),
            self.cfg.incremental_snapshot_path.clone(),
            (snapshot.full_slot, snapshot.hash ),
            SnapshotKind::FullSnapshot,
            self.cfg.maximum_full_snapshot_archives_to_retain,
            self.cfg.maximum_incremental_snapshot_archives_to_retain,
            true,
        ).await.await?
    }

    pub async fn load_latest_incremental_snapshot(&self) -> anyhow::Result<PathBuf> {
        let snapshot = latest_incremental_snapshot(self.cfg.hosts.clone()).await.unwrap();
        download_snapshot(
            snapshot.host,
            self.cfg.full_snapshot_path.clone(),
            self.cfg.incremental_snapshot_path.clone(),
            (snapshot.full_slot, snapshot.hash, ),
            SnapshotKind::IncrementalSnapshot(snapshot.incremental_slot),
            self.cfg.maximum_full_snapshot_archives_to_retain,
            self.cfg.maximum_incremental_snapshot_archives_to_retain,
            true,
        ).await.await?
    }
}

