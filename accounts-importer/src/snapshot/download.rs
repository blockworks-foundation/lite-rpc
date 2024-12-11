// Copyright 2024 Solana Foundation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file contains code vendored from https://github.com/solana-labs/solana


use std::fs;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

use anyhow::bail;
use log::info;
use solana_download_utils::download_file;
use solana_runtime::snapshot_hash::SnapshotHash;
use solana_runtime::snapshot_package::SnapshotKind;
use solana_runtime::snapshot_utils;
use solana_runtime::snapshot_utils::ArchiveFormat;
use solana_sdk::clock::Slot;
use tokio::task;
use tokio::task::JoinHandle;

use crate::snapshot::HostUrl;

pub(crate) async fn download_snapshot(
    host: HostUrl,
    full_snapshot_archives_dir: PathBuf,
    incremental_snapshot_archives_dir: PathBuf,
    desired_snapshot_hash: (Slot, SnapshotHash),
    snapshot_kind: SnapshotKind,
    maximum_full_snapshot_archives_to_retain: NonZeroUsize,
    maximum_incremental_snapshot_archives_to_retain: NonZeroUsize,
    use_progress_bar: bool,
) -> JoinHandle<anyhow::Result<PathBuf>> {
    task::spawn_blocking(move || {
        let full_snapshot_archives_dir = &full_snapshot_archives_dir;
        let incremental_snapshot_archives_dir = &incremental_snapshot_archives_dir;

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
                return Ok(destination_path);
            }

            let url = format!("{}/{}", host.0, destination_path.file_name().unwrap().to_str().unwrap());
            info!("Attempt to download: {}", &url);

            match download_file(
                &format!(
                    "{}/{}",
                    host.0,
                    destination_path.file_name().unwrap().to_str().unwrap()
                ),
                &destination_path,
                use_progress_bar,
                &mut None,
            ) {
                Ok(()) => return Ok(destination_path),
                Err(err) => bail!("{}", err),
            }
        }

        bail!(
            "Failed to download a snapshot archive for slot {} from {}",
            desired_snapshot_hash.0, host.0
        )
    })
}
