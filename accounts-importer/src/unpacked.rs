use {
    crate::{
        deserialize_from, parse_append_vec_name, AccountsDbFields, AppendVec, AppendVecIterator,
        DeserializableVersionedBank, ReadProgressTracking, SerializableAccountStorageEntry,
        SnapshotError, SnapshotExtractor, SnapshotResult, SNAPSHOTS_DIR,
    },
    itertools::Itertools,
    log::info,
    solana_runtime::snapshot_utils::SNAPSHOT_STATUS_CACHE_FILENAME,
    std::{
        fs::OpenOptions,
        io::BufReader,
        path::{Path, PathBuf},
        str::FromStr,
        time::Instant,
    },
};

/// Extracts account data from snapshots that were unarchived to a file system.
pub struct UnpackedSnapshotExtractor {
    root: PathBuf,
    accounts_db_fields: AccountsDbFields<SerializableAccountStorageEntry>,
}

impl SnapshotExtractor for UnpackedSnapshotExtractor {
    fn iter(&mut self) -> AppendVecIterator<'_> {
        Box::new(self.unboxed_iter())
    }
}

impl UnpackedSnapshotExtractor {
    pub fn open(
        path: &Path,
        progress_tracking: Box<dyn ReadProgressTracking>,
    ) -> SnapshotResult<Self> {
        let snapshots_dir = path.join(SNAPSHOTS_DIR);
        let status_cache = snapshots_dir.join(SNAPSHOT_STATUS_CACHE_FILENAME);
        if !status_cache.is_file() {
            return Err(SnapshotError::NoStatusCache);
        }

        let snapshot_files = snapshots_dir.read_dir()?;

        let snapshot_file_path = snapshot_files
            .filter_map(|entry| entry.ok())
            .find(|entry| u64::from_str(&entry.file_name().to_string_lossy()).is_ok())
            .map(|entry| entry.path().join(entry.file_name()))
            .ok_or(SnapshotError::NoSnapshotManifest)?;

        info!("Opening snapshot manifest: {:?}", snapshot_file_path);
        let snapshot_file = OpenOptions::new().read(true).open(&snapshot_file_path)?;
        let snapshot_file_len = snapshot_file.metadata()?.len();

        let snapshot_file = progress_tracking.new_read_progress_tracker(
            &snapshot_file_path,
            Box::new(snapshot_file),
            snapshot_file_len,
        )?;
        let mut snapshot_file = BufReader::new(snapshot_file);

        let pre_unpack = Instant::now();
        let versioned_bank: DeserializableVersionedBank = deserialize_from(&mut snapshot_file)?;
        drop(versioned_bank);
        let versioned_bank_post_time = Instant::now();

        let accounts_db_fields: AccountsDbFields<SerializableAccountStorageEntry> =
            deserialize_from(&mut snapshot_file)?;
        let accounts_db_fields_post_time = Instant::now();
        drop(snapshot_file);

        info!(
            "Read bank fields in {:?}",
            versioned_bank_post_time - pre_unpack
        );
        info!(
            "Read accounts DB fields in {:?}",
            accounts_db_fields_post_time - versioned_bank_post_time
        );

        Ok(UnpackedSnapshotExtractor {
            root: path.to_path_buf(),
            accounts_db_fields,
        })
    }

    pub fn unboxed_iter(&self) -> impl Iterator<Item = SnapshotResult<AppendVec>> + '_ {
        std::iter::once(self.iter_streams())
            .flatten_ok()
            .flatten_ok()
    }

    fn iter_streams(&self) -> SnapshotResult<impl Iterator<Item = SnapshotResult<AppendVec>> + '_> {
        let accounts_dir = self.root.join("accounts");
        Ok(accounts_dir
            .read_dir()?
            .filter_map(|f| f.ok())
            .filter_map(|f| {
                let name = f.file_name();
                parse_append_vec_name(&f.file_name()).map(move |parsed| (parsed, name))
            })
            .map(move |((slot, version), name)| {
                self.open_append_vec(slot, version, &accounts_dir.join(name))
            }))
    }

    fn open_append_vec(&self, slot: u64, id: u64, path: &Path) -> SnapshotResult<AppendVec> {
        let known_vecs = self
            .accounts_db_fields
            .0
            .get(&slot)
            .map(|v| &v[..])
            .unwrap_or(&[]);
        let known_vec = known_vecs.iter().find(|entry| entry.id == (id as usize));
        let known_vec = match known_vec {
            None => return Err(SnapshotError::UnexpectedAppendVec),
            Some(v) => v,
        };

        Ok(AppendVec::new_from_file(
            path,
            known_vec.accounts_current_len,
            slot,
        )?)
    }
}
