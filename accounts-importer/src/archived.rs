use {
    crate::{
        AccountsDbFields, AppendVecIterator, DeserializableVersionedBank, deserialize_from,
        parse_append_vec_name, SerializableAccountStorageEntry, SnapshotError,
        SnapshotExtractor, SnapshotResult,
    },
    log::info,
    std::{
        fs::File,
        io::{BufReader, Read},
        path::{Component, Path},
        pin::Pin,
        time::Instant,
    },
    tar::{Archive, Entries, Entry},
};

use crate::append_vec::AppendVec;

/// Extracts account data from a .tar.zst stream.
pub struct ArchiveSnapshotExtractor<Source>
    where
        Source: Read + Unpin + 'static,
{
    accounts_db_fields: AccountsDbFields<SerializableAccountStorageEntry>,
    _archive: Pin<Box<Archive<zstd::Decoder<'static, BufReader<Source>>>>>,
    entries: Option<Entries<'static, zstd::Decoder<'static, BufReader<Source>>>>,
}

impl<Source> SnapshotExtractor for ArchiveSnapshotExtractor<Source>
    where
        Source: Read + Unpin + 'static,
{
    fn iter(&mut self) -> AppendVecIterator<'_> {
        Box::new(self.unboxed_iter())
    }
}

impl<Source> ArchiveSnapshotExtractor<Source>
    where
        Source: Read + Unpin + 'static,
{
    pub fn from_reader(source: Source) -> SnapshotResult<Self> {
        let tar_stream = zstd::stream::read::Decoder::new(source)?;
        let mut archive = Box::pin(Archive::new(tar_stream));

        // This is safe as long as we guarantee that entries never gets accessed past drop.
        let archive_static = unsafe { &mut *((&mut *archive) as *mut Archive<_>) };
        let mut entries = archive_static.entries()?;

        // Search for snapshot manifest.
        let mut snapshot_file: Option<Entry<_>> = None;
        for entry in entries.by_ref() {
            let entry = entry?;
            let path = entry.path()?;
            if Self::is_snapshot_manifest_file(&path) {
                snapshot_file = Some(entry);
                break;
            } else if Self::is_appendvec_file(&path) {
                // TODO Support archives where AppendVecs precede snapshot manifests
                return Err(SnapshotError::UnexpectedAppendVec);
            }
        }
        let snapshot_file = snapshot_file.ok_or(SnapshotError::NoSnapshotManifest)?;
        //let snapshot_file_len = snapshot_file.size();
        let snapshot_file_path = snapshot_file.path()?.as_ref().to_path_buf();

        info!("Opening snapshot manifest: {:?}", &snapshot_file_path);
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

        Ok(ArchiveSnapshotExtractor {
            _archive: archive,
            accounts_db_fields,
            entries: Some(entries),
        })
    }

    fn unboxed_iter(&mut self) -> impl Iterator<Item=SnapshotResult<AppendVec>> + '_ {
        self.entries
            .take()
            .into_iter()
            .flatten()
            .filter_map(|entry| {
                let mut entry = match entry {
                    Ok(x) => x,
                    Err(e) => return Some(Err(e.into())),
                };
                let path = match entry.path() {
                    Ok(x) => x,
                    Err(e) => return Some(Err(e.into())),
                };
                let (slot, id) = path.file_name().and_then(parse_append_vec_name)?;
                Some(self.process_entry(&mut entry, slot, id))
            })
    }

    fn process_entry(
        &self,
        entry: &mut Entry<'static, zstd::Decoder<'static, BufReader<Source>>>,
        slot: u64,
        id: u64,
    ) -> SnapshotResult<AppendVec> {
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
        Ok(AppendVec::new_from_reader(
            entry,
            known_vec.accounts_current_len,
            slot,
        )?)
    }

    fn is_snapshot_manifest_file(path: &Path) -> bool {
        let mut components = path.components();
        if components.next() != Some(Component::Normal("snapshots".as_ref())) {
            return false;
        }
        let slot_number_str_1 = match components.next() {
            Some(Component::Normal(slot)) => slot,
            _ => return false,
        };
        // Check if slot number file is valid u64.
        if slot_number_str_1
            .to_str()
            .and_then(|s| s.parse::<u64>().ok())
            .is_none()
        {
            return false;
        }
        let slot_number_str_2 = match components.next() {
            Some(Component::Normal(slot)) => slot,
            _ => return false,
        };
        components.next().is_none() && slot_number_str_1 == slot_number_str_2
    }

    fn is_appendvec_file(path: &Path) -> bool {
        let mut components = path.components();
        if components.next() != Some(Component::Normal("accounts".as_ref())) {
            return false;
        }
        let name = match components.next() {
            Some(Component::Normal(c)) => c,
            _ => return false,
        };
        components.next().is_none() && parse_append_vec_name(name).is_some()
    }
}

impl ArchiveSnapshotExtractor<File> {
    pub fn open(path: &Path) -> SnapshotResult<Self> {
        Self::from_reader(File::open(path)?)
    }
}
