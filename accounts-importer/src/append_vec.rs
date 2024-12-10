// Copyright 2022 Solana Foundation.
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
// Source: solana/runtime/src/append_vec.rs

use {
    log::*,
    memmap2::{Mmap, MmapMut},
    solana_accounts_db::{
        account_storage::meta::{AccountMeta, StoredMeta},
        accounts_file::ALIGN_BOUNDARY_OFFSET,
        append_vec::MAXIMUM_APPEND_VEC_FILE_SIZE,
        u64_align,
    },
    solana_sdk::{
        account::{Account, AccountSharedData},
        hash::Hash,
    },
    std::{
        convert::TryFrom,
        fs::OpenOptions,
        io::{self, Read},
        mem,
        path::Path,
    },
};

/// References to account data stored elsewhere. Getting an `Account` requires cloning
/// (see `StoredAccountMeta::clone_account()`).
#[derive(PartialEq, Eq, Debug)]
pub struct StoredAccountMeta<'a> {
    pub meta: &'a StoredMeta,
    /// account data
    pub account_meta: &'a AccountMeta,
    pub data: &'a [u8],
    pub offset: usize,
    pub stored_size: usize,
    pub hash: &'a Hash,
}

impl<'a> StoredAccountMeta<'a> {
    /// Return a new Account by copying all the data referenced by the `StoredAccountMeta`.
    pub fn clone_account(&self) -> AccountSharedData {
        AccountSharedData::from(Account {
            lamports: self.account_meta.lamports,
            owner: self.account_meta.owner,
            executable: self.account_meta.executable,
            rent_epoch: self.account_meta.rent_epoch,
            data: self.data.to_vec(),
        })
    }
}

/// A thread-safe, file-backed block of memory used to store `Account` instances. Append operations
/// are serialized such that only one thread updates the internal `append_lock` at a time. No
/// restrictions are placed on reading. That is, one may read items from one thread while another
/// is appending new items.
pub struct AppendVec {
    /// A file-backed block of memory that is used to store the data for each appended item.
    map: Mmap,

    /// The number of bytes used to store items, not the number of items.
    current_len: usize,

    /// The number of bytes available for storing items.
    file_size: u64,

    slot: u64,
}

impl AppendVec {
    fn sanitize_len_and_size(current_len: usize, file_size: usize) -> io::Result<()> {
        if file_size == 0 {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("too small file size {} for AppendVec", file_size),
            ))
        } else if usize::try_from(MAXIMUM_APPEND_VEC_FILE_SIZE)
            .map(|max| file_size > max)
            .unwrap_or(true)
        {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("too large file size {} for AppendVec", file_size),
            ))
        } else if current_len > file_size {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("current_len is larger than file size ({})", file_size),
            ))
        } else {
            Ok(())
        }
    }

    /// how many more bytes can be stored in this append vec
    pub const fn remaining_bytes(&self) -> u64 {
        (self.capacity()).saturating_sub(self.len() as u64)
    }

    pub const fn len(&self) -> usize {
        self.current_len
    }

    pub const fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub const fn capacity(&self) -> u64 {
        self.file_size
    }

    pub fn new_from_file<P: AsRef<Path>>(
        path: P,
        current_len: usize,
        slot: u64,
    ) -> io::Result<Self> {
        let data = OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(&path)?;

        let file_size = std::fs::metadata(&path)?.len();
        AppendVec::sanitize_len_and_size(current_len, file_size as usize)?;

        let map = unsafe {
            let result = Mmap::map(&data);
            if result.is_err() {
                // for vm.max_map_count, error is: {code: 12, kind: Other, message: "Cannot allocate memory"}
                info!("memory map error: {:?}. This may be because vm.max_map_count is not set correctly.", result);
            }
            result?
        };

        let new = AppendVec {
            map,
            current_len,
            file_size,
            slot,
        };

        Ok(new)
    }

    pub fn new_from_reader<R: Read>(
        reader: &mut R,
        current_len: usize,
        slot: u64,
    ) -> io::Result<Self> {
        let mut map = MmapMut::map_anon(current_len)?;
        io::copy(&mut reader.take(current_len as u64), &mut map.as_mut())?;
        Ok(AppendVec {
            map: map.make_read_only()?,
            current_len,
            file_size: current_len as u64,
            slot,
        })
    }

    /// Get a reference to the data at `offset` of `size` bytes if that slice
    /// doesn't overrun the internal buffer. Otherwise return None.
    /// Also return the offset of the first byte after the requested data that
    /// falls on a 64-byte boundary.
    fn get_slice(&self, offset: usize, size: usize) -> Option<(&[u8], usize)> {
        let (next, overflow) = offset.overflowing_add(size);
        if overflow || next > self.len() {
            return None;
        }
        let data = &self.map[offset..next];
        let next = u64_align!(next);

        Some((
            //UNSAFE: This unsafe creates a slice that represents a chunk of self.map memory
            //The lifetime of this slice is tied to &self, since it points to self.map memory
            unsafe { std::slice::from_raw_parts(data.as_ptr(), size) },
            next,
        ))
    }

    /// Return a reference to the type at `offset` if its data doesn't overrun the internal buffer.
    /// Otherwise return None. Also return the offset of the first byte after the requested data
    /// that falls on a 64-byte boundary.
    fn get_type<'a, T>(&self, offset: usize) -> Option<(&'a T, usize)> {
        let (data, next) = self.get_slice(offset, mem::size_of::<T>())?;
        let ptr: *const T = data.as_ptr() as *const T;
        //UNSAFE: The cast is safe because the slice is aligned and fits into the memory
        //and the lifetime of the &T is tied to self, which holds the underlying memory map
        Some((unsafe { &*ptr }, next))
    }

    /// Return account metadata for the account at `offset` if its data doesn't overrun
    /// the internal buffer. Otherwise return None. Also return the offset of the first byte
    /// after the requested data that falls on a 64-byte boundary.
    pub fn get_account<'a>(&'a self, offset: usize) -> Option<(StoredAccountMeta<'a>, usize)> {
        let (meta, next): (&'a StoredMeta, _) = self.get_type(offset)?;
        let (account_meta, next): (&'a AccountMeta, _) = self.get_type(next)?;
        let (hash, next): (&'a Hash, _) = self.get_type(next)?;
        let (data, next) = self.get_slice(next, meta.data_len as usize)?;
        let stored_size = next - offset;
        Some((
            StoredAccountMeta {
                meta,
                account_meta,
                data,
                offset,
                stored_size,
                hash,
            },
            next,
        ))
    }

    pub const fn slot(&self) -> u64 {
        self.slot
    }
}
