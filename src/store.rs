use std::{
    collections::HashMap,
    fs::{self, File},
    io::{IoSlice, Read, Seek, SeekFrom, Write},
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::{
    error::KvError,
    helper::system_time_to_bytes,
    record::{Record, RecordType},
    wal::should_rotate,
};

/// The maximum size (in bytes) of uncompacted data before triggering compaction (2KB).
const MAX_COMPACTION_SIZE: u64 = 1024 * 2; // 2KB

/// Number of bytes used to store the record type.
const TYPE_SIZE: usize = 1; // 1 byte for RecordType
/// Number of bytes used to store the length of key/value.
const LEN_SIZE: usize = 4; // 4 bytes for u32 lengths
/// Number of bytes used to store the timestamp.
const TIMESTAMP_SIZE: usize = 8; // 8 bytes timestamp
/// Total size of the record header in bytes.
const HEADER_SIZE: usize = TYPE_SIZE + TIMESTAMP_SIZE + LEN_SIZE + LEN_SIZE;

/// Appends a record to the specified log file.
///
/// # Arguments
/// * `record` - The record to append (Put/Delete).
/// * `file_path` - The path to the log file.
///
/// # Returns
/// Returns the number of bytes written and the offset at which the record was written.
fn append(record: Record, file_path: impl Into<PathBuf>) -> Result<(usize, u64), KvError> {
    let key = record.key;
    let value = record.value;
    let record_type = &[record.record_type as u8];
    let timestamp = system_time_to_bytes(&record.timestamp);
    let key_len = key.len() as u32;
    let value_len = value.len() as u32;

    let mut file = File::options()
        .create(true)
        .append(true)
        .open(file_path.into())?;

    // current size of the log file before appending
    let offset = file.metadata()?.len();

    let key_len_bytes = key_len.to_le_bytes();
    let value_len_bytes = value_len.to_le_bytes();

    // buffer contents: record_type 1byte | timestamp 8bytes | key_size 4bytes | value_size 4bytes | key n-bytes | value n-bytes
    let bufs = [
        IoSlice::new(record_type),
        IoSlice::new(&timestamp),
        IoSlice::new(&key_len_bytes),
        IoSlice::new(&value_len_bytes), // 0 for Delete
        IoSlice::new(key),
        IoSlice::new(value), // Would be empty for Delete
    ];

    let size = file.write_vectored(&bufs)?;

    file.sync_all()?;

    Ok((size, offset))
}

/// Reads a value and timestamp from the log file at the given offset.
///
/// # Arguments
/// * `offset` - The offset in the file to start reading from.
/// * `file_path` - The path to the log file.
///
/// # Returns
/// Returns `Some((value, timestamp))` if a valid Put record is found, otherwise `None`.
///
/// # Errors
/// Returns an error if the file cannot be read or the header is invalid.
///
/// TODO: Update the return type for more flexibility.
fn read(offset: u64, file_path: impl Into<PathBuf>) -> Result<Option<(Vec<u8>, i64)>, KvError> {
    let mut file = File::open(file_path.into())?;

    file.seek(SeekFrom::Start(offset))?;

    let mut header = [0u8; HEADER_SIZE];

    file.read_exact(&mut header)?;

    if header[0] != RecordType::Put as u8 {
        return Ok(None);
    }

    let timestamp = i64::from_le_bytes(
        header[TYPE_SIZE..TYPE_SIZE + TIMESTAMP_SIZE]
            .try_into()
            .expect("timestamp size should be 4bytes"),
    );

    let key_len = u32::from_le_bytes(
        header[TYPE_SIZE + TIMESTAMP_SIZE..TYPE_SIZE + TIMESTAMP_SIZE + LEN_SIZE]
            .try_into()
            .expect("Key size should be 4bytes"),
    ) as usize;

    let value_len = u32::from_le_bytes(
        header[TYPE_SIZE + TIMESTAMP_SIZE + LEN_SIZE
            ..TYPE_SIZE + TIMESTAMP_SIZE + LEN_SIZE + LEN_SIZE]
            .try_into()
            .expect("Value size should be 4bytes"),
    ) as usize;

    // skip the key
    file.seek(SeekFrom::Current(key_len as i64))?;

    let mut value = vec![0u8; value_len];

    file.read_exact(&mut value)?;

    Ok(Some((value.to_vec(), timestamp)))
}

/// The main key-value store structure, holding the in-memory index and managing log files.
#[derive(Debug)]
pub struct KvStore {
    memory_store: HashMap<Vec<u8>, (PathBuf, u64, usize)>, //file_id, offset, size
    dir_path: PathBuf,
    compaction_size: usize,
    current_file_id: u64,
}

impl KvStore {
    /// Opens a key-value store at the given directory path, creating it if it doesn't exist.
    ///
    /// Reconstructs the in-memory index from log files and starts compaction task if needed.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, KvError> {
        let dir_path = path.into();

        if !dir_path.exists() {
            std::fs::create_dir(&dir_path)?;
        }

        let mut store = KvStore {
            memory_store: HashMap::new(),
            dir_path,
            compaction_size: 0,
            current_file_id: 0,
        };

        // re-constructs the in-memory index from log files
        store.recovery()?;

        // task to run compaction
        store.task()?;
        Ok(store)
    }

    /// Checks if compaction should be triggered based on the current compaction size.
    ///
    /// Runs compaction if the uncompacted data exceeds the threshold.
    fn task(&mut self) -> Result<(), KvError> {
        // spawn a task that checks the compaction size after a duration
        if self.compaction_size > MAX_COMPACTION_SIZE as usize {
            self.compaction()?;
        }

        Ok(())
    }

    /// Inserts or updates a key-value pair in the store.
    ///
    /// Appends a Put record to the log and updates the in-memory index.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), KvError> {
        let mut active_path = self.dir_path.join(format!("{}.log", self.current_file_id));

        // should rotate and check file_size (recursively check)
        // Because on start-up the active file might be 1 but 2.log exists and is already full, but after rotating 1.log you get 2.log which is already full
        if should_rotate(&active_path) {
            self.current_file_id += 1;
            active_path = self.dir_path.join(format!("{}.log", self.current_file_id));
        }

        let record = Record {
            record_type: RecordType::Put,
            timestamp: SystemTime::now(),
            key,
            value,
        };

        let (size, offset) = append(record, &active_path)?;

        self.memory_store
            .insert(key.to_vec(), (active_path, offset, size));

        self.compaction_size += size;

        Ok(())
    }

    // TODO: return type should be refactored
    /// Retrieves the value associated with the given key, if it exists.
    ///
    /// Reads the value from the log file using the in-memory index.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
        let (file, offset, ..) = match self.memory_store.get(key) {
            Some(v) => v,
            None => return Ok(None),
        };

        match read(*offset, file)? {
            Some((value, ..)) => Ok(Some(value)),
            None => Ok(None),
        }
    }

    /// Deletes a key-value pair from the store.
    ///
    /// Appends a Delete record to the log and removes the key from the in-memory index.
    pub fn delete(&mut self, key: &[u8]) -> Result<(), KvError> {
        if !self.memory_store.contains_key(key) {
            return Ok(()); // Since nothing is affected, returning a unit type is fine
        }

        let mut active_path = self.dir_path.join(format!("{}.log", self.current_file_id));

        if should_rotate(&active_path) {
            self.current_file_id += 1;
            active_path = self.dir_path.join(format!("{}.log", self.current_file_id));
        }

        let record = Record {
            record_type: RecordType::Delete,
            timestamp: SystemTime::now(),
            key,
            value: &[0u8; 0], // &[]
        };

        let (size, ..) = append(record, &active_path)?;

        self.memory_store.remove(key);

        self.compaction_size += size;

        Ok(())
    }

    /// Reconstructs the in-memory index by scanning all log files in the directory.
    ///
    /// Updates the current file ID and compaction size as needed.
    fn recovery(&mut self) -> Result<(), KvError> {
        if !self.dir_path.is_dir() {
            return Err(KvError::InvalidDir);
        }

        for log in fs::read_dir(&self.dir_path)? {
            let log_path = log?.path();

            let mut file = File::open(&log_path)?;
            let mut offset = 0u64;
            let file_len = file.metadata()?.len();

            while offset < file_len {
                // read Header
                let mut header = [0u8; HEADER_SIZE];
                file.read_exact(&mut header)?;

                let key_len = u32::from_le_bytes(
                    header[TYPE_SIZE + TIMESTAMP_SIZE..TYPE_SIZE + TIMESTAMP_SIZE + LEN_SIZE]
                        .try_into()
                        .expect("Key size should be 4bytes"),
                ) as usize;

                let value_len = u32::from_le_bytes(
                    header[TYPE_SIZE + TIMESTAMP_SIZE + LEN_SIZE
                        ..TYPE_SIZE + TIMESTAMP_SIZE + LEN_SIZE + LEN_SIZE]
                        .try_into()
                        .expect("Value size should be 4bytes"),
                ) as usize;

                // read Key to update the HashMap
                let mut key = vec![0u8; key_len];
                file.read_exact(&mut key)?;

                // total size of this specific record on disk
                let total_size = HEADER_SIZE + key_len + value_len;

                if header[0] == RecordType::Put as u8 {
                    // TODO: Clone here is expensive
                    self.memory_store
                        .insert(key, (log_path.clone(), offset, total_size));
                } else {
                    self.compaction_size += total_size;
                    self.memory_store.remove(&key);
                }

                offset += total_size as u64;
                file.seek(SeekFrom::Start(offset))?;
            }

            if should_rotate(&log_path) {
                self.current_file_id += 1;
            }
        }

        Ok(())
    }

    /// Compacts log files by rewriting only the latest key-value pairs to a new log file.
    ///
    /// Removes obsolete log files and resets the compaction size.
    fn compaction(&mut self) -> Result<(), KvError> {
        let compact_path = self.dir_path.join("compacted.log");
        let new_file = File::create(&compact_path)?;
        let active_path = self.dir_path.join(format!("{}.log", self.current_file_id));

        for (key, (file, old_offset, ..)) in self.memory_store.iter_mut() {
            if *file == active_path {
                continue;
            }

            let (value, timestamp) = match read(*old_offset, &file)? {
                Some(val) => val,
                None => continue,
            };

            let converted_time = Duration::from_secs(timestamp as u64);

            let record = Record {
                record_type: RecordType::Put,
                timestamp: UNIX_EPOCH + converted_time,
                key,
                value: &value,
            };

            // TODO: clone is expensive
            let (_, offset) = append(record, compact_path.clone())?;

            // Check if current compact file size is more than the MAX_LOG_SIZE //

            // ACTIVE_LOG file will never be `0` because for this function to run the size of un-compacted data should be above the threshold
            *file = self.dir_path.join(format!("{}.log", 0)); // set new file to the 0th index log
            *old_offset = offset; // new_offset
        }

        new_file.sync_all()?;

        // Delete all old .log files except the active one
        for file in fs::read_dir(&self.dir_path)? {
            let path = file?.path();

            if path == active_path || path == compact_path {
                continue;
            }

            fs::remove_file(path).expect("Should delete file");
        }

        // Have a structured way of storing compacted data so it can renamed accordingly: compacted.0.log -> 0.log
        // When the max size cap is reached for a log file, it should be rotated

        // Rename compacted.log to 0.log
        fs::rename(compact_path, self.dir_path.join(format!("{}.log", 0)))
            .expect("Should rename successfully");

        self.compaction_size = 0;

        Ok(())
    }
}
