use std::{
    collections::HashMap,
    fs::{self, File},
    io::{IoSlice, Read, Seek, SeekFrom, Write},
    path::PathBuf,
    time::SystemTime,
};

use crate::{
    error::KvError,
    helper::system_time_to_bytes,
    record::{Record, RecordType},
    wal::should_rotate,
};

const TYPE_SIZE: usize = 1; // 1 byte for RecordType
const LEN_SIZE: usize = 4; // 4 bytes for u32 lengths
const TIMESTAMP_SIZE: usize = 8; // 8 bytes timestamp
const HEADER_SIZE: usize = TYPE_SIZE + TIMESTAMP_SIZE + LEN_SIZE + LEN_SIZE;

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

// TODO: update the return type
fn read(offset: u64, file_path: impl Into<PathBuf>) -> Result<Option<Vec<u8>>, KvError> {
    let mut file = File::open(file_path.into())?;

    file.seek(SeekFrom::Start(offset))?;

    let mut header = [0u8; HEADER_SIZE];

    file.read_exact(&mut header)?;

    if header[0] != RecordType::Put as u8 {
        return Ok(None);
    }

    // timestamp record[TYPE_SIZE..TYPE_SIZE + TIMESTAMP_SIZE](1..9)

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

    Ok(Some(value.to_vec()))
}

#[derive(Debug)]
pub struct KvStore {
    memory_store: HashMap<Vec<u8>, (PathBuf, u64, usize)>, //file_id, offset, size
    dir_path: PathBuf,
    compaction_size: usize,
    current_file_id: u64,
}

impl KvStore {
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

        Ok(store)
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), KvError> {
        let mut active_path = self.dir_path.join(format!("{}.log", self.current_file_id));

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

    // return type should be refactored
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
        let (file, offset, ..) = match self.memory_store.get(key) {
            Some(v) => v,
            None => return Ok(None),
        };

        read(*offset, file)
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<(), KvError> {
        if !self.memory_store.contains_key(key) {
            return Ok(()); // Since nothing is affected, returning this a unit type is fine
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
            value: &[0u8; 0],
        };

        let (size, ..) = append(record, &active_path)?;

        self.memory_store.remove(key);

        self.compaction_size += size;

        Ok(())
    }

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
        }

        Ok(())
    }
}
