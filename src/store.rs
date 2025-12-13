use std::{
    collections::HashMap,
    fs::File,
    io::{IoSlice, Read, Seek, SeekFrom, Write},
    path::PathBuf,
};

use crate::{
    error::KvError,
    record::{Record, RecordType},
};

fn append(record: Record, file_path: &str) -> Result<(usize, u64), KvError> {
    let key = &record.key;
    let value = &record.value;
    let record_type = &[record.record_type as u8];
    let key_len = key.len() as u32;
    let value_len = value.len() as u32;

    let mut file = File::options().create(true).append(true).open(file_path)?;

    let offset = file.metadata()?.len();

    let key_len_bytes = key_len.to_le_bytes();
    let value_len_bytes = value_len.to_le_bytes();

    // buffer contents: record_type 1byte | key_size 4bytes | value_size 4bytes | key n-bytes | value n-bytes
    let bufs = [
        IoSlice::new(record_type),
        IoSlice::new(&key_len_bytes),
        IoSlice::new(&value_len_bytes), // 0 for Delete
        IoSlice::new(key),
        IoSlice::new(value), // Would be empty for Delete
    ];

    let size = file.write_vectored(&bufs)?;

    file.sync_all()?;

    Ok((size, offset))
}

fn read((size, offset): (&usize, &u64), file_path: &str) -> Result<Option<Vec<u8>>, KvError> {
    let mut file = File::open(file_path)?;

    file.seek(SeekFrom::Start(*offset))?;

    // Implement zero-cost
    let mut record = vec![0u8; *size];

    file.read_exact(&mut record)?;

    if record[0] != RecordType::Put as u8 {
        return Ok(None);
    }

    let key_size =
        u32::from_le_bytes(record[1..5].try_into().expect("Key size should be 4bytes")) as usize;
    let value_size = u32::from_le_bytes(
        record[5..9]
            .try_into()
            .expect("Value size should be 4bytes"),
    ) as usize;

    let key_start = 9;
    let value_start = key_start + key_size;
    let value_end = value_start + value_size;

    let value = &record[value_start..value_end];

    Ok(Some(value.to_vec()))
}

#[derive(Debug)]
pub struct KvStore {
    // current_offset: u64
    memory_store: HashMap<Vec<u8>, (String, u64, usize)>, //file_id, offset, size
    dir_path: PathBuf,
}

impl KvStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, KvError> {
        let dir_path = path.into();

        if !dir_path.exists() {
            std::fs::create_dir(&dir_path)?;
        }

        // rebuild in-memory store

        Ok(KvStore {
            memory_store: HashMap::new(),
            dir_path,
        })
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), KvError> {
        // auto generate file_id
        let file_id = "seg-1";

        let path = format!("{}/{}", &self.dir_path.display(), file_id);
        let record = Record {
            record_type: RecordType::Put,
            key,
            value,
        };

        let (size, offset) = append(record, &path)?;

        self.memory_store
            .insert(key.to_vec(), (file_id.to_owned(), offset, size));

        Ok(())
    }

    // return type should be refactored
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
        let (file_id, offset, size) = self.memory_store.get(key).ok_or(KvError::NotFound)?;

        let path = format!("{}/{}", &self.dir_path.display(), file_id);

        read((size, offset), &path)
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<(), KvError> {
        if !self.memory_store.contains_key(key) {
            return Ok(());
        }

        let file_id = "seg-1";

        let path = format!("{}/{}", &self.dir_path.display(), file_id);

        let record = Record {
            record_type: RecordType::Delete,
            key,
            value: &[0u8; 0],
        };

        append(record, &path)?;

        self.memory_store.remove(key);

        Ok(())
    }
}
