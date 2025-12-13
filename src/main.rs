use std::{
    collections::HashMap,
    fs::{self, File},
    io::{IoSlice, Read, Seek, SeekFrom, Write},
    path::Path,
};

#[derive(Debug)]
pub enum RecordType {
    Put = 0,
    Delete = 1,
}

// Record
// -Header
// -Payload

// struct Record {
//     record_type: RecordType,
//     key: Vec<u8>,
//     value: Vec<u8>,
// }

// kv store
#[derive(Debug)]
struct KvStore {
    // current_offset: u64
    memory_store: HashMap<Vec<u8>, (String, u64, usize)>, //file_id, offset, size
    dir_path: String,
}

// impl Default for KvStore {
//     fn default() -> Self {
//         Self {
//             memory_store: HashMap::new(),
//             dir_path: String::from("tmp"),
//         }
//     }
// }

impl KvStore {
    fn new(dir_path: &str) -> Self {
        if !Path::new(dir_path).exists() {
            fs::create_dir(dir_path).unwrap();
        }

        Self {
            memory_store: HashMap::new(),
            dir_path: dir_path.to_owned(),
        }
    }

    fn put(&mut self, key: &[u8], value: &[u8]) {
        let key_len = key.len() as u32;
        let value_len = value.len() as u32;

        let file_id = "active";

        let mut file = File::options()
            .create(true)
            .append(true)
            .open(format!("{}/{}", &self.dir_path, file_id)) // auto generate file_id
            .unwrap();

        let offset = file.metadata().unwrap().len();

        let key_len_bytes = key_len.to_le_bytes();
        let value_len_bytes = value_len.to_le_bytes();

        // buffer contents: record_type 1byte | key_size 4bytes | value_size 4bytes | key n-bytes | value n-bytes
        let bufs = [
            IoSlice::new(&[RecordType::Put as u8]),
            IoSlice::new(&key_len_bytes),
            IoSlice::new(&value_len_bytes),
            IoSlice::new(key),
            IoSlice::new(value),
        ];

        let size = file.write_vectored(&bufs).unwrap();

        file.sync_all().unwrap();

        self.memory_store
            .insert(key.to_vec(), (file_id.to_owned(), offset, size));
    }

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let (file_id, offset, size) = self.memory_store.get(key)?;

        let mut file = File::open(format!("{}/{}", &self.dir_path, file_id)).unwrap();

        file.seek(SeekFrom::Start(*offset)).unwrap();

        // Implement zero-cost
        let mut record = vec![0u8; *size];

        file.read_exact(&mut record).unwrap();

        if record[0] != RecordType::Put as u8 {
            return None;
        }

        let key_size = u32::from_le_bytes(record[1..5].try_into().unwrap()) as usize;
        let value_size = u32::from_le_bytes(record[5..9].try_into().unwrap()) as usize;

        let key_start = 9;
        let value_start = key_start + key_size;
        let value_end = value_start + value_size;

        let value = &record[value_start..value_end];

        Some(value.to_vec())
    }

    fn delete(&mut self, key: &[u8]) {
        // delete should append a new record(delete)
        if self.memory_store.contains_key(key) {
            let key_len = key.len() as u32;

            let file_id = "active";

            let mut file = File::options()
                .create(true)
                .append(true)
                .open(format!("{}/{}", &self.dir_path, file_id)) // auto generate file_id
                .unwrap();

            let key_len_bytes = key_len.to_le_bytes();

            // buffer contents: record_type 1byte | key_size 4bytes | key n-bytes
            let bufs = [
                IoSlice::new(&[RecordType::Delete as u8]),
                IoSlice::new(&key_len_bytes),
                IoSlice::new(&[0u8]),
                IoSlice::new(key),
            ];

            let _ = file.write_vectored(&bufs).unwrap();

            file.sync_all().unwrap();

            self.memory_store.remove(key);
        }
    }
}

fn main() {
    let mut db = KvStore::new("tmp");

    db.put(b"name", b"Arnold");

    db.put(b"name", b"Emmanuel");
    db.put(b"age", b"21");

    let value = db.get(b"age");

    println!("age: {:?}", value);

    let value = db.get(b"name");

    println!("name: {:?}", value);

    db.delete(b"name");

    let value = db.get(b"name");

    println!("name: {:?}", value);
}
