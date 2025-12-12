use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
    thread,
    time::Duration,
};

#[derive(Debug)]
pub enum RecordType {
    Put = 0,
    Delete = 1,
}

struct Record {
    record_type: RecordType,
    key: Vec<u8>,
    value: Vec<u8>,
}

// kv store
#[derive(Default, Debug)]
struct KvStore {
    // current_offset: u64
    memory_store: HashMap<String, (String, u64, usize)>, //file_id, position, size
}

impl KvStore {
    fn put(&mut self, key: String, value: String) {
        // record_type - 1byte | key_size 4bytes | value_size 4bytes | key n-bytes | value n-bytes
        let mut buf = Vec::with_capacity(1 + 4 + 4 + key.len() + value.len());

        // TODO: implement zero-copy
        buf.push(RecordType::Put as u8); // using push because it is a single byte

        // convert key & value length to 4 bytes
        buf.extend(&(key.len() as u32).to_le_bytes());
        buf.extend(&(value.len() as u32).to_le_bytes());
        buf.extend(key.as_bytes());
        buf.extend(value.as_bytes());

        let path = Path::new("tmp/active");

        let mut file = File::options()
            .create(true)
            .append(true)
            .open(path)
            .unwrap();

        let offset = file.metadata().unwrap().len();

        file.write_all(&buf).unwrap();

        file.sync_all().unwrap();

        let size = buf.len();

        self.memory_store
            .insert(key.clone(), ("tmp/active".to_owned(), offset, size));
    }

    fn get(&self, key: String) -> Option<String> {
        match self.memory_store.get(&key) {
            Some((p, offset, size)) => {
                let mut file = File::open(p).unwrap();

                file.seek(SeekFrom::Start(*offset)).unwrap();

                let mut record = vec![0u8; *size];

                file.read_exact(&mut record).unwrap();

                let key_size = u32::from_le_bytes(record[1..5].try_into().unwrap()) as usize;
                let value_size = u32::from_le_bytes(record[5..9].try_into().unwrap()) as usize;

                let key_start = 9;
                let value_start = key_start + key_size;
                let value_end = value_start + value_size;

                // let key = &record[key_start..value_start];
                let value = &record[value_start..value_end];

                Some(String::from_utf8_lossy(value).to_string())
            }
            None => None,
        }
    }
}

fn main() {
    let mut db = KvStore::default();

    db.put(String::from("name"), String::from("Arnold"));

    db.put(String::from("name"), String::from("Emmanuel"));
    db.put(String::from("age"), String::from("21"));
    thread::sleep(Duration::from_secs(1));

    let value = db.get(String::from("age"));

    println!("age: {:?}", value);

    let value = db.get(String::from("name"));

    println!("name: {:?}", value);
}
