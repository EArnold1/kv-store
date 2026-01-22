use std::{
    sync::{Arc, Mutex},
    thread::{self, JoinHandle},
};

use crate::{
    error::KvError,
    store::{DbTraits, KvStore},
};

pub struct KvDB {
    store: Arc<Mutex<KvStore>>,
    compaction_thread: Option<JoinHandle<()>>,
}

impl Drop for KvDB {
    fn drop(&mut self) {
        self.store
            .lock()
            .expect("Store lock should not be poisoned")
            .shutdown();

        if let Some(handle) = self.compaction_thread.take() {
            handle.join().expect("Compaction thread should not panic");
        }
    }
}

impl DbTraits for KvDB {
    fn open(path: impl Into<std::path::PathBuf>) -> Result<Self, KvError> {
        let store = Arc::new(Mutex::new(KvStore::open(path)?));
        let store_clone = Arc::clone(&store);
        let thread_handle = thread::spawn(|| KvStore::compaction_task(store_clone));

        Ok(Self {
            store,
            compaction_thread: Some(thread_handle),
        })
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), KvError> {
        self.store
            .lock()
            .expect("Store lock should not be poisoned")
            .put(key, value)
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
        self.store
            .lock()
            .expect("Store lock should not be poisoned")
            .get(key)
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), KvError> {
        self.store
            .lock()
            .expect("Store lock should not be poisoned")
            .delete(key)
    }
}
