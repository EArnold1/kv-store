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

        Ok(Self {
            store,
            compaction_thread: None,
        })
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), KvError> {
        let mut store = self
            .store
            .lock()
            .expect("Store lock should not be poisoned");

        if store.check_compaction() {
            let tx = store.sender.clone();
            thread::spawn(move || {
                tx.send(()).expect("Receiver should not be dropped")
                // KvStore::check_compaction(store_clone);
            });
        }

        store.put(key, value)
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
