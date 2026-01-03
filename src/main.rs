use kv_store::{error::KvError, store::KvStore};

fn main() -> Result<(), KvError> {
    let mut db = KvStore::open("tmp")?;

    db.put(b"name", b"Arnold")?;

    db.put(b"name", b"Emmanuel")?;
    db.put(b"age", b"21")?;

    if let Some(v) = db.get(b"age")? {
        println!("age: {:?}", String::from_utf8_lossy(&v));
    }

    let value = db.get(b"name")?;

    println!("name: {:?}", value);

    db.delete(b"name")?;

    let value = db.get(b"name")?;

    println!("name: {:?}", value);

    Ok(())
}
