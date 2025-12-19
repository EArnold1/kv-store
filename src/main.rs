mod error;
mod helper;
mod record;
mod store;

use error::KvError;
use store::KvStore;

fn main() -> Result<(), KvError> {
    let mut db = KvStore::open("tmp")?;

    db.put(b"name", b"Arnold")?;

    db.put(b"name", b"Emmanuel")?;
    db.put(b"age", b"21")?;

    let value = db.get(b"age")?;

    println!("age: {:?}", value);

    let value = db.get(b"name")?;

    println!("name: {:?}", value);

    db.delete(b"name")?;

    let value = db.get(b"name")?;

    println!("name: {:?}", value);

    Ok(())
}
