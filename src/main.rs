use kv_db::{error::KvError, store::KvStore};

fn main() -> Result<(), KvError> {
    let mut db = KvStore::open("tmp")?;

    db.put(
        b"message",
        b"This pattern is seen more frequently in Rust (and for simpler objects) than in many other languages because Rust lacks overloading and default values for function parameters. Since you can only have a single method with a given name, having multiple constructors is less nice in Rust than in C++, Java, or others.",
    )?;

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
