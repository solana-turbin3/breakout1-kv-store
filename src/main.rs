use breakout1_kv_store::Engine;

fn main() -> std::io::Result<()> {
    let db = Engine::load("data.db")?; // No need to make it mutable any longer

    db.set(b"hello".to_vec(), b"world".to_vec())?;
    // db.del(b"hello".to_vec())?;

    match db.get(b"hello")? {
        Some(val) => println!("hello = {}", String::from_utf8_lossy(&val)),
        None => println!("not found"),
    }

    Ok(())
}
