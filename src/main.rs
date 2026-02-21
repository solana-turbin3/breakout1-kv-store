mod engine;
mod types;

use engine::Engine;

fn main() -> std::io::Result<()> {
    let mut db = Engine::load("data.db")?;

    db.set(b"hello".to_vec(), b"world".to_vec())?;
    db.del(b"hello".to_vec())?;

    match db.get(b"hello")? {
        Some(val) => println!("hello = {}", String::from_utf8_lossy(&val)),
        None => println!("not found"),
    }

    Ok(())
}
