use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::types::{DataFileEntry, LogIndex};

pub struct Engine {
    file: File,
    index: HashMap<Vec<u8>, LogIndex>,
}

impl Engine {
    pub fn load(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let mut engine = Engine {
            file,
            index: HashMap::new(),
        };

        engine.rebuild_index()?;

        Ok(engine)
    }

    fn rebuild_index(&mut self) -> io::Result<()> {
        self.file.seek(SeekFrom::Start(0))?;

        loop {
            let mut len_buf = [0u8; 8];
            match self.file.read_exact(&mut len_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }

            let entry_len = u64::from_le_bytes(len_buf);
            let data_pos = self.file.stream_position()?;

            let mut data = vec![0u8; entry_len as usize];
            self.file.read_exact(&mut data)?;

            let entry: DataFileEntry = wincode::deserialize(&data)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

            match entry.value {
                Some(_) => {
                    self.index.insert(
                        entry.key,
                        LogIndex {
                            pos: data_pos,
                            len: entry_len,
                        },
                    );
                }
                None => {
                    self.index.remove(&entry.key);
                }
            }
        }

        Ok(())
    }

    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> io::Result<()> {
        let tstamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        let entry = DataFileEntry {
            tstamp,
            key: key.clone(),
            value: Some(value),
        };

        let data = wincode::serialize(&entry)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        let entry_len = data.len() as u64;

        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&entry_len.to_le_bytes())?;

        let data_pos = self.file.stream_position()?;
        self.file.write_all(&data)?;
        self.file.flush()?;

        self.index.insert(
            key,
            LogIndex {
                pos: data_pos,
                len: entry_len,
            },
        );

        Ok(())
    }

    pub fn del(&mut self, key: Vec<u8>) -> io::Result<()>{
        let tstamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        let entry = DataFileEntry {
            tstamp,
            key: key.clone(),
            value: None,
        };

        let data = wincode::serialize(&entry)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        let entry_len = data.len() as u64;

        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&entry_len.to_le_bytes())?;


        self.file.write_all(&data)?;
        self.file.flush()?;

        self.index.remove(&key);

        Ok(())
    }

    pub fn get(&mut self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let log_index = match self.index.get(key) {
            Some(idx) => idx.clone(),
            None => return Ok(None),
        };

        self.file.seek(SeekFrom::Start(log_index.pos))?;

        let mut data = vec![0u8; log_index.len as usize];
        self.file.read_exact(&mut data)?;

        let entry: DataFileEntry = wincode::deserialize(&data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

        Ok(entry.value)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    // Helper to create a temp engine
    fn temp_engine() -> (Engine, tempfile::NamedTempFile) {
        let file = NamedTempFile::new().unwrap();
        let engine = Engine::load(file.path()).unwrap();
        (engine, file)
    }

    // --- Basic CRUD ---

    #[test]
    fn test_set_and_get() {
        let (mut engine, _f) = temp_engine();
        engine.set(b"name".to_vec(), b"alice".to_vec()).unwrap();
        let val = engine.get(b"name").unwrap();
        assert_eq!(val, Some(b"alice".to_vec()));
    }

    #[test]
    fn test_get_nonexistent_key_returns_none() {
        let (mut engine, _f) = temp_engine();
        let val = engine.get(b"ghost").unwrap();
        assert_eq!(val, None);
    }

    #[test]
    fn test_delete_key() {
        let (mut engine, _f) = temp_engine();
        engine.set(b"key".to_vec(), b"value".to_vec()).unwrap();
        engine.del(b"key".to_vec()).unwrap();
        let val = engine.get(b"key").unwrap();
        assert_eq!(val, None);
    }

    #[test]
    fn test_delete_nonexistent_key_is_ok() {
        let (mut engine, _f) = temp_engine();
        // should not panic or error
        engine.del(b"nothing".to_vec()).unwrap();
    }

    #[test]
    fn test_overwrite_key() {
        let (mut engine, _f) = temp_engine();
        engine.set(b"k".to_vec(), b"v1".to_vec()).unwrap();
        engine.set(b"k".to_vec(), b"v2".to_vec()).unwrap();
        let val = engine.get(b"k").unwrap();
        assert_eq!(val, Some(b"v2".to_vec()));
    }

    // --- Multiple keys ---

    #[test]
    fn test_multiple_keys() {
        let (mut engine, _f) = temp_engine();
        engine.set(b"a".to_vec(), b"1".to_vec()).unwrap();
        engine.set(b"b".to_vec(), b"2".to_vec()).unwrap();
        engine.set(b"c".to_vec(), b"3".to_vec()).unwrap();

        assert_eq!(engine.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(engine.get(b"b").unwrap(), Some(b"2".to_vec()));
        assert_eq!(engine.get(b"c").unwrap(), Some(b"3".to_vec()));
    }

    // --- Persistence / index rebuild ---

    #[test]
    fn test_index_rebuilt_after_reload() {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_owned();

        {
            let mut engine = Engine::load(&path).unwrap();
            engine.set(b"foo".to_vec(), b"bar".to_vec()).unwrap();
            engine.set(b"hello".to_vec(), b"world".to_vec()).unwrap();
        } // engine dropped, file stays

        // reload engine from same file
        let mut engine = Engine::load(&path).unwrap();
        assert_eq!(engine.get(b"foo").unwrap(), Some(b"bar".to_vec()));
        assert_eq!(engine.get(b"hello").unwrap(), Some(b"world".to_vec()));
    }

    #[test]
    fn test_delete_persists_after_reload() {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_owned();

        {
            let mut engine = Engine::load(&path).unwrap();
            engine.set(b"key".to_vec(), b"val".to_vec()).unwrap();
            engine.del(b"key".to_vec()).unwrap();
        }

        let mut engine = Engine::load(&path).unwrap();
        assert_eq!(engine.get(b"key").unwrap(), None);
    }

    #[test]
    fn test_overwrite_persists_after_reload() {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_owned();

        {
            let mut engine = Engine::load(&path).unwrap();
            engine.set(b"k".to_vec(), b"old".to_vec()).unwrap();
            engine.set(b"k".to_vec(), b"new".to_vec()).unwrap();
        }

        let mut engine = Engine::load(&path).unwrap();
        assert_eq!(engine.get(b"k").unwrap(), Some(b"new".to_vec()));
    }

    // --- Edge cases ---

    #[test]
    fn test_empty_value() {
        let (mut engine, _f) = temp_engine();
        engine.set(b"empty".to_vec(), b"".to_vec()).unwrap();
        assert_eq!(engine.get(b"empty").unwrap(), Some(b"".to_vec()));
    }

    #[test]
    fn test_large_value() {
        let (mut engine, _f) = temp_engine();
        let large_val = vec![0xABu8; 1024 * 1024]; // 1MB
        engine.set(b"big".to_vec(), large_val.clone()).unwrap();
        assert_eq!(engine.get(b"big").unwrap(), Some(large_val));
    }

    #[test]
    fn test_binary_keys_and_values() {
        let (mut engine, _f) = temp_engine();
        let key = vec![0x00, 0xFF, 0x42, 0x13];
        let val = vec![0xDE, 0xAD, 0xBE, 0xEF];
        engine.set(key.clone(), val.clone()).unwrap();
        assert_eq!(engine.get(&key).unwrap(), Some(val));
    }

    #[test]
    fn test_many_overwrites_index_stays_correct() {
        let (mut engine, _f) = temp_engine();
        for i in 0..100u32 {
            engine.set(b"counter".to_vec(), i.to_le_bytes().to_vec()).unwrap();
        }
        assert_eq!(engine.get(b"counter").unwrap(), Some(99u32.to_le_bytes().to_vec()));
    }
}