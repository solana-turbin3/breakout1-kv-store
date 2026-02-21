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
