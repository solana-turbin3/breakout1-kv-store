use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::constants::{DEFAULT_COMPACT_THRESHOLD, LEN_PREFIX_SIZE};
use crate::types::{DataFileEntry, LogIndex};

pub struct Engine {
    path: PathBuf,
    file: File,
    index: HashMap<Vec<u8>, LogIndex>,
    file_size: u64,
    compact_threshold: u64,
}

impl Engine {
    pub fn load(path: impl AsRef<Path>) -> io::Result<Self> {
        Self::load_with_threshold(path, DEFAULT_COMPACT_THRESHOLD)
    }

    pub fn load_with_threshold(path: impl AsRef<Path>, compact_threshold: u64) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        let mut engine = Engine {
            path,
            file,
            index: HashMap::new(),
            file_size: 0,
            compact_threshold,
        };

        engine.rebuild_index()?;

        Ok(engine)
    }

    fn rebuild_index(&mut self) -> io::Result<()> {
        self.file.seek(SeekFrom::Start(0))?;

        loop {
            let mut len_buf = [0u8; LEN_PREFIX_SIZE as usize];
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

        self.file_size = self.file.stream_position()?;

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

        let data = wincode::serialize(&entry).map_err(|e| io::Error::other(e.to_string()))?;

        let entry_len = data.len() as u64;

        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&entry_len.to_le_bytes())?;

        let data_pos = self.file.stream_position()?;
        self.file.write_all(&data)?;
        self.file.flush()?;

        self.file_size += LEN_PREFIX_SIZE + entry_len;

        self.index.insert(
            key,
            LogIndex {
                pos: data_pos,
                len: entry_len,
            },
        );

        if self.file_size >= self.compact_threshold {
            self.compact()?;
        }

        Ok(())
    }

    pub fn del(&mut self, key: Vec<u8>) -> io::Result<()> {
        let tstamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        let entry = DataFileEntry {
            tstamp,
            key: key.clone(),
            value: None,
        };

        let data = wincode::serialize(&entry).map_err(|e| io::Error::other(e.to_string()))?;

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

    pub fn compact(&mut self) -> io::Result<()> {
        let tmp_path = self.path.with_extension("tmp");

        let mut tmp_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)?;

        let entries: Vec<(Vec<u8>, LogIndex)> = self
            .index
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        let mut new_index: HashMap<Vec<u8>, LogIndex> = HashMap::new();
        let mut new_file_size: u64 = 0;

        for (key, log_index) in entries {
            self.file.seek(SeekFrom::Start(log_index.pos))?;
            let mut data = vec![0u8; log_index.len as usize];
            self.file.read_exact(&mut data)?;

            let entry_len = data.len() as u64;
            tmp_file.write_all(&entry_len.to_le_bytes())?;
            let new_pos = tmp_file.stream_position()?;
            tmp_file.write_all(&data)?;

            new_file_size += LEN_PREFIX_SIZE + entry_len;
            new_index.insert(
                key,
                LogIndex {
                    pos: new_pos,
                    len: entry_len,
                },
            );
        }

        tmp_file.flush()?;
        drop(tmp_file);

        std::fs::rename(&tmp_path, &self.path)?;

        self.file = OpenOptions::new().read(true).write(true).open(&self.path)?;
        self.index = new_index;
        self.file_size = new_file_size;

        Ok(())
    }
}
