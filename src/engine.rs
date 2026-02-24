use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::constants::{
    DEFAULT_COMPACT_THRESHOLD, FILE_HEADER_MAGIC, FILE_HEADER_SIZE, LEN_PREFIX_SIZE,
};
use crate::types::{DataFileEntry, LogIndex};

pub struct Engine {
    path: PathBuf,
    file: Mutex<File>,
    index: RwLock<HashMap<Vec<u8>, LogIndex>>,
    file_size: Mutex<u64>,
    compact_threshold: Mutex<u64>,
    reader_pool: Mutex<Vec<File>>,
}

impl Engine {
    pub fn load(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let compact_threshold = Self::ensure_header(&path, DEFAULT_COMPACT_THRESHOLD)?;
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        let mut readers = Vec::new();
        for _ in 0..4 {
            if let Ok(r) = OpenOptions::new().read(true).open(&path) {
                readers.push(r);
            }
        }

        let engine = Engine {
            path,
            file: Mutex::new(file),
            index: RwLock::new(HashMap::new()),
            file_size: Mutex::new(0),
            compact_threshold: Mutex::new(compact_threshold),
            reader_pool: Mutex::new(readers),
        };

        engine.rebuild_index()?;

        Ok(engine)
    }

    fn ensure_header(path: &Path, compact_threshold: u64) -> io::Result<u64> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        let file_len = file.metadata()?.len();
        if file_len == 0 {
            Self::write_header(&mut file, compact_threshold)?;
            return Ok(compact_threshold);
        }

        if file_len < FILE_HEADER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid data.db: missing header",
            ));
        }

        file.seek(SeekFrom::Start(0))?;
        let mut magic = [0u8; FILE_HEADER_MAGIC.len()];
        file.read_exact(&mut magic)?;
        if magic != FILE_HEADER_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid data.db: unsupported format (missing KVS1 header)",
            ));
        }

        let mut threshold_buf = [0u8; 8];
        file.read_exact(&mut threshold_buf)?;
        Ok(u64::from_le_bytes(threshold_buf))
    }

    fn write_header(file: &mut File, compact_threshold: u64) -> io::Result<()> {
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&FILE_HEADER_MAGIC)?;
        file.write_all(&compact_threshold.to_le_bytes())?;
        file.flush()?;
        Ok(())
    }

    fn persist_threshold(&self, compact_threshold: u64) -> io::Result<()> {
        let mut file = OpenOptions::new().read(true).write(true).open(&self.path)?;
        Self::write_header(&mut file, compact_threshold)
    }

    fn rebuild_index(&self) -> io::Result<()> {
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(FILE_HEADER_SIZE))?;
        let mut rebuilt_index: HashMap<Vec<u8>, LogIndex> = HashMap::new();

        loop {
            let mut len_buf = [0u8; LEN_PREFIX_SIZE as usize];
            match file.read_exact(&mut len_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }

            let entry_len = u64::from_le_bytes(len_buf);
            let data_pos = file.stream_position()?;

            let mut data = vec![0u8; entry_len as usize];
            match file.read_exact(&mut data) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }

            let entry: DataFileEntry = wincode::deserialize(&data)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

            match entry.value {
                Some(_) => {
                    rebuilt_index.insert(
                        entry.key,
                        LogIndex {
                            pos: data_pos,
                            len: entry_len,
                        },
                    );
                }
                None => {
                    rebuilt_index.remove(&entry.key);
                }
            }
        }

        *self.index.write().unwrap() = rebuilt_index;
        *self.file_size.lock().unwrap() = file.stream_position()?;

        Ok(())
    }

    pub fn set(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        let tstamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        let entry = DataFileEntry {
            tstamp,
            key: key.to_vec(),
            value: Some(value.to_vec()),
        };

        let data = wincode::serialize(&entry).map_err(|e| io::Error::other(e.to_string()))?;

        let entry_len = data.len() as u64;

        let mut file = self.file.lock().unwrap();
        file.write_all(&entry_len.to_le_bytes())?;

        let data_pos = file.stream_position()?;
        file.write_all(&data)?;

        let new_file_size = *self.file_size.lock().unwrap() + LEN_PREFIX_SIZE + entry_len;
        *self.file_size.lock().unwrap() = new_file_size;

        self.index.write().unwrap().insert(
            key.to_vec(),
            LogIndex {
                pos: data_pos,
                len: entry_len,
            },
        );

        let current_threshold = *self.compact_threshold.lock().unwrap();
        let should_compact = new_file_size >= current_threshold;
        drop(file);

        if should_compact {
            self.compact()?;
        }

        Ok(())
    }

    pub fn del(&self, key: &[u8]) -> io::Result<()> {
        let tstamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        let entry = DataFileEntry {
            tstamp,
            key: key.to_vec(),
            value: None,
        };

        let data = wincode::serialize(&entry).map_err(|e| io::Error::other(e.to_string()))?;

        let entry_len = data.len() as u64;

        let mut file = self.file.lock().unwrap();
        file.write_all(&entry_len.to_le_bytes())?;

        file.write_all(&data)?;

        *self.file_size.lock().unwrap() += LEN_PREFIX_SIZE + entry_len;
        self.index.write().unwrap().remove(key);

        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let index = self.index.read().unwrap();

        let log_index = match index.get(key) {
            Some(idx) => idx.clone(),
            None => return Ok(None),
        };

        let mut reader = {
            let mut pool = self.reader_pool.lock().unwrap();
            match pool.pop() {
                Some(r) => r,
                None => OpenOptions::new().read(true).open(&self.path)?,
            }
        };

        reader.seek(SeekFrom::Start(log_index.pos))?;

        let mut data = vec![0u8; log_index.len as usize];
        reader.read_exact(&mut data)?;

        {
            let mut pool = self.reader_pool.lock().unwrap();
            if pool.len() < 8 {
                pool.push(reader);
            }
        }

        drop(index);

        let entry: DataFileEntry = wincode::deserialize(&data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

        Ok(entry.value)
    }

    pub fn compact(&self) -> io::Result<()> {
        let mut file = self.file.lock().unwrap();
        let old_file_size = *self.file_size.lock().unwrap();
        let compact_threshold = *self.compact_threshold.lock().unwrap();

        let tmp_path = self.path.with_extension("tmp");

        let mut tmp_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)?;
        Self::write_header(&mut tmp_file, compact_threshold)?;
        tmp_file.seek(SeekFrom::Start(FILE_HEADER_SIZE))?;

        let entries: Vec<(Vec<u8>, LogIndex)> = self
            .index
            .read()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        let mut new_index: HashMap<Vec<u8>, LogIndex> = HashMap::new();
        let mut new_file_size: u64 = FILE_HEADER_SIZE;

        for (key, log_index) in entries {
            file.seek(SeekFrom::Start(log_index.pos))?;
            let mut data = vec![0u8; log_index.len as usize];
            file.read_exact(&mut data)?;

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

        self.reader_pool.lock().unwrap().clear();

        let mut index = self.index.write().unwrap();

        std::fs::rename(&tmp_path, &self.path)?;
        *file = OpenOptions::new().read(true).write(true).open(&self.path)?;
        *index = new_index;
        *self.file_size.lock().unwrap() = new_file_size;

        let mut pool = self.reader_pool.lock().unwrap();
        for _ in 0..4 {
            if let Ok(r) = OpenOptions::new().read(true).open(&self.path) {
                pool.push(r);
            }
        }

        if new_file_size * 100 > old_file_size * 75 {
            let mut threshold = self.compact_threshold.lock().unwrap();
            *threshold = threshold.saturating_mul(2);
            let updated_threshold = *threshold;
            drop(threshold);
            self.persist_threshold(updated_threshold)?;
        }

        Ok(())
    }
}
