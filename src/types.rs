use wincode::{SchemaRead, SchemaWrite};

#[derive(SchemaWrite, SchemaRead, Debug)]
pub struct DataFileEntry {
    pub tstamp: i64,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct LogIndex {
    pub pos: u64,
    pub len: u64,
}
