use breakout1_kv_store::Engine;
use breakout1_kv_store::constants::DEFAULT_COMPACT_THRESHOLD;
use std::fs;
use tempfile::NamedTempFile;

fn temp_engine() -> (Engine, NamedTempFile) {
    let file = NamedTempFile::new().unwrap();
    let engine = Engine::load(file.path()).unwrap();
    (engine, file)
}

#[test]
fn test_set_and_get() {
    let (mut engine, _f) = temp_engine();
    engine.set(b"name".to_vec(), b"alice".to_vec()).unwrap();
    assert_eq!(engine.get(b"name").unwrap(), Some(b"alice".to_vec()));
}

#[test]
fn test_get_nonexistent_key_returns_none() {
    let (mut engine, _f) = temp_engine();
    assert_eq!(engine.get(b"ghost").unwrap(), None);
}

#[test]
fn test_delete_key() {
    let (mut engine, _f) = temp_engine();
    engine.set(b"key".to_vec(), b"value".to_vec()).unwrap();
    engine.del(b"key".to_vec()).unwrap();
    assert_eq!(engine.get(b"key").unwrap(), None);
}

#[test]
fn test_delete_nonexistent_key_is_ok() {
    let (mut engine, _f) = temp_engine();
    engine.del(b"nothing".to_vec()).unwrap();
}

#[test]
fn test_overwrite_key() {
    let (mut engine, _f) = temp_engine();
    engine.set(b"k".to_vec(), b"v1".to_vec()).unwrap();
    engine.set(b"k".to_vec(), b"v2".to_vec()).unwrap();
    assert_eq!(engine.get(b"k").unwrap(), Some(b"v2".to_vec()));
}

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

#[test]
fn test_index_rebuilt_after_reload() {
    let file = NamedTempFile::new().unwrap();
    let path = file.path().to_owned();

    {
        let mut engine = Engine::load(&path).unwrap();
        engine.set(b"foo".to_vec(), b"bar".to_vec()).unwrap();
        engine.set(b"hello".to_vec(), b"world".to_vec()).unwrap();
    }

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

#[test]
fn test_empty_value() {
    let (mut engine, _f) = temp_engine();
    engine.set(b"empty".to_vec(), b"".to_vec()).unwrap();
    assert_eq!(engine.get(b"empty").unwrap(), Some(b"".to_vec()));
}

#[test]
fn test_large_value() {
    let (mut engine, _f) = temp_engine();
    let large_val = vec![0xABu8; DEFAULT_COMPACT_THRESHOLD as usize];
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
        engine
            .set(b"counter".to_vec(), i.to_le_bytes().to_vec())
            .unwrap();
    }
    assert_eq!(
        engine.get(b"counter").unwrap(),
        Some(99u32.to_le_bytes().to_vec())
    );
}

#[test]
fn test_compact_live_keys_still_readable() {
    let (mut engine, _f) = temp_engine();
    engine.set(b"a".to_vec(), b"1".to_vec()).unwrap();
    engine.set(b"b".to_vec(), b"2".to_vec()).unwrap();
    engine.compact().unwrap();
    assert_eq!(engine.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(engine.get(b"b").unwrap(), Some(b"2".to_vec()));
}

#[test]
fn test_compact_removes_stale_entries() {
    let file = NamedTempFile::new().unwrap();
    let path = file.path().to_owned();
    let mut engine = Engine::load(&path).unwrap();

    for i in 0..50u32 {
        engine.set(b"k".to_vec(), i.to_le_bytes().to_vec()).unwrap();
    }

    let size_before = fs::metadata(&path).unwrap().len();
    engine.compact().unwrap();
    let size_after = fs::metadata(&path).unwrap().len();

    assert!(size_after < size_before);
    assert_eq!(
        engine.get(b"k").unwrap(),
        Some(49u32.to_le_bytes().to_vec())
    );
}

#[test]
fn test_compact_drops_deleted_keys() {
    let (mut engine, _f) = temp_engine();
    engine.set(b"gone".to_vec(), b"bye".to_vec()).unwrap();
    engine.del(b"gone".to_vec()).unwrap();
    engine.compact().unwrap();
    assert_eq!(engine.get(b"gone").unwrap(), None);
}

#[test]
fn test_compact_empty_engine() {
    let (mut engine, _f) = temp_engine();
    engine.compact().unwrap();
    assert_eq!(engine.get(b"anything").unwrap(), None);
}

#[test]
fn test_auto_compact_triggered_by_threshold() {
    let file = NamedTempFile::new().unwrap();
    let path = file.path().to_owned();
    let threshold = 512;
    let mut engine = Engine::load_with_threshold(&path, threshold).unwrap();

    for i in 0..200u32 {
        engine
            .set(b"key".to_vec(), i.to_le_bytes().to_vec())
            .unwrap();
    }

    let size = fs::metadata(&path).unwrap().len();
    assert!(size < threshold * 10);
    assert_eq!(
        engine.get(b"key").unwrap(),
        Some(199u32.to_le_bytes().to_vec())
    );
}
