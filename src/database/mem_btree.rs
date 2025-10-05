use crate::database::Database;
use std::collections::BTreeMap;
use std::sync::RwLock;

#[derive(Default)]
pub struct MemBTree {
    data: RwLock<BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl Database for MemBTree {
    fn init(&self) -> anyhow::Result<()> {
        Ok(())
    }

    fn get(&self, key: &[u8]) -> anyhow::Result<()> {
        self.data.read().unwrap_or_else(|e| e.into_inner()).get(key);
        Ok(())
    }

    fn set(&self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        self.data
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(Vec::from(key), Vec::from(value));
        Ok(())
    }
}
