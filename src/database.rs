mod mem_btree;
mod redb;

use crate::database::redb::Redb;
use anyhow::Result;
use std::sync::Arc;
use crate::database::mem_btree::MemBTree;
use crate::DatabaseType;

pub trait Database: Send + Sync {
    fn init(&self) -> Result<()>;
    fn get(&self, key: &[u8]) -> Result<()>;
    fn set(&self, key: &[u8], value: &[u8]) -> Result<()>;
}

pub fn get_db(database: DatabaseType) -> Result<Arc<dyn Database>> {
    match database {
        DatabaseType::MemBtree => Ok(Arc::new(MemBTree::default())),
        DatabaseType::Redb => Ok(Arc::new(Redb::new())),
    }
}
