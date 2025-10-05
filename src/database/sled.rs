use crate::database::Database;
use tempfile::TempDir;

pub struct Sled {
    db: sled::Db,
    _f: TempDir,
}

impl Sled {
    pub fn new() -> Self {
        let f = TempDir::new().unwrap();
        let db = sled::open(f.path()).unwrap();
        Sled { db, _f: f }
    }
}
impl Database for Sled {
    fn init(&self) -> anyhow::Result<()> {
        Ok(())
    }

    fn get(&self, key: &[u8]) -> anyhow::Result<()> {
        self.db.get(key)?;
        Ok(())
    }

    fn set(&self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        self.db.insert(key, value)?;
        Ok(())
    }
}
