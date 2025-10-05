use redb::{Database, ReadableDatabase, TableDefinition};
use tempfile::NamedTempFile;

static TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("data");
pub struct Redb {
    _f: NamedTempFile,
    db: Database,
}

impl Redb {
    pub fn new() -> Self {
        let f = NamedTempFile::new().unwrap();
        let db = Database::create(f.path()).unwrap();
        Redb { _f: f, db }
    }
}

impl crate::database::Database for Redb {
    fn init(&self) -> anyhow::Result<()> {
        let tx = self.db.begin_write()?;
        {
            let _ = tx.open_table(TABLE)?;
        }
        tx.commit()?;
        Ok(())
    }

    fn get(&self, key: &[u8]) -> anyhow::Result<()> {
        let tx = self.db.begin_read()?;
        let t = tx.open_table(TABLE)?;
        t.get(key)?;
        Ok(())
    }

    fn set(&self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        let tx = self.db.begin_write()?;
        {
            let mut t = tx.open_table(TABLE)?;
            t.insert(key, value)?;
        }
        tx.commit()?;
        Ok(())
    }
}