use anyhow::Result;
use rusqlite::{params, Connection};
use std::{path::Path, sync::Mutex};

pub struct Store {
    connection: Mutex<Connection>,
}

impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)?;
        }
        let connection = Connection::open(path)?;
        connection.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA foreign_keys=ON;
             CREATE TABLE IF NOT EXISTS conversation (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               role TEXT NOT NULL CHECK(role IN ('user','assistant','system')),
               content TEXT NOT NULL,
               created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
             );
             CREATE INDEX IF NOT EXISTS conversation_created_idx ON conversation(created_at);"
        )?;
        Ok(Self { connection: Mutex::new(connection) })
    }

    pub fn append_message(&self, role: &str, content: &str) -> Result<()> {
        let connection = self.connection.lock().map_err(|_| anyhow::anyhow!("database lock poisoned"))?;
        connection.execute("INSERT INTO conversation(role, content) VALUES (?1, ?2)", params![role, content])?;
        Ok(())
    }

    pub fn recent_messages(&self, limit: usize) -> Result<Vec<(String, String)>> {
        let connection = self.connection.lock().map_err(|_| anyhow::anyhow!("database lock poisoned"))?;
        let mut statement = connection.prepare(
            "SELECT role, content FROM conversation ORDER BY id DESC LIMIT ?1"
        )?;
        let rows = statement.query_map([limit.min(100) as i64], |row| Ok((row.get(0)?, row.get(1)?)))?;
        let mut messages = rows.collect::<rusqlite::Result<Vec<_>>>()?;
        messages.reverse();
        Ok(messages)
    }
}
