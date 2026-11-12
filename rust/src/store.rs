use anyhow::Result;
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde::Serialize;
use std::{path::Path, sync::Mutex};

#[derive(Debug, Clone, Serialize)]
pub struct Goal {
    pub id: i64,
    pub description: String,
    pub status: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct AgentTask {
    pub id: i64,
    pub goal_id: i64,
    pub role: String,
    pub description: String,
    pub priority: i32,
    pub status: String,
    pub assigned_agent: Option<String>,
    pub lease_until: Option<String>,
}

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
             CREATE INDEX IF NOT EXISTS conversation_created_idx ON conversation(created_at);
             CREATE TABLE IF NOT EXISTS goals (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               description TEXT NOT NULL,
               status TEXT NOT NULL DEFAULT 'active',
               created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
             );
             CREATE TABLE IF NOT EXISTS agent_tasks (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               goal_id INTEGER NOT NULL REFERENCES goals(id) ON DELETE CASCADE,
               role TEXT NOT NULL,
               description TEXT NOT NULL,
               priority INTEGER NOT NULL DEFAULT 0,
               status TEXT NOT NULL DEFAULT 'pending',
               assigned_agent TEXT,
               lease_until TEXT,
               created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
               updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
             );
             CREATE INDEX IF NOT EXISTS agent_tasks_queue_idx ON agent_tasks(status, role, priority DESC, id);"
        )?;
        let has_lease_column = {
            let mut statement = connection.prepare("PRAGMA table_info(agent_tasks)")?;
            let columns = statement.query_map([], |row| row.get::<_, String>(1))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            columns.iter().any(|name| name == "lease_until")
        };
        if !has_lease_column {
            connection.execute("ALTER TABLE agent_tasks ADD COLUMN lease_until TEXT", [])?;
        }
        Ok(Self { connection: Mutex::new(connection) })
    }

    pub fn append_message(&self, role: &str, content: &str) -> Result<()> {
        let connection = self.connection.lock().map_err(|_| anyhow::anyhow!("database lock poisoned"))?;
        connection.execute("INSERT INTO conversation(role, content) VALUES (?1, ?2)", params![role, content])?;
        connection.execute(
            "DELETE FROM conversation WHERE id <= (SELECT COALESCE(MAX(id), 0) - 10000 FROM conversation)",
            [],
        )?;
        Ok(())
    }

    pub fn recent_messages(&self, limit: usize) -> Result<Vec<(String, String)>> {
        let connection = self.connection.lock().map_err(|_| anyhow::anyhow!("database lock poisoned"))?;
        let mut statement = connection.prepare("SELECT role, content FROM conversation ORDER BY id DESC LIMIT ?1")?;
        let rows = statement.query_map([limit.min(100) as i64], |row| Ok((row.get(0)?, row.get(1)?)))?;
        let mut messages = rows.collect::<rusqlite::Result<Vec<_>>>()?;
        messages.reverse();
        Ok(messages)
    }

    pub fn create_goal(&self, description: &str) -> Result<i64> {
        let connection = self.connection.lock().map_err(|_| anyhow::anyhow!("database lock poisoned"))?;
        connection.execute("INSERT INTO goals(description) VALUES (?1)", [description])?;
        Ok(connection.last_insert_rowid())
    }

    pub fn add_task(&self, goal_id: i64, role: &str, description: &str, priority: i32) -> Result<i64> {
        let connection = self.connection.lock().map_err(|_| anyhow::anyhow!("database lock poisoned"))?;
        connection.execute(
            "INSERT INTO agent_tasks(goal_id, role, description, priority) VALUES (?1, ?2, ?3, ?4)",
            params![goal_id, role, description, priority.clamp(0, 100)],
        )?;
        Ok(connection.last_insert_rowid())
    }

    pub fn list_goals(&self) -> Result<Vec<Goal>> {
        let connection = self.connection.lock().map_err(|_| anyhow::anyhow!("database lock poisoned"))?;
        let mut statement = connection.prepare(
            "SELECT id, description, status, created_at FROM goals ORDER BY id DESC LIMIT 100"
        )?;
        let rows = statement.query_map([], |row| Ok(Goal {
            id: row.get(0)?, description: row.get(1)?, status: row.get(2)?, created_at: row.get(3)?
        }))?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn list_tasks(&self, goal_id: i64) -> Result<Vec<AgentTask>> {
        let connection = self.connection.lock().map_err(|_| anyhow::anyhow!("database lock poisoned"))?;
        let mut statement = connection.prepare(
            "SELECT id, goal_id, role, description, priority, status, assigned_agent, lease_until FROM agent_tasks WHERE goal_id=?1 ORDER BY priority DESC, id"
        )?;
        let rows = statement.query_map([goal_id], |row| Ok(AgentTask {
            id: row.get(0)?, goal_id: row.get(1)?, role: row.get(2)?, description: row.get(3)?,
            priority: row.get(4)?, status: row.get(5)?, assigned_agent: row.get(6)?, lease_until: row.get(7)?
        }))?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn claim_task(&self, agent_id: &str, role: &str) -> Result<Option<AgentTask>> {
        let mut connection = self.connection.lock().map_err(|_| anyhow::anyhow!("database lock poisoned"))?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let task = tx.query_row(
            "SELECT id, goal_id, role, description, priority, status, assigned_agent, lease_until
             FROM agent_tasks
             WHERE role=?1 AND (status='pending' OR (status='claimed' AND lease_until IS NOT NULL AND lease_until < CURRENT_TIMESTAMP))
             ORDER BY priority DESC, id ASC LIMIT 1",
            [role],
            |row| Ok(AgentTask {
                id: row.get(0)?, goal_id: row.get(1)?, role: row.get(2)?, description: row.get(3)?,
                priority: row.get(4)?, status: row.get(5)?, assigned_agent: row.get(6)?, lease_until: row.get(7)?
            }),
        ).optional()?;

        if let Some(mut task) = task {
            tx.execute(
                "UPDATE agent_tasks SET status='claimed', assigned_agent=?1, lease_until=datetime('now','+2 minutes'), updated_at=CURRENT_TIMESTAMP WHERE id=?2",
                params![agent_id, task.id],
            )?;
            task.status = "claimed".into();
            task.assigned_agent = Some(agent_id.to_owned());
            task.lease_until = tx.query_row("SELECT lease_until FROM agent_tasks WHERE id=?1", [task.id], |row| row.get(0))?;
            tx.commit()?;
            Ok(Some(task))
        } else {
            tx.commit()?;
            Ok(None)
        }
    }

    pub fn renew_task_lease(&self, task_id: i64, agent_id: &str) -> Result<bool> {
        let connection = self.connection.lock().map_err(|_| anyhow::anyhow!("database lock poisoned"))?;
        let changed = connection.execute(
            "UPDATE agent_tasks SET lease_until=datetime('now','+2 minutes'), updated_at=CURRENT_TIMESTAMP
             WHERE id=?1 AND assigned_agent=?2 AND status='claimed' AND lease_until > CURRENT_TIMESTAMP",
            params![task_id, agent_id],
        )?;
        Ok(changed == 1)
    }

    pub fn complete_task(&self, task_id: i64, agent_id: &str) -> Result<bool> {
        let mut connection = self.connection.lock().map_err(|_| anyhow::anyhow!("database lock poisoned"))?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let goal_id: Option<i64> = tx.query_row(
            "SELECT goal_id FROM agent_tasks WHERE id=?1 AND assigned_agent=?2 AND status='claimed' AND lease_until > CURRENT_TIMESTAMP",
            params![task_id, agent_id],
            |row| row.get(0),
        ).optional()?;
        let Some(goal_id) = goal_id else {
            tx.commit()?;
            return Ok(false);
        };
        let changed = tx.execute(
            "UPDATE agent_tasks SET status='complete', lease_until=NULL, updated_at=CURRENT_TIMESTAMP
             WHERE id=?1 AND assigned_agent=?2 AND status='claimed' AND lease_until > CURRENT_TIMESTAMP",
            params![task_id, agent_id],
        )?;
        if changed != 1 {
            tx.commit()?;
            return Ok(false);
        }
        tx.execute(
            "UPDATE goals SET status='complete'
             WHERE id=?1 AND NOT EXISTS (
               SELECT 1 FROM agent_tasks WHERE goal_id=?1 AND status!='complete'
             )",
            [goal_id],
        )?;
        tx.commit()?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn specialist_tasks_are_claimed_once_and_complete_the_goal() {
        let store = Store::open(Path::new(":memory:")).unwrap();
        let goal = store.create_goal("Build a safe starter base").unwrap();
        let task = store.add_task(goal, "builder", "Build a small shelter", 80).unwrap();

        assert!(store.claim_task("scout-1", "scout").unwrap().is_none());
        let claimed = store.claim_task("builder-1", "builder").unwrap().unwrap();
        assert_eq!(claimed.id, task);
        assert_eq!(claimed.assigned_agent.as_deref(), Some("builder-1"));
        assert!(claimed.lease_until.is_some());
        assert!(store.renew_task_lease(task, "builder-1").unwrap());
        assert!(store.claim_task("builder-2", "builder").unwrap().is_none());

        assert!(!store.complete_task(task, "builder-2").unwrap());
        assert!(store.complete_task(task, "builder-1").unwrap());
        assert_eq!(store.list_goals().unwrap()[0].status, "complete");
    }
}
