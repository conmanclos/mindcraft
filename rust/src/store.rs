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

    /// Format recent conversation history within a UTF-8 byte budget, keeping
    /// the newest messages and the tail of any message that must be truncated.
    pub fn recent_context(&self, limit: usize, max_bytes: usize) -> Result<String> {
        let history = self.recent_messages(limit)?;
        let mut selected = Vec::new();
        let mut remaining = max_bytes;
        for (role, content) in history.into_iter().rev() {
            let prefix = format!("{role}: ");
            if remaining <= prefix.len() + 1 { break; }
            let body_budget = remaining - prefix.len() - 1;
            let mut body_start = content.len();
            for (index, _) in content.char_indices().rev() {
                if content.len() - index > body_budget { break; }
                body_start = index;
            }
            let line = format!("{prefix}{}", &content[body_start..]);
            let used = line.len() + 1;
            if used > remaining { break; }
            remaining -= used;
            selected.push(line);
        }
        selected.reverse();
        Ok(selected.join("\n"))
    }

    pub fn create_goal(&self, description: &str) -> Result<i64> {
        let connection = self.connection.lock().map_err(|_| anyhow::anyhow!("database lock poisoned"))?;
        connection.execute("INSERT INTO goals(description) VALUES (?1)", [description])?;
        Ok(connection.last_insert_rowid())
    }

    /// Create a goal and all of its specialist tasks in one transaction so
    /// an interrupted write cannot leave a partially populated strategic plan.
    pub fn create_goal_with_tasks(
        &self,
        description: &str,
        tasks: &[(String, String, i32)],
    ) -> Result<(i64, Vec<i64>)> {
        let mut connection = self.connection.lock().map_err(|_| anyhow::anyhow!("database lock poisoned"))?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute("INSERT INTO goals(description) VALUES (?1)", [description])?;
        let goal_id = tx.last_insert_rowid();
        let mut task_ids = Vec::with_capacity(tasks.len());
        for (role, task_description, priority) in tasks {
            tx.execute(
                "INSERT INTO agent_tasks(goal_id, role, description, priority) VALUES (?1, ?2, ?3, ?4)",
                params![goal_id, role.as_str(), task_description.as_str(), (*priority).clamp(0, 100)],
            )?;
            task_ids.push(tx.last_insert_rowid());
        }
        tx.commit()?;
        Ok((goal_id, task_ids))
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
             WHERE (status='pending' OR (status='claimed' AND (lease_until IS NULL OR lease_until < CURRENT_TIMESTAMP)))
               AND (
                 role=?1
                 OR NOT EXISTS (
                   SELECT 1 FROM agent_tasks preferred
                   WHERE preferred.role=?1
                     AND (preferred.status='pending'
                       OR (preferred.status='claimed' AND (preferred.lease_until IS NULL OR preferred.lease_until < CURRENT_TIMESTAMP)))
                 )
               )
             ORDER BY CASE WHEN role=?1 THEN 0 ELSE 1 END, priority DESC, id ASC LIMIT 1",
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
             WHERE id=?1 AND assigned_agent=?2
               AND (status='review' OR (status='claimed' AND lease_until > CURRENT_TIMESTAMP))",
            params![task_id, agent_id],
        )?;
        Ok(changed == 1)
    }

    pub fn mark_task_for_review(&self, task_id: i64, agent_id: &str) -> Result<bool> {
        let connection = self.connection.lock().map_err(|_| anyhow::anyhow!("database lock poisoned"))?;
        let changed = connection.execute(
            "UPDATE agent_tasks
             SET status='review', lease_until=NULL, updated_at=CURRENT_TIMESTAMP
             WHERE id=?1 AND assigned_agent=?2
               AND (status='review' OR (status='claimed' AND lease_until > CURRENT_TIMESTAMP))",
            params![task_id, agent_id],
        )?;
        Ok(changed == 1)
    }

    pub fn has_review_task(&self, agent_id: &str) -> Result<bool> {
        let connection = self.connection.lock().map_err(|_| anyhow::anyhow!("database lock poisoned"))?;
        let exists: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM agent_tasks WHERE assigned_agent=?1 AND status='review')",
            [agent_id],
            |row| row.get(0),
        )?;
        Ok(exists)
    }

    pub fn list_review_tasks(&self) -> Result<Vec<AgentTask>> {
        let connection = self.connection.lock().map_err(|_| anyhow::anyhow!("database lock poisoned"))?;
        let mut statement = connection.prepare(
            "SELECT id, goal_id, role, description, priority, status, assigned_agent, lease_until
             FROM agent_tasks WHERE status='review' ORDER BY priority DESC, id ASC"
        )?;
        let rows = statement.query_map([], |row| Ok(AgentTask {
            id: row.get(0)?, goal_id: row.get(1)?, role: row.get(2)?, description: row.get(3)?,
            priority: row.get(4)?, status: row.get(5)?, assigned_agent: row.get(6)?, lease_until: row.get(7)?,
        }))?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn release_task(&self, task_id: i64, agent_id: &str) -> Result<bool> {
        let connection = self.connection.lock().map_err(|_| anyhow::anyhow!("database lock poisoned"))?;
        let changed = connection.execute(
            "UPDATE agent_tasks
             SET status='pending', assigned_agent=NULL, lease_until=NULL, updated_at=CURRENT_TIMESTAMP
             WHERE id=?1 AND assigned_agent=?2
               AND (status='review' OR (status='claimed' AND lease_until > CURRENT_TIMESTAMP))",
            params![task_id, agent_id],
        )?;
        Ok(changed == 1)
    }

    pub fn complete_task(&self, task_id: i64, agent_id: &str) -> Result<bool> {
        let mut connection = self.connection.lock().map_err(|_| anyhow::anyhow!("database lock poisoned"))?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let goal_id: Option<i64> = tx.query_row(
            "SELECT goal_id FROM agent_tasks WHERE id=?1 AND assigned_agent=?2
             AND (status='review' OR (status='claimed' AND lease_until > CURRENT_TIMESTAMP))",
            params![task_id, agent_id],
            |row| row.get(0),
        ).optional()?;
        let Some(goal_id) = goal_id else {
            tx.commit()?;
            return Ok(false);
        };
        // A task may be completed directly while its lease is active, or
        // after the worker explicitly submitted it for human review. Review
        // tasks intentionally have no lease, so do not apply the lease check
        // to that state.
        let changed = tx.execute(
            "UPDATE agent_tasks SET status='complete', lease_until=NULL, updated_at=CURRENT_TIMESTAMP
             WHERE id=?1 AND assigned_agent=?2
               AND (status='review' OR (status='claimed' AND lease_until > CURRENT_TIMESTAMP))",
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
    fn recent_context_respects_utf8_byte_budget_and_keeps_message_tail() {
        let store = Store::open(Path::new(":memory:")).unwrap();
        let message = "🙂".repeat(100);
        store.append_message("user", &message).unwrap();
        let context = store.recent_context(1, 40).unwrap();
        assert!(context.len() <= 40);
        assert!(context.starts_with("user: "));
        let expected_suffix = "🙂".repeat(8);
        assert!(context.ends_with(expected_suffix.as_str()));
    }

    #[test]
    fn recent_context_keeps_newest_messages_within_budget() {
        let store = Store::open(Path::new(":memory:")).unwrap();
        store.append_message("user", &"old".repeat(20)).unwrap();
        store.append_message("assistant", &"new".repeat(20)).unwrap();
        let context = store.recent_context(12, 40).unwrap();
        assert!(context.chars().count() <= 40);
        assert!(context.contains("assistant:"));
        assert!(context.contains("new"));
        assert!(!context.contains("old"));
    }

    #[test]
    fn goal_and_specialist_tasks_are_created_as_one_durable_plan() {
        let store = Store::open(Path::new(":memory:")).unwrap();
        let tasks = vec![
            ("scout".to_owned(), "Find a safe base location".to_owned(), 90),
            ("gatherer".to_owned(), "Collect nearby wood".to_owned(), 70),
        ];
        let (goal_id, task_ids) = store.create_goal_with_tasks("Build a starter base", &tasks).unwrap();
        assert_eq!(task_ids.len(), 2);
        let stored = store.list_tasks(goal_id).unwrap();
        assert_eq!(stored.len(), 2);
        assert_eq!(stored[0].id, task_ids[0]);
        assert_eq!(stored[0].role, "scout");
        assert_eq!(stored[1].role, "gatherer");
        assert!(stored.iter().all(|task| task.status == "pending"));
    }

    #[test]
    fn worker_falls_back_to_other_specialist_roles_when_its_queue_is_empty() {
        let store = Store::open(Path::new(":memory:")).unwrap();
        let goal = store.create_goal("Prepare a safe camp").unwrap();
        let scout_id = store.add_task(goal, "scout", "Find a safe location", 90).unwrap();
        let gatherer_id = store.add_task(goal, "gatherer", "Collect wood", 70).unwrap();

        let first = store.claim_task("SoloBot", "planner").unwrap().unwrap();
        assert_eq!(first.id, scout_id);
        assert_eq!(first.role, "scout");
        assert!(store.mark_task_for_review(first.id, "SoloBot").unwrap());

        let second = store.claim_task("SoloBot", "planner").unwrap().unwrap();
        assert_eq!(second.id, gatherer_id);
        assert_eq!(second.role, "gatherer");
    }

    #[test]
    fn reviewed_task_can_be_released_back_to_pending() {
        let store = Store::open(Path::new(":memory:")).unwrap();
        let goal = store.create_goal("Build a shelter").unwrap();
        let task_id = store.add_task(goal, "builder", "Place the shelter blocks", 80).unwrap();

        store.claim_task("Builder", "builder").unwrap().unwrap();
        assert!(store.mark_task_for_review(task_id, "Builder").unwrap());
        assert!(store.release_task(task_id, "Builder").unwrap());

        let task = store.list_tasks(goal).unwrap().remove(0);
        assert_eq!(task.status, "pending");
        assert_eq!(task.assigned_agent, None);
        assert!(store.claim_task("OtherBuilder", "builder").unwrap().is_some());
    }

    #[test]
    fn reviewed_task_can_be_completed_by_its_assigned_worker() {
        let store = Store::open(Path::new(":memory:")).unwrap();
        let goal = store.create_goal("Build a shelter").unwrap();
        let task_id = store.add_task(goal, "builder", "Place the shelter blocks", 80).unwrap();

        let claimed = store.claim_task("Builder", "builder").unwrap().unwrap();
        assert_eq!(claimed.id, task_id);
        assert!(store.mark_task_for_review(task_id, "Builder").unwrap());
        assert!(store.complete_task(task_id, "Builder").unwrap());

        let tasks = store.list_tasks(goal).unwrap();
        assert_eq!(tasks[0].status, "complete");
        assert!(store.list_goals().unwrap().iter().find(|item| item.id == goal).unwrap().status == "complete");
    }

    #[test]
    fn task_leases_are_owned_renewable_and_reclaimable_after_expiry() {
        let store = Store::open(Path::new(":memory:")).unwrap();
        let goal = store.create_goal("Gather supplies").unwrap();
        let task_id = store.add_task(goal, "scout", "Locate nearby wood", 60).unwrap();

        let first = store.claim_task("worker-a", "scout").unwrap().unwrap();
        assert_eq!(first.id, task_id);
        assert_eq!(first.assigned_agent.as_deref(), Some("worker-a"));
        assert!(store.renew_task_lease(task_id, "worker-a").unwrap());
        assert!(!store.renew_task_lease(task_id, "worker-b").unwrap());

        {
            let connection = store.connection.lock().unwrap();
            connection.execute(
                "UPDATE agent_tasks SET lease_until=datetime('now','-1 second') WHERE id=?1",
                [task_id],
            ).unwrap();
        }

        let recovered = store.claim_task("worker-b", "scout").unwrap().unwrap();
        assert_eq!(recovered.assigned_agent.as_deref(), Some("worker-b"));
        assert!(!store.complete_task(task_id, "worker-a").unwrap());
        assert!(store.complete_task(task_id, "worker-b").unwrap());
    }

    #[test]
    fn task_review_queue_holds_work_until_human_confirmation_or_release() {
        let store = Store::open(Path::new(":memory:")).unwrap();
        let (_, task_ids) = store.create_goal_with_tasks(
            "build shelter",
            &[("builder".into(), "place the shelter walls".into(), 50)],
        ).unwrap();
        let task_id = task_ids[0];
        assert!(store.claim_task("Builder", "builder").unwrap().is_some());
        assert!(store.mark_task_for_review(task_id, "Builder").unwrap());
        assert!(store.has_review_task("Builder").unwrap());
        assert_eq!(store.list_review_tasks().unwrap()[0].status, "review");
        assert!(store.claim_task("Builder2", "builder").unwrap().is_none());
        assert!(store.complete_task(task_id, "Builder").unwrap());
        assert!(!store.has_review_task("Builder").unwrap());
    }

    #[test]
    fn claimed_task_can_be_released_for_another_specialist() {
        let store = Store::open(Path::new(":memory:")).unwrap();
        let (_, task_ids) = store.create_goal_with_tasks(
            "collect wood",
            &[("gatherer".into(), "collect logs".into(), 10)],
        ).unwrap();
        let task_id = task_ids[0];
        assert!(store.claim_task("Scout", "gatherer").unwrap().is_some());
        assert!(!store.release_task(task_id, "Builder").unwrap());
        assert!(store.release_task(task_id, "Scout").unwrap());
        assert_eq!(store.list_tasks(1).unwrap()[0].status, "pending");
        assert!(store.claim_task("Builder", "gatherer").unwrap().is_some());
    }

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
