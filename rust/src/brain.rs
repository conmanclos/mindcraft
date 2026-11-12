use crate::config::Config;
use anyhow::{anyhow, bail, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::Semaphore;

#[derive(Clone)]
pub struct Brain {
    config: Arc<Config>,
    http: Client,
    llm_slots: Arc<Semaphore>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskDraft {
    pub role: String,
    pub description: String,
    #[serde(default)]
    pub priority: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlannedAction {
    pub action: String,
    #[serde(default)]
    pub target: Option<String>,
    #[serde(default)]
    pub reason: String,
    #[serde(default)]
    pub risk: String,
}

impl Brain {
    pub fn new(config: Arc<Config>) -> Self {
        Self {
            config,
            http: Client::builder().timeout(std::time::Duration::from_secs(45)).build().expect("HTTP client"),
            llm_slots: Arc::new(Semaphore::new(2)),
        }
    }

    pub async fn reply(&self, prompt: &str) -> Result<String> {
        if prompt.trim().is_empty() { bail!("prompt cannot be empty"); }
        if prompt.len() > 12_000 { bail!("prompt exceeds 12,000 bytes"); }

        let _permit = self.llm_slots.acquire().await.map_err(|_| anyhow!("AI concurrency limiter is closed"))?;
        let system = format!(
            "You are Mindcraft, a cautious Minecraft Java 1.21.1 teammate. Autonomy profile: {}. You may advise or propose actions, but never claim that an in-game action occurred unless the runtime confirms it. Prioritize player safety, server rules, and explicit user intent. Keep responses concise and operational.",
            self.config.mode
        );
        match self.config.provider.as_str() {
            "anthropic" => self.anthropic(&system, prompt).await,
            "google" => self.google(&system, prompt).await,
            "openai" | "openai-compatible" | "ollama" => self.openai_compatible(&system, prompt).await,
            other => bail!("unsupported provider '{other}'; use openai, anthropic, google, ollama, or openai-compatible"),
        }
    }

    pub async fn plan(&self, request: &str) -> Result<PlannedAction> {
        let prompt = format!(
            "Convert this Minecraft request into exactly one JSON object with fields action, target, reason, risk.              Allowed actions: look, follow, stop, say, goto, explore, mine, place, deposit, withdraw, equip, jump, sneak, use_item, craft, attack. Only craft planks, sticks, or crafting_table; do not invent other recipes. Do not invent inventory-drop, status, or inspection actions. sneak target must be true or false. use_item uses the held item: right-click the block under the crosshair, or use it in air for a bounded 32-tick hold. craft target must be planks, sticks, or crafting_table. deposit target must be container coordinates x,y,z; the bot preserves its hotbar. withdraw target must be x,y,z,item_id and only moves matching stacks from that container. explore target is a radius in blocks from 8 to 128. attack target must be an online Minecraft player username currently loaded in the world and within melee range.              risk must be low, medium, or high. Do not use markdown fences. User request: {request}"
        );
        let raw = self.reply(&prompt).await?;
        let start = raw.find('{').ok_or_else(|| anyhow!("planner did not return a JSON object"))?;
        let end = raw.rfind('}').ok_or_else(|| anyhow!("planner returned incomplete JSON"))?;
        let action: PlannedAction = serde_json::from_str(&raw[start..=end])?;
        self.validate_action(&action)?;
        Ok(action)
    }

    pub async fn decompose_goal(&self, goal: &str) -> Result<Vec<TaskDraft>> {
        if goal.trim().is_empty() || goal.len() > 4_000 {
            bail!("goal must contain 1-4000 bytes");
        }
        let prompt = format!(
            "Decompose this Minecraft objective into 1 to 12 independently actionable tasks for a cooperative team. Return ONLY a JSON array. Each object must have role, description, priority. Allowed roles: planner, scout, gatherer, builder, crafter, guardian. Priority is an integer 0-100. Do not include code fences or prose. Objective: {goal}"
        );
        let raw = self.reply(&prompt).await?;
        let start = raw.find('[').ok_or_else(|| anyhow!("strategic planner did not return a JSON array"))?;
        let end = raw.rfind(']').ok_or_else(|| anyhow!("strategic planner returned incomplete JSON"))?;
        let mut tasks: Vec<TaskDraft> = serde_json::from_str(&raw[start..=end])?;
        if tasks.is_empty() || tasks.len() > 12 {
            bail!("strategic planner must return between 1 and 12 tasks");
        }
        let allowed = ["planner", "scout", "gatherer", "builder", "crafter", "guardian"];
        for task in &mut tasks {
            task.role = task.role.to_lowercase();
            if !allowed.contains(&task.role.as_str()) {
                bail!("unsupported agent role '{}'", task.role);
            }
            if task.description.trim().is_empty() || task.description.len() > 500 {
                bail!("agent task descriptions must contain 1-500 bytes");
            }
            task.priority = task.priority.clamp(0, 100);
        }
        Ok(tasks)
    }

    pub fn validate_action(&self, action: &PlannedAction) -> Result<()> {
        let action_name = action.action.to_lowercase();
        let supported = ["look", "follow", "stop", "say", "goto", "explore", "mine", "place", "deposit", "withdraw", "equip", "jump", "sneak", "use_item", "craft", "attack", "drop"];
        if !supported.contains(&action_name.as_str()) {
            bail!("unsupported action '{}'; this runtime currently supports chat, stop, follow, look, goto, mine, place, equip, jump, sneak, block-targeted item use, basic crafting, inventory deposit/withdrawal, exploration, and player-targeted attack", action.action);
        }
        if !["low", "medium", "high"].contains(&action.risk.to_lowercase().as_str()) {
            bail!("risk must be low, medium, or high");
        }
        if action.target.as_ref().is_some_and(|target| target.len() > 256 || target.chars().any(char::is_control)) {
            bail!("action target must be at most 256 characters and contain no control characters");
        }
        Ok(())
    }

    pub fn authorize_action(&self, action: &PlannedAction, explicit_approval: bool) -> Result<()> {
        self.validate_action(action)?;
        let action_name = action.action.to_lowercase();
        let safe = ["look", "follow", "stop", "say", "goto", "explore", "jump", "sneak"];
        let destructive = ["attack", "mine", "place", "drop", "equip", "use_item", "craft", "deposit", "withdraw"];
        if !safe.contains(&action_name.as_str()) && !destructive.contains(&action_name.as_str()) {
            bail!("unknown action '{}'", action.action);
        }
        if self.config.mode == "assisted" && destructive.contains(&action_name.as_str()) && !explicit_approval {
            bail!("assisted mode requires explicit approval for '{}'", action.action);
        }
        if action_name == "attack" {
            let target = action.target.as_deref().unwrap_or_default();
            if !(3..=16).contains(&target.len()) || !target.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                bail!("attack target must be a valid Minecraft username");
            }
        }
        if action_name == "attack" && !explicit_approval {
            bail!("attack always requires explicit approval, including in autonomous mode");
        }
        if action_name == "use_item" && !explicit_approval {
            bail!("using the held item always requires explicit approval");
        }
        if action_name == "drop" {
            bail!("drop is disabled until a safe inventory-item selection adapter is active");
        }
        if self.config.mode != "autonomous" && action.risk.eq_ignore_ascii_case("high") && !explicit_approval {
            bail!("high-risk action requires explicit approval in {} mode", self.config.mode);
        }
        Ok(())
    }

    async fn openai_compatible(&self, system: &str, prompt: &str) -> Result<String> {
        let base = self.config.base_url.trim_end_matches('/');
        let url = if base.ends_with("/chat/completions") { base.to_string() } else { format!("{base}/chat/completions") };
        let mut request = self.http.post(url).json(&json!({
            "model": self.config.model,
            "messages": [{"role":"system","content":system},{"role":"user","content":prompt}],
            "temperature": 0.35
        }));
        if !self.config.api_key.is_empty() { request = request.bearer_auth(&self.config.api_key); }
        let value: Value = request.send().await?.error_for_status()?.json().await?;
        value.pointer("/choices/0/message/content").and_then(Value::as_str)
            .map(str::to_owned).ok_or_else(|| anyhow!("provider response did not contain choices[0].message.content"))
    }

    async fn anthropic(&self, system: &str, prompt: &str) -> Result<String> {
        if self.config.api_key.is_empty() { bail!("MINDCRAFT_API_KEY is required for Anthropic"); }
        let value: Value = self.http.post("https://api.anthropic.com/v1/messages")
            .header("x-api-key", &self.config.api_key)
            .header("anthropic-version", "2023-06-01")
            .json(&json!({"model":self.config.model,"max_tokens":1200,"system":system,"messages":[{"role":"user","content":prompt}]}))
            .send().await?.error_for_status()?.json().await?;
        value.pointer("/content/0/text").and_then(Value::as_str)
            .map(str::to_owned).ok_or_else(|| anyhow!("Anthropic response did not contain text"))
    }

    async fn google(&self, system: &str, prompt: &str) -> Result<String> {
        if self.config.api_key.is_empty() { bail!("MINDCRAFT_API_KEY is required for Google Gemini"); }
        let url = format!("https://generativelanguage.googleapis.com/v1beta/models/{}:generateContent", self.config.model);
        let value: Value = self.http.post(url)
            .header("x-goog-api-key", &self.config.api_key)
            .json(&json!({"systemInstruction":{"parts":[{"text":system}]},"contents":[{"role":"user","parts":[{"text":prompt}]}],"generationConfig":{"temperature":0.35,"maxOutputTokens":1200}}))
            .send().await?.error_for_status()?.json().await?;
        value.pointer("/candidates/0/content/parts/0/text").and_then(Value::as_str)
            .map(str::to_owned).ok_or_else(|| anyhow!("Gemini response did not contain text"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use std::{net::SocketAddr, path::PathBuf};

    fn brain(mode: &str) -> Brain {
        Brain::new(Arc::new(Config {
            bind: "127.0.0.1:8787".parse::<SocketAddr>().unwrap(),
            api_token: "test".into(), api_token_was_generated: false,
            mode: mode.into(), provider: "ollama".into(), model: "test".into(),
            api_key: String::new(), base_url: "http://localhost:11434/v1".into(),
            minecraft_server: None, minecraft_username: "test".into(),
            minecraft_accounts: vec!["test".into()],
            minecraft_auth: "offline".into(), microsoft_email: None,
            database: PathBuf::from(":memory:")
        }))
    }

    #[test]
    fn assisted_mode_blocks_destructive_actions() {
        let action = PlannedAction { action:"mine".into(), target:None, reason:String::new(), risk:"low".into() };
        assert!(brain("assisted").authorize_action(&action, false).is_err());
        assert!(brain("assisted").authorize_action(&action, true).is_ok());
        assert!(brain("autonomous").authorize_action(&action, false).is_ok());
    }


    #[test]
    fn attack_requires_explicit_approval_even_in_autonomous_mode() {
        let action = PlannedAction { action:"attack".into(), target:Some("Notch".into()), reason:String::new(), risk:"high".into() };
        assert!(brain("autonomous").authorize_action(&action, false).is_err());
        assert!(brain("autonomous").authorize_action(&action, true).is_ok());
    }

    #[test]
    fn planner_rejects_invalid_risk_and_unsafe_targets() {
        let brain = brain("balanced");
        let invalid_risk = PlannedAction {
            action: "goto".into(), target: Some("1,64,1".into()),
            reason: String::new(), risk: "unknown".into(),
        };
        assert!(brain.validate_action(&invalid_risk).is_err());
        let control_target = PlannedAction {
            action: "say".into(), target: Some("hello\n/stop".into()),
            reason: String::new(), risk: "low".into(),
        };
        assert!(brain.validate_action(&control_target).is_err());
    }

    #[test]
    fn held_item_use_always_requires_explicit_approval() {
        let action = PlannedAction {
            action: "use_item".into(), target: None,
            reason: String::new(), risk: "medium".into(),
        };
        assert!(brain("autonomous").authorize_action(&action, false).is_err());
        assert!(brain("autonomous").authorize_action(&action, true).is_ok());
    }

    #[test]
    fn drop_is_blocked_until_executor_exists() {
        let action = PlannedAction { action:"drop".into(), target:Some("diamond".into()), reason:String::new(), risk:"high".into() };
        assert!(brain("autonomous").authorize_action(&action, true).is_err());
    }
}
