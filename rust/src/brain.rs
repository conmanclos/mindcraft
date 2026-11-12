use crate::config::Config;
use anyhow::{anyhow, bail, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::Arc;

#[derive(Clone)]
pub struct Brain {
    config: Arc<Config>,
    http: Client,
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
        Self { config, http: Client::builder().timeout(std::time::Duration::from_secs(45)).build().expect("HTTP client") }
    }

    pub async fn reply(&self, prompt: &str) -> Result<String> {
        if prompt.trim().is_empty() { bail!("prompt cannot be empty"); }
        if prompt.len() > 12_000 { bail!("prompt exceeds 12,000 bytes"); }

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
            "Convert this Minecraft request into exactly one JSON object with fields action, target, reason, risk.              Allowed actions: look, follow, stop, say, status, inspect, goto, mine, place, equip, craft, use_item, attack, drop.              risk must be low, medium, or high. Do not use markdown fences. User request: {request}"
        );
        let raw = self.reply(&prompt).await?;
        let start = raw.find('{').ok_or_else(|| anyhow!("planner did not return a JSON object"))?;
        let end = raw.rfind('}').ok_or_else(|| anyhow!("planner returned incomplete JSON"))?;
        let action: PlannedAction = serde_json::from_str(&raw[start..=end])?;
        self.authorize_action(&action)?;
        Ok(action)
    }

    pub fn authorize_action(&self, action: &PlannedAction) -> Result<()> {
        let action_name = action.action.to_lowercase();
        let safe = ["look", "follow", "stop", "say", "status", "inspect", "goto"];
        let destructive = ["attack", "mine", "place", "drop", "equip", "craft", "use_item"];
        if !safe.contains(&action_name.as_str()) && !destructive.contains(&action_name.as_str()) {
            bail!("unknown action '{}'", action.action);
        }
        if self.config.mode == "assisted" && destructive.contains(&action_name.as_str()) {
            bail!("assisted mode requires explicit approval for '{}'", action.action);
        }
        if action_name == "drop" || action_name == "attack" {
            bail!("'{}' is disabled until a target-aware execution adapter is active", action.action);
        }
        if self.config.mode != "autonomous" && action.risk.eq_ignore_ascii_case("high") {
            bail!("high-risk action blocked by {} autonomy profile", self.config.mode);
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
        let url = format!("https://generativelanguage.googleapis.com/v1beta/models/{}:generateContent?key={}", self.config.model, self.config.api_key);
        let value: Value = self.http.post(url)
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
            database: PathBuf::from(":memory:")
        }))
    }

    #[test]
    fn assisted_mode_blocks_destructive_actions() {
        let action = PlannedAction { action:"mine".into(), target:None, reason:String::new(), risk:"low".into() };
        assert!(brain("assisted").authorize_action(&action).is_err());
        assert!(brain("autonomous").authorize_action(&action).is_ok());
    }

    #[test]
    fn drop_is_blocked_until_executor_exists() {
        let action = PlannedAction { action:"drop".into(), target:Some("diamond".into()), reason:String::new(), risk:"high".into() };
        assert!(brain("autonomous").authorize_action(&action).is_err());
    }
}
