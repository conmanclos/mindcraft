use anyhow::{bail, Result};
use std::{env, net::SocketAddr, path::PathBuf};

#[derive(Clone)]
pub struct Config {
    pub bind: SocketAddr,
    pub api_token: String,
    pub api_token_was_generated: bool,
    pub mode: String,
    pub provider: String,
    pub model: String,
    pub api_key: String,
    pub base_url: String,
    pub minecraft_server: Option<String>,
    pub minecraft_username: String,
    pub database: PathBuf,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let bind: SocketAddr = env::var("MINDCRAFT_BIND")
            .unwrap_or_else(|_| "127.0.0.1:8787".into())
            .parse()?;
        let mode = env::var("MINDCRAFT_MODE").unwrap_or_else(|_| "balanced".into()).to_lowercase();
        if !["assisted", "balanced", "autonomous"].contains(&mode.as_str()) {
            bail!("MINDCRAFT_MODE must be assisted, balanced, or autonomous");
        }
        let provider = env::var("MINDCRAFT_PROVIDER").unwrap_or_else(|_| "ollama".into()).to_lowercase();
        if !["openai", "openai-compatible", "anthropic", "google", "ollama"].contains(&provider.as_str()) {
            bail!("MINDCRAFT_PROVIDER must be openai, openai-compatible, anthropic, google, or ollama");
        }
        let api_key = env::var("MINDCRAFT_API_KEY").unwrap_or_default();
        if provider == "openai" && api_key.is_empty() {
            bail!("MINDCRAFT_API_KEY is required when MINDCRAFT_PROVIDER=openai");
        }

        let mut api_token = env::var("MINDCRAFT_API_TOKEN").unwrap_or_default();
        let api_token_was_generated = api_token.is_empty();
        if api_token_was_generated {
            if !bind.ip().is_loopback() {
                bail!("MINDCRAFT_API_TOKEN is required when binding outside loopback");
            }
            api_token = uuid::Uuid::new_v4().to_string();
        }

        Ok(Self {
            bind,
            api_token,
            api_token_was_generated,
            mode,
            provider,
            model: env::var("MINDCRAFT_MODEL").unwrap_or_else(|_| "llama3.1".into()),
            api_key,
            base_url: env::var("MINDCRAFT_BASE_URL").unwrap_or_else(|_| "https://api.openai.com/v1".into()),
            minecraft_server: env::var("MINDCRAFT_MC_SERVER").ok().filter(|s| !s.trim().is_empty()),
            minecraft_username: env::var("MINDCRAFT_MC_USERNAME").unwrap_or_else(|_| "Mindcraft".into()),
            database: PathBuf::from(env::var("MINDCRAFT_DATABASE").unwrap_or_else(|_| "./mindcraft.sqlite3".into())),
        })
    }
}
