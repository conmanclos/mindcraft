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
    pub minecraft_accounts: Vec<String>,
    pub minecraft_auth: String,
    pub microsoft_email: Option<String>,
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
        let minecraft_auth = env::var("MINDCRAFT_MC_AUTH").unwrap_or_else(|_| "offline".into()).to_lowercase();
        if !["offline", "microsoft"].contains(&minecraft_auth.as_str()) {
            bail!("MINDCRAFT_MC_AUTH must be offline or microsoft");
        }
        let microsoft_email = env::var("MINDCRAFT_MICROSOFT_EMAIL").ok().filter(|value| !value.trim().is_empty());
        if minecraft_auth == "microsoft" && microsoft_email.is_none() {
            bail!("MINDCRAFT_MICROSOFT_EMAIL is required when MINDCRAFT_MC_AUTH=microsoft");
        }
        let minecraft_username = env::var("MINDCRAFT_MC_USERNAME").unwrap_or_else(|_| "Mindcraft".into());
        let minecraft_accounts = env::var("MINDCRAFT_MC_ACCOUNTS").ok()
            .map(|value| value.split(',').map(str::trim).filter(|name| !name.is_empty()).map(str::to_owned).collect::<Vec<_>>())
            .filter(|accounts| !accounts.is_empty())
            .unwrap_or_else(|| vec![minecraft_username.clone()]);
        if minecraft_auth == "microsoft" && minecraft_accounts.len() != 1 {
            bail!("Microsoft auth currently supports one account; use MINDCRAFT_MC_ACCOUNTS for multiple offline accounts");
        }
        if minecraft_accounts.len() > 8 {
            bail!("MINDCRAFT_MC_ACCOUNTS is limited to 8 clients per runtime");
        }
        if minecraft_auth == "offline" {
            let mut unique = std::collections::HashSet::new();
            for account in &minecraft_accounts {
                if !(3..=16).contains(&account.len())
                    || !account.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
                    || !unique.insert(account.to_lowercase()) {
                    bail!("offline usernames must be unique Minecraft names (3-16 letters, digits, or underscores)");
                }
            }
        }
        let api_key = env::var("MINDCRAFT_API_KEY").unwrap_or_default();
        let default_base_url = if provider == "ollama" { "http://127.0.0.1:11434/v1" } else { "https://api.openai.com/v1" };
        let base_url = env::var("MINDCRAFT_BASE_URL").unwrap_or_else(|_| default_base_url.into());
        if ["openai", "anthropic", "google"].contains(&provider.as_str()) && api_key.is_empty() {
            bail!("MINDCRAFT_API_KEY is required for the selected hosted provider");
        }
        let model = env::var("MINDCRAFT_MODEL").ok().filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| if provider == "ollama" { "llama3.1".into() } else { String::new() });
        if model.is_empty() {
            bail!("MINDCRAFT_MODEL is required when using a hosted or OpenAI-compatible provider");
        }
        let parsed_base_url = reqwest::Url::parse(&base_url)
            .map_err(|error| anyhow::anyhow!("MINDCRAFT_BASE_URL is invalid: {error}"))?;
        if !["http", "https"].contains(&parsed_base_url.scheme()) || parsed_base_url.host_str().is_none() {
            bail!("MINDCRAFT_BASE_URL must be an absolute HTTP or HTTPS URL");
        }

        let mut api_token = env::var("MINDCRAFT_API_TOKEN").unwrap_or_default();
        let api_token_was_generated = api_token.is_empty();
        if api_token_was_generated {
            if !bind.ip().is_loopback() {
                bail!("MINDCRAFT_API_TOKEN is required when binding outside loopback");
            }
            api_token = uuid::Uuid::new_v4().to_string();
        }
        if api_token.chars().any(char::is_whitespace) {
            bail!("MINDCRAFT_API_TOKEN must not contain whitespace");
        }
        if !bind.ip().is_loopback() && api_token.len() < 32 {
            bail!("MINDCRAFT_API_TOKEN must be at least 32 characters when binding outside loopback");
        }

        Ok(Self {
            bind,
            api_token,
            api_token_was_generated,
            mode,
            provider,
            model,
            api_key,
            base_url,
            minecraft_server: env::var("MINDCRAFT_MC_SERVER").ok().filter(|s| !s.trim().is_empty()),
            minecraft_username,
            minecraft_accounts,
            minecraft_auth,
            microsoft_email,
            database: PathBuf::from(env::var("MINDCRAFT_DATABASE").unwrap_or_else(|_| "./mindcraft.sqlite3".into())),
        })
    }
}
