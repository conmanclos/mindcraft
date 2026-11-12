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
    pub trusted_chat_players: Vec<String>,
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
        let minecraft_server = env::var("MINDCRAFT_MC_SERVER").ok()
            .map(|server| server.trim().to_owned()).filter(|server| !server.is_empty());
        if let Some(server) = &minecraft_server {
            validate_minecraft_server(server)?;
        }
        let trusted_chat_players = parse_trusted_chat_players(
            &env::var("MINDCRAFT_TRUSTED_PLAYERS").unwrap_or_default()
        )?;
        let model = env::var("MINDCRAFT_MODEL").unwrap_or_else(|_| "llama3.1".into()).trim().to_owned();
        if model.trim().is_empty() {
            bail!("MINDCRAFT_MODEL must not be empty");
        }
        let api_key = env::var("MINDCRAFT_API_KEY").unwrap_or_default().trim().to_owned();
        let default_base_url = if provider == "ollama" { "http://127.0.0.1:11434/v1" } else { "https://api.openai.com/v1" };
        let base_url = env::var("MINDCRAFT_BASE_URL")
            .unwrap_or_else(|_| default_base_url.into()).trim().to_owned();
        if ["openai", "anthropic", "google"].contains(&provider.as_str()) && api_key.is_empty() {
            bail!("MINDCRAFT_API_KEY is required for the selected hosted AI provider");
        }
        validate_provider_base_url(&base_url)?;

        let mut api_token = env::var("MINDCRAFT_API_TOKEN").unwrap_or_default();
        let api_token_was_generated = api_token.is_empty();
        if api_token_was_generated {
            if !bind.ip().is_loopback() {
                bail!("MINDCRAFT_API_TOKEN is required when binding outside loopback");
            }
            api_token = uuid::Uuid::new_v4().to_string();
        }
        validate_api_token(&api_token)?;

        Ok(Self {
            bind,
            api_token,
            api_token_was_generated,
            mode,
            provider,
            model,
            api_key,
            base_url,
            minecraft_server,
            minecraft_username,
            minecraft_accounts,
            minecraft_auth,
            microsoft_email,
            trusted_chat_players,
            database: PathBuf::from(env::var("MINDCRAFT_DATABASE").unwrap_or_else(|_| "./mindcraft.sqlite3".into())),
        })
    }
}


fn validate_minecraft_server(value: &str) -> Result<()> {
    let value = value.trim();
    if value.is_empty() || value.chars().any(char::is_whitespace)
        || value.chars().any(|character| character.is_control() || matches!(character, '/' | '\\' | '?' | '#' | '@')) {
        bail!("MINDCRAFT_MC_SERVER must be a host name or host:port without a URL scheme, path, or whitespace");
    }
    let host = match value.rsplit_once(':') {
        Some((host, port)) => {
            if host.contains(':') || !port.parse::<u16>().is_ok_and(|port| port > 0) {
                bail!("MINDCRAFT_MC_SERVER must use a valid host:port (IPv6 literals are not supported by the pinned Azalea address parser)");
            }
            host
        }
        None => value,
    };
    if host.is_empty() || !host.chars().all(|character| character.is_ascii_alphanumeric() || matches!(character, '.' | '-' | '_')) {
        bail!("MINDCRAFT_MC_SERVER contains an invalid host name");
    }
    Ok(())
}

fn parse_trusted_chat_players(raw: &str) -> Result<Vec<String>> {
    let names = raw.split(',').map(str::trim).filter(|name| !name.is_empty())
        .map(str::to_owned).collect::<Vec<_>>();
    if names.len() > 32 {
        bail!("MINDCRAFT_TRUSTED_PLAYERS is limited to 32 usernames");
    }
    let mut unique = std::collections::HashSet::new();
    for name in &names {
        if !(3..=16).contains(&name.len())
            || !name.chars().all(|character| character.is_ascii_alphanumeric() || character == '_')
            || !unique.insert(name.to_ascii_lowercase()) {
            bail!("trusted chat players must be unique Minecraft usernames (3-16 letters, digits, or underscores)");
        }
    }
    Ok(names)
}

fn validate_provider_base_url(value: &str) -> Result<()> {
    let parsed = reqwest::Url::parse(value)
        .map_err(|_| anyhow::anyhow!("MINDCRAFT_BASE_URL must be a valid http:// or https:// base URL"))?;
    let host = parsed.host_str().unwrap_or_default();
    let loopback_host = host.eq_ignore_ascii_case("localhost")
        || host.parse::<std::net::IpAddr>().is_ok_and(|address| address.is_loopback());
    if !["http", "https"].contains(&parsed.scheme())
        || host.is_empty()
        || (parsed.scheme() == "http" && !loopback_host)
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        bail!("MINDCRAFT_BASE_URL must use HTTPS for remote hosts (HTTP is allowed only for localhost/loopback), include a host, and contain no credentials, query, or fragment");
    }
    Ok(())
}

fn validate_api_token(token: &str) -> Result<()> {
    if token.len() < 32 || !token.is_ascii()
        || token.bytes().any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control()) {
        bail!("MINDCRAFT_API_TOKEN must contain at least 32 non-whitespace ASCII bytes");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{parse_trusted_chat_players, validate_api_token, validate_minecraft_server, validate_provider_base_url};

    #[test]
    fn provider_urls_require_https_for_remote_hosts_and_reject_credentials() {
        assert!(validate_provider_base_url("http://127.0.0.1:11434/v1").is_ok());
        assert!(validate_provider_base_url("http://localhost:11434/v1").is_ok());
        assert!(validate_provider_base_url("https://api.example.com/v1").is_ok());
        for invalid in ["http://api.example.com/v1", "http://", "ftp://example.com", "https://user:pass@example.com", "https://example.com/?key=x", "https://example.com/#fragment"] {
            assert!(validate_provider_base_url(invalid).is_err(), "accepted {invalid}");
        }
    }

    #[test]
    fn minecraft_server_address_requires_a_valid_host_and_port() {
        assert!(validate_minecraft_server("localhost").is_ok());
        assert!(validate_minecraft_server("play.example.net:25565").is_ok());
        assert!(validate_minecraft_server("192.168.1.20:25566").is_ok());
        for invalid in ["", "https://example.com", "example.com/path", "host:0", "host:65536", "host:nope", "host name:25565", "::1:25565"] {
            assert!(validate_minecraft_server(invalid).is_err(), "accepted {invalid}");
        }
    }

    #[test]
    fn trusted_chat_allowlist_is_empty_by_default_and_validates_usernames() {
        assert!(parse_trusted_chat_players("").unwrap().is_empty());
        assert_eq!(parse_trusted_chat_players("Owner, Friend_2").unwrap(), vec!["Owner".to_owned(), "Friend_2".to_owned()]);
        assert!(parse_trusted_chat_players("Owner,owner").is_err());
        assert!(parse_trusted_chat_players("bad-name").is_err());
        let too_many = (0..33).map(|index| format!("Player{index:02}")).collect::<Vec<_>>().join(",");
        assert!(parse_trusted_chat_players(&too_many).is_err());
    }

    #[test]
    fn api_tokens_must_have_sufficient_entropy_length_and_safe_header_characters() {
        assert!(validate_api_token("0123456789abcdef0123456789abcdef").is_ok());
        assert!(validate_api_token("short").is_err());
        assert!(validate_api_token("0123456789abcdef0123456789abcde ").is_err());
        assert!(validate_api_token("0123456789abcdef0123456789abcde\n").is_err());
    }
}
