#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use keyring::Entry;
use serde::{Deserialize, Serialize};
use std::{
    fs,
    fs::OpenOptions,
    io::{Read, Seek, SeekFrom, Write},
    net::{SocketAddr, TcpStream},
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::Mutex,
    thread,
    time::{Duration, Instant},
};
use tauri::{AppHandle, Manager, State};
use url::Url;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

struct RuntimeProcess(Mutex<Option<Child>>);

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RuntimeLaunch {
    token: String,
    api_port: u16,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SavedSetup {
    server: String, username: String, accounts: String, auth: String, microsoft_email: String, trusted_players: String,
    provider: String, model: String, base_url: String, azure_api_version: String, autonomy: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Setup {
    server: String,
    username: String,
    accounts: String,
    auth: String,
    microsoft_email: String,
    trusted_players: String,
    provider: String,
    model: String,
    api_key: String,
    base_url: String,
    azure_api_version: String,
    autonomy: String,
}

fn app_data(app: &AppHandle) -> Result<PathBuf, String> {
    let path = app.path().app_data_dir().map_err(|e| e.to_string())?;
    fs::create_dir_all(&path).map_err(|e| e.to_string())?;
    #[cfg(unix)]
    fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).map_err(|e| e.to_string())?;
    Ok(path)
}

fn valid_http_base_url(value: &str) -> bool {
    Url::parse(value.trim()).ok().is_some_and(|url| {
        let host = url.host_str().unwrap_or_default();
        let loopback_host = host.eq_ignore_ascii_case("localhost")
            || host.parse::<std::net::IpAddr>().is_ok_and(|address| address.is_loopback());
        ["http", "https"].contains(&url.scheme())
            && !host.is_empty()
            && (url.scheme() == "https" || loopback_host)
            && url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none()
    })
}

fn supported_provider(provider: &str) -> bool {
    matches!(provider,
        "openai" | "openai-compatible" | "anthropic" | "google" | "ollama"
        | "deepseek" | "groq" | "cerebras" | "mistral" | "openrouter"
        | "xai" | "grok" | "qwen" | "huggingface" | "hyperbolic" | "novita"
        | "mercury" | "lmstudio" | "vllm" | "azure" | "replicate"
    )
}

fn provider_requires_api_key(provider: &str) -> bool {
    matches!(provider,
        "openai" | "anthropic" | "google" | "deepseek" | "groq" | "cerebras"
        | "mistral" | "openrouter" | "xai" | "grok" | "qwen" | "huggingface"
        | "hyperbolic" | "novita" | "mercury" | "azure" | "replicate"
    )
}

fn default_provider_base_url(provider: &str) -> &'static str {
    match provider {
        "ollama" => "http://127.0.0.1:11434/v1",
        "lmstudio" => "http://127.0.0.1:1234/v1",
        "vllm" => "http://127.0.0.1:8000/v1",
        "openai-compatible" | "azure" => "",
        "replicate" => "https://api.replicate.com/v1",
        "deepseek" => "https://api.deepseek.com/v1",
        "groq" => "https://api.groq.com/openai/v1",
        "cerebras" => "https://api.cerebras.ai/v1",
        "mistral" => "https://api.mistral.ai/v1",
        "openrouter" => "https://openrouter.ai/api/v1",
        "xai" | "grok" => "https://api.x.ai/v1",
        "qwen" => "https://dashscope.aliyuncs.com/compatible-mode/v1",
        "huggingface" => "https://router.huggingface.co/v1",
        "hyperbolic" => "https://api.hyperbolic.xyz/v1",
        "novita" => "https://api.novita.ai/v3/openai",
        "mercury" => "https://api.inceptionlabs.ai/v1",
        _ => "https://api.openai.com/v1",
    }
}

fn validate_trusted_players(raw: &str) -> Result<String, String> {
    let names = raw.split(',').map(str::trim).filter(|name| !name.is_empty()).collect::<Vec<_>>();
    if names.len() > 32 {
        return Err("Enter no more than 32 trusted Minecraft player usernames.".into());
    }
    let mut unique = std::collections::HashSet::new();
    for name in &names {
        if !(3..=16).contains(&name.len())
            || !name.chars().all(|character| character.is_ascii_alphanumeric() || character == '_')
            || !unique.insert(name.to_ascii_lowercase()) {
            return Err("Trusted player names must be unique Minecraft usernames (3-16 letters, numbers, or underscores).".into());
        }
    }
    Ok(names.join(","))
}

fn valid_minecraft_server(value: &str) -> bool {
    let value = value.trim();
    if value.is_empty() || value.chars().any(char::is_whitespace)
        || value.chars().any(|character| character.is_control() || matches!(character, '/' | '\\' | '?' | '#' | '@')) {
        return false;
    }
    let host = match value.rsplit_once(':') {
        Some((host, port)) if host.contains(':') || !port.parse::<u16>().is_ok_and(|port| port > 0) => return false,
        Some((host, _)) => host,
        None => value,
    };
    valid_server_host(host)
}

fn valid_server_host(host: &str) -> bool {
    if host.parse::<std::net::Ipv4Addr>().is_ok() {
        return true;
    }
    if host.is_empty() || host.len() > 253 {
        return false;
    }
    host.split('.').all(|label| {
        !label.is_empty()
            && label.len() <= 63
            && label.as_bytes().first().is_some_and(u8::is_ascii_alphanumeric)
            && label.as_bytes().last().is_some_and(u8::is_ascii_alphanumeric)
            && label.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    })
}

fn dotenv_value(value: &str) -> String {
    let safe = value.chars().filter(|character| !character.is_control()).collect::<String>();
    format!("\"{}\"", safe.replace('\\', "\\\\").replace('\"', "\\\""))
}

fn validate_offline_accounts(raw: &str, fallback: &str) -> Result<String, String> {
    let source = if raw.trim().is_empty() { fallback.trim() } else { raw.trim() };
    let parsed = source.split(',').map(str::trim).filter(|name| !name.is_empty()).collect::<Vec<_>>();
    if parsed.is_empty() || parsed.len() > 8 {
        return Err("Enter between 1 and 8 bot usernames.".into());
    }
    let mut unique = std::collections::HashSet::new();
    for name in &parsed {
        if !(3..=16).contains(&name.len())
            || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
            || !unique.insert(name.to_ascii_lowercase()) {
            return Err("Bot usernames must be unique, 3-16 letters, numbers, or underscores.".into());
        }
    }
    Ok(parsed.join(","))
}

#[tauri::command]
fn load_setup(app: AppHandle) -> Result<Option<SavedSetup>, String> {
    let data = app_data(&app)?;
    let path = data.join(".env");
    if !path.exists() { return Ok(None); }
    let values = dotenvy::from_path_iter(path).map_err(|e| e.to_string())?
        .collect::<Result<std::collections::HashMap<_, _>, _>>().map_err(|e| e.to_string())?;
    let get = |key: &str| values.get(key).cloned().unwrap_or_default();
    Ok(Some(SavedSetup {
        server: get("MINDCRAFT_MC_SERVER"),
        username: get("MINDCRAFT_MC_USERNAME"),
        accounts: get("MINDCRAFT_MC_ACCOUNTS"),
        auth: get("MINDCRAFT_MC_AUTH"),
        microsoft_email: get("MINDCRAFT_MICROSOFT_EMAIL"),
        trusted_players: get("MINDCRAFT_TRUSTED_PLAYERS"),
        provider: get("MINDCRAFT_PROVIDER"),
        model: get("MINDCRAFT_MODEL"),
        base_url: get("MINDCRAFT_BASE_URL"),
        azure_api_version: get("MINDCRAFT_AZURE_API_VERSION"),
        autonomy: get("MINDCRAFT_MODE"),
    }))
}

#[tauri::command]
fn save_setup(app: AppHandle, setup: Setup) -> Result<(), String> {
    if !valid_minecraft_server(&setup.server) {
        return Err("Enter a valid Minecraft host name or host:port without a URL scheme, path, or whitespace. IPv6 literals are not supported by the current Azalea parser.".into());
    }
    if setup.username.trim().is_empty() {
        return Err("Enter the Minecraft bot username.".into());
    }
    if !["offline", "microsoft"].contains(&setup.auth.as_str()) {
        return Err("Choose a supported Minecraft sign-in method.".into());
    }
    let accounts = validate_offline_accounts(&setup.accounts, &setup.username)?;
    if setup.auth == "microsoft" {
        let emails = setup.microsoft_email.split(',').map(str::trim).filter(|email| !email.is_empty()).collect::<Vec<_>>();
        if emails.len() != accounts.split(',').count() {
            return Err("Enter one comma-separated Microsoft account email for each bot username.".into());
        }
        let mut unique_emails = std::collections::HashSet::new();
        for email in emails {
            if email.len() > 254 || email.chars().any(char::is_whitespace)
                || !email.split_once('@').is_some_and(|(local, domain)| !local.is_empty() && domain.contains('.') && !domain.starts_with('.') && !domain.ends_with('.'))
                || !unique_emails.insert(email.to_ascii_lowercase()) {
                return Err("Microsoft account emails must be valid, unique addresses.".into());
            }
        }
    }
    let trusted_players = validate_trusted_players(&setup.trusted_players)?;
    if !supported_provider(&setup.provider) {
        return Err("Choose a supported AI provider.".into());
    }
    if setup.model.trim().is_empty() {
        return Err("Enter an AI model name.".into());
    }
    if ["openai-compatible", "azure"].contains(&setup.provider.as_str()) && setup.base_url.trim().is_empty() {
        return Err(if setup.provider == "azure" {
            "Enter the Azure OpenAI resource endpoint URL.".into()
        } else {
            "Enter the OpenAI-compatible API base URL.".into()
        });
    }
    if setup.provider == "azure" && (setup.azure_api_version.trim().is_empty() || setup.azure_api_version.len() > 40
        || !setup.azure_api_version.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.'))) {
        return Err("Azure API version must contain 1-40 ASCII letters, numbers, dots, or hyphens.".into());
    }
    if ["ollama", "openai-compatible", "azure"].contains(&setup.provider.as_str())
        && !setup.base_url.trim().is_empty()
        && !valid_http_base_url(&setup.base_url) {
        return Err("Use HTTPS for remote AI endpoints; HTTP is allowed only for localhost/loopback URLs without credentials, query, or fragment.".into());
    }
    if !["assisted", "balanced", "autonomous"].contains(&setup.autonomy.as_str()) {
        return Err("Choose an autonomy level.".into());
    }
    let data = app_data(&app)?;
    let key_account = format!("ai-provider-api-key-{}", setup.provider);
    if setup.api_key.trim().is_empty() {
        if provider_requires_api_key(&setup.provider) {
            let entry = Entry::new("com.mindcraft.minecraft", &key_account)
                .map_err(|error| format!("The operating system credential store is unavailable: {error}"))?;
            if entry.get_password().is_err() {
                return Err("This AI provider requires an API key. Enter it once to store it in the operating system credential store.".into());
            }
        } else if let Ok(entry) = Entry::new("com.mindcraft.minecraft", &key_account) {
            // Never carry a previously stored hosted-provider key into a
            // user-configured local/OpenAI-compatible endpoint unless the
            // user enters that key explicitly for this setup.
            let _ = entry.delete_credential();
        }
    } else {
        let entry = Entry::new("com.mindcraft.minecraft", &key_account)
            .map_err(|error| format!("The operating system credential store is unavailable: {error}"))?;
        entry.set_password(setup.api_key.trim()).map_err(|e| e.to_string())?;
    }

    let token = uuid::Uuid::new_v4().to_string();
    let database = data.join("mindcraft.sqlite3");
    let values = [
        ("MINDCRAFT_MC_SERVER", setup.server.trim().to_owned()),
        ("MINDCRAFT_MC_USERNAME", setup.username.trim().to_owned()),
        ("MINDCRAFT_MC_ACCOUNTS", accounts),
        ("MINDCRAFT_MC_AUTH", setup.auth.clone()),
        ("MINDCRAFT_MICROSOFT_EMAIL", setup.microsoft_email.trim().to_owned()),
        ("MINDCRAFT_TRUSTED_PLAYERS", trusted_players),
        ("MINDCRAFT_PROVIDER", setup.provider.clone()),
        ("MINDCRAFT_MODEL", setup.model.trim().to_owned()),
        ("MINDCRAFT_BASE_URL", match setup.provider.as_str() {
            "openai-compatible" => setup.base_url.trim().to_owned(),
            "azure" => setup.base_url.trim().to_owned(),
            "ollama" if !setup.base_url.trim().is_empty() && valid_http_base_url(&setup.base_url) => setup.base_url.trim().to_owned(),
            provider => default_provider_base_url(provider).to_owned(),
        }),
        ("MINDCRAFT_AZURE_API_VERSION", if setup.azure_api_version.trim().is_empty() { "2024-10-21".into() } else { setup.azure_api_version.trim().to_owned() }),
        ("MINDCRAFT_MODE", setup.autonomy.clone()),
        ("MINDCRAFT_BIND", "127.0.0.1:8787".into()),
        ("MINDCRAFT_API_TOKEN", token),
        ("MINDCRAFT_DATABASE", database.to_string_lossy().into_owned()),
    ];
    let contents = values.iter().map(|(key, value)| format!("{key}={}\n", dotenv_value(value))).collect::<String>();
    let env_path = data.join(".env");
    fs::write(&env_path, contents).map_err(|e| e.to_string())?;
    #[cfg(unix)]
    fs::set_permissions(&env_path, fs::Permissions::from_mode(0o600)).map_err(|e| e.to_string())?;
    Ok(())
}

#[tauri::command]
fn start_runtime(app: AppHandle, process: State<'_, RuntimeProcess>) -> Result<RuntimeLaunch, String> {
    let mut child = process.0.lock().map_err(|_| "Runtime process lock is unavailable".to_string())?;
    if child.as_mut().is_some_and(|running| running.try_wait().ok().flatten().is_none()) {
        return Err("Mindcraft is already running.".into());
    }
    *child = None;

    let data = app_data(&app)?;
    #[cfg(target_os = "windows")]
    let runtime_name = "mindcraft.exe";
    #[cfg(not(target_os = "windows"))]
    let runtime_name = "mindcraft";
    let runtime = app.path().resource_dir().map_err(|e| e.to_string())?.join("binaries").join(runtime_name);
    if !runtime.is_file() {
        return Err(format!("The bundled Rust runtime was not found at {}. The release packager must place the compiled mindcraft binary in the app resources.", runtime.display()));
    }
    let log_path = data.join("runtime.log");
    if fs::metadata(&log_path).map(|meta| meta.len() > 2 * 1024 * 1024).unwrap_or(false) {
        let previous = data.join("runtime.log.1");
        let _ = fs::remove_file(&previous);
        fs::rename(&log_path, previous).map_err(|e| e.to_string())?;
    }
    let log = OpenOptions::new().create(true).append(true).open(&log_path).map_err(|e| e.to_string())?;
    #[cfg(unix)]
    fs::set_permissions(&log_path, fs::Permissions::from_mode(0o600)).map_err(|e| e.to_string())?;
    let log_stderr = log.try_clone().map_err(|e| e.to_string())?;
    let port_probe = std::net::TcpListener::bind(("127.0.0.1", 0)).map_err(|e| format!("Could not reserve a local dashboard port: {e}"))?;
    let api_port = port_probe.local_addr().map_err(|e| e.to_string())?.port();
    drop(port_probe);

    let env_file = fs::read_to_string(data.join(".env")).map_err(|e| e.to_string())?;
    let provider = env_file.lines()
        .find_map(|line| line.strip_prefix("MINDCRAFT_PROVIDER=\"").and_then(|v| v.strip_suffix('\"')))
        .ok_or("App setup is incomplete; save setup again.")?;
    let token = env_file.lines()
        .find_map(|line| line.strip_prefix("MINDCRAFT_API_TOKEN=\"").and_then(|v| v.strip_suffix('\"')))
        .ok_or("App setup is incomplete; save setup again.")?
        .to_owned();

    let mut command = Command::new(runtime);
    command.current_dir(&data).arg("serve").stdin(Stdio::null()).stdout(Stdio::from(log)).stderr(Stdio::from(log_stderr));
    // The saved app setup is authoritative. Prevent unrelated shell or launch-agent
    // MINDCRAFT_* variables from silently overriding it or leaking stale API keys.
    for key in [
        "MINDCRAFT_MC_SERVER", "MINDCRAFT_MC_USERNAME", "MINDCRAFT_MC_ACCOUNTS",
        "MINDCRAFT_MC_AUTH", "MINDCRAFT_MICROSOFT_EMAIL", "MINDCRAFT_TRUSTED_PLAYERS", "MINDCRAFT_PROVIDER",
        "MINDCRAFT_MODEL", "MINDCRAFT_API_KEY", "MINDCRAFT_BASE_URL", "MINDCRAFT_AZURE_API_VERSION", "MINDCRAFT_MODE",
        "MINDCRAFT_BIND", "MINDCRAFT_API_TOKEN", "MINDCRAFT_DATABASE",
        "MINDCRAFT_AZALEA_AUTH_CACHE",
    ] {
        command.env_remove(key);
    }
    command.env("MINDCRAFT_BIND", format!("127.0.0.1:{api_port}"));
    command.env("MINDCRAFT_AZALEA_AUTH_CACHE", data.join("azalea-auth.json"));
    let key_account = format!("ai-provider-api-key-{provider}");
    if let Ok(entry) = Entry::new("com.mindcraft.minecraft", &key_account) {
        if let Ok(key) = entry.get_password() {
            command.env("MINDCRAFT_API_KEY", key);
        }
    }

    let spawned = command.spawn().map_err(|e| format!("Could not start the Rust runtime: {e}"))?;
    *child = Some(spawned);

    // Do not report a successful launch until the child has actually bound
    // its API port and accepted the app's private bearer token. The public
    // health route alone could produce a false positive if another local
    // process wins the brief port-allocation race.
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let Some(running) = child.as_mut() else {
            return Err("The Rust runtime process disappeared during startup.".into());
        };
        if let Some(status) = running.try_wait().map_err(|error| error.to_string())? {
            *child = None;
            return Err(format!(
                "The Rust runtime exited during startup ({status}). Inspect {} for the startup error.",
                log_path.display()
            ));
        }

        let address = SocketAddr::from(([127, 0, 0, 1], api_port));
        if let Ok(mut stream) = TcpStream::connect_timeout(&address, Duration::from_millis(200)) {
            let _ = stream.set_read_timeout(Some(Duration::from_millis(500)));
            let request = format!(
                "GET /api/status HTTP/1.1\r\nHost: 127.0.0.1\r\nAuthorization: Bearer {token}\r\nConnection: close\r\n\r\n"
            );
            if stream.write_all(request.as_bytes()).is_ok() {
                let mut response = [0_u8; 128];
                if let Ok(read) = stream.read(&mut response) {
                    let status_line = String::from_utf8_lossy(&response[..read]);
                    if status_line.starts_with("HTTP/1.1 200") || status_line.starts_with("HTTP/1.0 200") {
                        return Ok(RuntimeLaunch { token, api_port });
                    }
                }
            }
        }

        if Instant::now() >= deadline {
            if let Some(mut running) = child.take() {
                let _ = running.kill();
                let _ = running.wait();
            }
            return Err(format!(
                "The Rust runtime did not become healthy within 15 seconds. Its startup log is at {}.",
                log_path.display()
            ));
        }
        thread::sleep(Duration::from_millis(100));
    }
}

#[tauri::command]
fn stop_runtime(process: State<'_, RuntimeProcess>) -> Result<(), String> {
    let mut child = process.0.lock().map_err(|_| "Runtime process lock is unavailable".to_string())?;
    if let Some(mut running) = child.take() {
        running.kill().map_err(|e| e.to_string())?;
        let _ = running.wait();
    }
    Ok(())
}


#[tauri::command]
fn runtime_log(app: AppHandle) -> Result<String, String> {
    let data = app_data(&app)?;
    let path = data.join("runtime.log");
    if !path.exists() { return Ok(String::new()); }
    let mut file = fs::File::open(path).map_err(|e| e.to_string())?;
    let length = file.metadata().map_err(|e| e.to_string())?.len();
    file.seek(SeekFrom::Start(length.saturating_sub(64 * 1024))).map_err(|e| e.to_string())?;
    let mut bytes = Vec::with_capacity(64 * 1024);
    file.read_to_end(&mut bytes).map_err(|e| e.to_string())?;
    let contents = String::from_utf8_lossy(&bytes);
    let tail = contents.lines().rev().take(80).collect::<Vec<_>>();
    Ok(tail.into_iter().rev().collect::<Vec<_>>().join("\n"))
}

#[tauri::command]
fn runtime_running(process: State<'_, RuntimeProcess>) -> Result<bool, String> {
    let mut child = process.0.lock().map_err(|_| "Runtime process lock is unavailable".to_string())?;
    match child.as_mut() {
        Some(running) => match running.try_wait().map_err(|e| e.to_string())? {
            Some(_) => { *child = None; Ok(false) }
            None => Ok(true),
        },
        None => Ok(false),
    }
}

fn main() {
    tauri::Builder::default()
        .manage(RuntimeProcess(Mutex::new(None)))
        .invoke_handler(tauri::generate_handler![load_setup, save_setup, start_runtime, stop_runtime, runtime_running, runtime_log])
        .build(tauri::generate_context!())
        .expect("error while building Mindcraft desktop")
        .run(|app, event| {
            if matches!(event, tauri::RunEvent::Exit) {
                if let Some(process) = app.try_state::<RuntimeProcess>() {
                    if let Ok(mut child) = process.0.lock() {
                        if let Some(mut running) = child.take() {
                            let _ = running.kill();
                            let _ = running.wait();
                        }
                    }
                }
            }
        });
}


#[cfg(test)]
mod tests {
    use super::validate_offline_accounts;

    #[test]
    fn minecraft_server_addresses_are_validated_before_saving() {
        assert!(super::valid_minecraft_server("localhost"));
        assert!(super::valid_minecraft_server("play.example.net:25565"));
        assert!(super::valid_minecraft_server("192.168.1.20:25566"));
        for invalid in ["", "https://example.com", "example.com/path", "host:0", "host:65536", "host:nope", "host name:25565", "::1:25565", "a..b", "-host", "host-", "bad_.host"] {
            assert!(!super::valid_minecraft_server(invalid), "accepted {invalid}");
        }
    }

    #[test]
    fn provider_base_urls_require_safe_http_endpoints() {
        assert!(super::valid_http_base_url("http://127.0.0.1:11434/v1"));
        assert!(super::valid_http_base_url("http://localhost:11434/v1"));
        assert!(super::valid_http_base_url("https://api.example.com/v1"));
        for invalid in ["http://", "http://api.example.com/v1", "ftp://example.com", "https://user:pass@example.com", "https://example.com/?key=secret", "https://example.com/#fragment"] {
            assert!(!super::valid_http_base_url(invalid), "accepted {invalid}");
        }
    }

    #[test]
    fn dotenv_values_cannot_inject_additional_environment_lines() {
        assert_eq!(super::dotenv_value("safe\r\nMINDCRAFT_MODE=autonomous"), "\"safeMINDCRAFT_MODE=autonomous\"");
        assert_eq!(super::dotenv_value("quoted\"value"), "\"quoted\\\"value\"");
    }

    #[test]
    fn trusted_chat_players_allow_empty_or_a_bounded_unique_username_list() {
        assert_eq!(super::validate_trusted_players("").unwrap(), "");
        assert_eq!(super::validate_trusted_players("Owner, Friend_2").unwrap(), "Owner,Friend_2");
        assert!(super::validate_trusted_players("Owner,owner").is_err());
        assert!(super::validate_trusted_players("bad-name").is_err());
        assert!(super::validate_trusted_players(&(0..33).map(|i| format!("Player{i:02}")).collect::<Vec<_>>().join(",")).is_err());
    }

    #[test]
    fn offline_team_accepts_one_to_eight_unique_valid_names() {
        assert_eq!(validate_offline_accounts("Mindcraft, Scout_2, Builder", "fallback").unwrap(), "Mindcraft,Scout_2,Builder");
        assert_eq!(validate_offline_accounts("", "Mindcraft").unwrap(), "Mindcraft");
        assert!(validate_offline_accounts("Mindcraft,mindcraft", "fallback").is_err());
        assert!(validate_offline_accounts("ab", "fallback").is_err());
        assert!(validate_offline_accounts("Mindcraft, Scout, Builder, Crafter, Guardian, One, Two, Three, Four", "fallback").is_err());
    }
}
