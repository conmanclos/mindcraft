#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use keyring::Entry;
use serde::{Deserialize, Serialize};
use std::{
    fs,
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::Mutex,
};
use tauri::{AppHandle, Manager, State};

struct RuntimeProcess(Mutex<Option<Child>>);

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SavedSetup {
    server: String, username: String, auth: String, microsoft_email: String,
    provider: String, model: String, base_url: String, autonomy: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Setup {
    server: String,
    username: String,
    auth: String,
    microsoft_email: String,
    provider: String,
    model: String,
    api_key: String,
    base_url: String,
    autonomy: String,
}

fn app_data(app: &AppHandle) -> Result<PathBuf, String> {
    let path = app.path().app_data_dir().map_err(|e| e.to_string())?;
    fs::create_dir_all(&path).map_err(|e| e.to_string())?;
    Ok(path)
}

fn dotenv_value(value: &str) -> String {
    format!("\"{}\"", value.replace('\\', "\\\\").replace('\"', "\\\"").replace('\n', ""))
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
        auth: get("MINDCRAFT_MC_AUTH"),
        microsoft_email: get("MINDCRAFT_MICROSOFT_EMAIL"),
        provider: get("MINDCRAFT_PROVIDER"),
        model: get("MINDCRAFT_MODEL"),
        base_url: get("MINDCRAFT_BASE_URL"),
        autonomy: get("MINDCRAFT_MODE"),
    }))
}

#[tauri::command]
fn save_setup(app: AppHandle, setup: Setup) -> Result<(), String> {
    if setup.server.trim().is_empty() {
        return Err("Enter your Minecraft server address.".into());
    }
    if setup.username.trim().is_empty() {
        return Err("Enter the Minecraft bot username.".into());
    }
    if !["offline", "microsoft"].contains(&setup.auth.as_str()) {
        return Err("Choose a supported Minecraft sign-in method.".into());
    }
    if setup.auth == "microsoft" && setup.microsoft_email.trim().is_empty() {
        return Err("Enter the Microsoft account email.".into());
    }
    if !["openai", "openai-compatible", "anthropic", "google", "ollama"].contains(&setup.provider.as_str()) {
        return Err("Choose a supported AI provider.".into());
    }
    if setup.model.trim().is_empty() {
        return Err("Enter an AI model name.".into());
    }
    if !["assisted", "balanced", "autonomous"].contains(&setup.autonomy.as_str()) {
        return Err("Choose an autonomy level.".into());
    }
    if setup.provider != "ollama" && setup.api_key.trim().is_empty() {
        return Err("This AI provider requires an API key.".into());
    }

    let data = app_data(&app)?;
    let entry = Entry::new("com.mindcraft.minecraft", "ai-provider-api-key").map_err(|e| e.to_string())?;
    if setup.api_key.trim().is_empty() {
        let _ = entry.delete_credential();
    } else {
        entry.set_password(setup.api_key.trim()).map_err(|e| e.to_string())?;
    }

    let token = uuid::Uuid::new_v4().to_string();
    let database = data.join("mindcraft.sqlite3");
    let values = [
        ("MINDCRAFT_MC_SERVER", setup.server.trim().to_owned()),
        ("MINDCRAFT_MC_USERNAME", setup.username.trim().to_owned()),
        ("MINDCRAFT_MC_AUTH", setup.auth.clone()),
        ("MINDCRAFT_MICROSOFT_EMAIL", setup.microsoft_email.trim().to_owned()),
        ("MINDCRAFT_PROVIDER", setup.provider.clone()),
        ("MINDCRAFT_MODEL", setup.model.trim().to_owned()),
        ("MINDCRAFT_BASE_URL", if setup.base_url.trim().is_empty() { "http://127.0.0.1:11434/v1".into() } else { setup.base_url.trim().to_owned() }),
        ("MINDCRAFT_MODE", setup.autonomy.clone()),
        ("MINDCRAFT_BIND", "127.0.0.1:8787".into()),
        ("MINDCRAFT_API_TOKEN", token),
        ("MINDCRAFT_DATABASE", database.to_string_lossy().into_owned()),
    ];
    let contents = values.iter().map(|(key, value)| format!("{key}={}\n", dotenv_value(value))).collect::<String>();
    fs::write(data.join(".env"), contents).map_err(|e| e.to_string())?;
    Ok(())
}

#[tauri::command]
fn start_runtime(app: AppHandle, process: State<'_, RuntimeProcess>) -> Result<String, String> {
    let mut child = process.0.lock().map_err(|_| "Runtime process lock is unavailable".to_string())?;
    if child.as_mut().is_some_and(|running| running.try_wait().ok().flatten().is_none()) {
        return Err("Mindcraft is already running.".into());
    }
    *child = None;

    let data = app_data(&app)?;
    let runtime = app.path().resource_dir().map_err(|e| e.to_string())?.join("mindcraft");
    if !runtime.is_file() {
        return Err(format!("The bundled Rust runtime was not found at {}. The release packager must place the compiled mindcraft binary in the app resources.", runtime.display()));
    }
    let mut command = Command::new(runtime);
    command.current_dir(&data).arg("serve").stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null());
    if let Ok(entry) = Entry::new("com.mindcraft.minecraft", "ai-provider-api-key") {
        if let Ok(key) = entry.get_password() {
            command.env("MINDCRAFT_API_KEY", key);
        }
    }
    let spawned = command.spawn().map_err(|e| format!("Could not start the Rust runtime: {e}"))?;
    *child = Some(spawned);

    let env_file = fs::read_to_string(data.join(".env")).map_err(|e| e.to_string())?;
    let token = env_file.lines().find_map(|line| line.strip_prefix("MINDCRAFT_API_TOKEN=\"").and_then(|v| v.strip_suffix('\"'))).ok_or("App setup is incomplete; save setup again.")?;
    Ok(token.to_owned())
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
        .invoke_handler(tauri::generate_handler![load_setup, save_setup, start_runtime, stop_runtime, runtime_running])
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
