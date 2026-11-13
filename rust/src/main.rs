mod api;
mod brain;
mod config;
mod minecraft;
mod reflex;
mod store;

use anyhow::{bail, Result};
use clap::{Parser, Subcommand};
use reqwest::Method;
use serde_json::{json, Value};
use std::{net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr}, sync::Arc};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "mindcraft", version, about = "Native Rust Minecraft AI teammate")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Start the shared runtime, dashboard, API, and optional Minecraft connection.
    Serve,
    /// Chat through the running runtime so history and persistent memory are shared.
    Chat { prompt: String },
    /// Send a one-off prompt directly to the configured AI provider without runtime history.
    DirectChat { prompt: String },
    /// Print a safe configuration and dependency diagnostic.
    Doctor,
    /// Read live runtime and Minecraft status from the local dashboard API.
    Status,
    /// List persisted strategic goals and their current states.
    Goals,
    /// List persistent semantic, episodic, preference, and world memories.
    Memories,
    /// Save or update a persistent memory record.
    Remember {
        kind: String,
        subject: String,
        content: String,
    },
    /// Delete a persistent memory record by ID.
    ForgetMemory { memory_id: i64 },
    /// List the tasks belonging to one strategic goal.
    GoalTasks { goal_id: i64 },
    /// List tasks awaiting human result verification.
    ReviewTasks,
    /// Claim the next eligible task for an agent.
    ClaimTask { agent_id: String, role: String },
    /// Renew an agent's active task lease.
    HeartbeatTask { task_id: i64, agent_id: String },
    /// Mark a claimed or reviewed task complete after verifying its in-game result.
    CompleteTask { task_id: i64, agent_id: String },
    /// Release a task back to the shared queue.
    ReleaseTask { task_id: i64, agent_id: String },
    /// Report an execution failure and schedule a bounded retry.
    FailTask { task_id: i64, agent_id: String, reason: String },
    /// Issue a short-lived credential for an external specialist worker.
    IssueAgentCredential { agent_id: String },
    /// Revoke an external specialist worker credential.
    RevokeAgentCredential { agent_id: String },
    /// Ask the runtime planner to propose or autonomously dispatch an action.
    Plan {
        message: String,
        #[arg(long)]
        agent: Option<String>,
    },
    /// Submit a bounded mixed-material schematic to the native builder.
    Build { schematic_json: String },
    /// Execute an explicitly approved action supplied as a JSON object.
    Execute {
        action_json: String,
        #[arg(long)]
        agent: Option<String>,
    },
    /// Create a strategic goal and let the planner decompose it into tasks.
    CreateGoal { description: String },
    /// Cancel an active strategic goal and revoke its queued work.
    CancelGoal { goal_id: i64 },
    /// Retry a failed strategic goal without repeating verified completed tasks.
    RetryGoal { goal_id: i64 },
}

fn local_api_url(config: &config::Config) -> Result<String> {
    let bind = config.bind;
    let ip = match bind.ip() {
        IpAddr::V4(address) if address.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V6(address) if address.is_unspecified() => IpAddr::V6(Ipv6Addr::LOCALHOST),
        address if address.is_loopback() => address,
        _ => bail!("CLI API commands are restricted to loopback-bound runtimes; use the authenticated dashboard for a non-loopback bind"),
    };
    let scheme = if config.tls_cert_path.is_some() { "https" } else { "http" };
    Ok(format!("{scheme}://{}/api", SocketAddr::new(ip, bind.port())))
}

async fn api_request(
    config: &config::Config,
    method: Method,
    path: &str,
    body: Option<Value>,
) -> Result<Value> {
    if config.api_token_was_generated {
        bail!("CLI API commands require MINDCRAFT_API_TOKEN to be set to the token used by the running runtime");
    }
    let base = local_api_url(config)?;
    let mut client_builder = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(60))
        .redirect(reqwest::redirect::Policy::none());
    if let Some(cert_path) = &config.tls_cert_path {
        let certificate_pem = std::fs::read(cert_path)?;
        let certificate = reqwest::Certificate::from_pem(&certificate_pem)?;
        client_builder = client_builder.add_root_certificate(certificate);
    }
    let client = client_builder.build()?;
    let mut request = client.request(method, format!("{base}{path}"))
        .bearer_auth(&config.api_token);
    if let Some(body) = body {
        request = request.json(&body);
    }
    let response = request.send().await?;
    let status = response.status();
    let text = response.text().await?;
    if !status.is_success() {
        bail!("local runtime API returned {status}: {text}");
    }
    if text.trim().is_empty() {
        return Ok(Value::Null);
    }
    Ok(serde_json::from_str(&text)?)
}

fn valid_agent_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= 64
        && value.chars().all(|character| character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-'))
}

fn print_json(value: &Value) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let cli = Cli::parse();
    let config = Arc::new(config::Config::from_env()?);

    match cli.command.unwrap_or(Command::Serve) {
        Command::Serve => {
            let store = Arc::new(store::Store::open(&config.database)?);
            let brain = Arc::new(brain::Brain::new(config.clone()));
            let status = Arc::new(api::RuntimeStatus::default());

            tracing::info!(bind = %config.bind, mode = %config.mode, "Mindcraft runtime starting");
            if config.api_token_was_generated {
                tracing::warn!(token = %config.api_token, "Generated one-time dashboard API token; copy it now");
            }

            if let Some(server) = config.minecraft_server.clone() {
                let bot_brain = brain.clone();
                let bot_status = status.clone();
                let bot_store = store.clone();
                let bot_accounts = config.minecraft_accounts.clone();
                let bot_auth = config.minecraft_auth.clone();
                let microsoft_email = config.microsoft_email.clone();
                let trusted_chat_players = config.trusted_chat_players.clone();
                tokio::select! {
                    result = api::serve(config.clone(), brain, store, status) => result?,
                    result = minecraft::connect(server, bot_accounts, bot_auth, microsoft_email, trusted_chat_players, bot_brain, bot_status, bot_store) => result?,
                    signal = tokio::signal::ctrl_c() => {
                        signal?;
                        tracing::info!("shutdown requested; stopping the Minecraft swarm and dashboard");
                    }
                }
            } else {
                tracing::warn!("MINDCRAFT_MC_SERVER is unset; dashboard/API will start without a Minecraft connection");
                tokio::select! {
                    result = api::serve(config, brain, store, status) => result?,
                    signal = tokio::signal::ctrl_c() => {
                        signal?;
                        tracing::info!("shutdown requested; stopping the dashboard");
                    }
                }
            }
        }
        Command::Chat { prompt } => {
            let response = api_request(&config, Method::POST, "/chat", Some(json!({"message": prompt}))).await?;
            println!("{}", response.get("reply").and_then(Value::as_str).unwrap_or_default());
        }
        Command::DirectChat { prompt } => {
            let brain = brain::Brain::new(config);
            let response = brain.reply(&prompt).await?;
            println!("{response}");
        }
        Command::Doctor => {
            println!("Mindcraft native runtime");
            println!("Minecraft target: 26.1 (Azalea 0.16.0+mc26.1)");
            println!("Provider: {}", config.provider);
            println!("Model: {}", config.model);
            println!("Minecraft server configured: {}", config.minecraft_server.is_some());
            println!("Minecraft auth: {}", config.minecraft_auth);
            println!("Trusted in-game command senders configured: {}", config.trusted_chat_players.len());
            println!("API bind: {}", config.bind);
            println!("API token configured: {}", !config.api_token.is_empty());
            println!("Database: {}", config.database.display());
            println!("Autonomy: {}", config.mode);
        }
        Command::Status => print_json(&api_request(&config, Method::GET, "/status", None).await?)?,
        Command::Goals => print_json(&api_request(&config, Method::GET, "/goals", None).await?)?,
        Command::Memories => print_json(&api_request(&config, Method::GET, "/memory", None).await?)?,
        Command::Remember { kind, subject, content } => {
            print_json(&api_request(&config, Method::POST, "/memory", Some(json!({
                "kind": kind, "subject": subject, "content": content
            }))).await?)?;
        }
        Command::ForgetMemory { memory_id } => {
            if memory_id <= 0 { bail!("memory_id must be positive"); }
            print_json(&api_request(&config, Method::DELETE, &format!("/memory/{memory_id}"), None).await?)?;
        }
        Command::GoalTasks { goal_id } => {
            if goal_id <= 0 { bail!("goal_id must be positive"); }
            print_json(&api_request(&config, Method::GET, &format!("/goals/{goal_id}/tasks"), None).await?)?;
        }
        Command::ReviewTasks => print_json(&api_request(&config, Method::GET, "/tasks/review", None).await?)?,
        Command::ClaimTask { agent_id, role } => {
            print_json(&api_request(&config, Method::POST, "/tasks/claim", Some(json!({
                "agent_id": agent_id, "role": role
            }))).await?)?;
        }
        Command::HeartbeatTask { task_id, agent_id } => {
            if task_id <= 0 { bail!("task_id must be positive"); }
            print_json(&api_request(&config, Method::POST, &format!("/tasks/{task_id}/heartbeat"), Some(json!({"agent_id": agent_id}))).await?)?;
        }
        Command::CompleteTask { task_id, agent_id } => {
            if task_id <= 0 { bail!("task_id must be positive"); }
            print_json(&api_request(&config, Method::POST, &format!("/tasks/{task_id}/complete"), Some(json!({"agent_id": agent_id}))).await?)?;
        }
        Command::ReleaseTask { task_id, agent_id } => {
            if task_id <= 0 { bail!("task_id must be positive"); }
            print_json(&api_request(&config, Method::POST, &format!("/tasks/{task_id}/release"), Some(json!({"agent_id": agent_id}))).await?)?;
        }
        Command::FailTask { task_id, agent_id, reason } => {
            if task_id <= 0 { bail!("task_id must be positive"); }
            print_json(&api_request(&config, Method::POST, &format!("/tasks/{task_id}/fail"), Some(json!({
                "agent_id": agent_id, "reason": reason
            }))).await?)?;
        }
        Command::IssueAgentCredential { agent_id } => {
            if !valid_agent_id(&agent_id) { bail!("agent_id must contain 1-64 ASCII letters, numbers, dots, underscores, or hyphens"); }
            print_json(&api_request(&config, Method::POST, &format!("/agents/{agent_id}/credential"), Some(json!({}))).await?)?;
        }
        Command::RevokeAgentCredential { agent_id } => {
            if !valid_agent_id(&agent_id) { bail!("agent_id must contain 1-64 ASCII letters, numbers, dots, underscores, or hyphens"); }
            print_json(&api_request(&config, Method::DELETE, &format!("/agents/{agent_id}/credential"), None).await?)?;
        }
        Command::Plan { message, agent } => {
            let mut body = json!({"message": message});
            if let Some(agent) = agent { body["agent"] = json!(agent); }
            print_json(&api_request(&config, Method::POST, "/plan", Some(body)).await?)?;
        }
        Command::Build { schematic_json } => {
            let schematic: Value = serde_json::from_str(&schematic_json)?;
            if !schematic.is_object() {
                bail!("schematic_json must be an object containing origin and blocks");
            }
            print_json(&api_request(&config, Method::POST, "/build", Some(schematic)).await?)?;
        }
        Command::Execute { action_json, agent } => {
            let action: Value = serde_json::from_str(&action_json)?;
            if !action.is_object() {
                bail!("action_json must be a JSON object matching the planner action schema");
            }
            let mut body = json!({"action": action});
            if let Some(agent) = agent { body["agent"] = json!(agent); }
            print_json(&api_request(&config, Method::POST, "/execute", Some(body)).await?)?;
        }
        Command::CreateGoal { description } => {
            print_json(&api_request(&config, Method::POST, "/goals", Some(json!({"description": description}))).await?)?;
        }
        Command::CancelGoal { goal_id } => {
            if goal_id <= 0 { bail!("goal_id must be positive"); }
            print_json(&api_request(&config, Method::POST, &format!("/goals/{goal_id}/cancel"), Some(json!({}))).await?)?;
        }
        Command::RetryGoal { goal_id } => {
            if goal_id <= 0 { bail!("goal_id must be positive"); }
            print_json(&api_request(&config, Method::POST, &format!("/goals/{goal_id}/retry"), Some(json!({}))).await?)?;
        }
    }
    Ok(())
}
