use crate::{brain::{Brain, PlannedAction}, config::Config, minecraft::BotCommand, store::{AgentTask, Goal, Store}};
use axum::{
    extract::{DefaultBodyLimit, Path, State},
    http::{header, HeaderMap, StatusCode},
    response::Html,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::{atomic::AtomicBool, Arc};
use subtle::ConstantTimeEq;
use tokio::sync::{mpsc, Mutex};
use tower_http::trace::TraceLayer;
use std::time::Duration;

#[derive(Clone, Serialize)]
pub struct WorldSnapshot {
    pub agent: String,
    pub position: [f64; 3],
    pub health: f32,
    pub food: u32,
    pub saturation: f32,
    pub inventory: Vec<String>,
}

pub struct RuntimeStatus {
    pub minecraft_connected: AtomicBool,
    pub connected_agents: std::sync::atomic::AtomicUsize,
    pub command_tx: Mutex<Option<mpsc::Sender<BotCommand>>>,
    pub world: Mutex<Option<WorldSnapshot>>,
}

impl Default for RuntimeStatus {
    fn default() -> Self {
        Self {
            minecraft_connected: AtomicBool::new(false),
            connected_agents: std::sync::atomic::AtomicUsize::new(0),
            command_tx: Mutex::new(None),
            world: Mutex::new(None),
        }
    }
}

#[derive(Clone)]
struct ApiState {
    config: Arc<Config>,
    brain: Arc<Brain>,
    store: Arc<Store>,
    status: Arc<RuntimeStatus>,
}

#[derive(Deserialize)]
pub struct ChatRequest { pub message: String }

#[derive(Deserialize)]
struct GoalRequest { description: String }

#[derive(Deserialize)]
struct AgentClaimRequest { agent_id: String, role: String }

#[derive(Deserialize)]
struct CompleteTaskRequest { agent_id: String }

#[derive(Deserialize)]
struct ExecuteRequest { action: PlannedAction }

#[derive(Serialize)]
struct ChatResponse { reply: String }

#[derive(Serialize)]
struct PlanResponse {
    proposed_action: PlannedAction,
    execution: &'static str,
    auto_queued: bool,
}

#[derive(Serialize)]
struct ExecuteResponse {
    accepted: bool,
    message: &'static str,
}

#[derive(Serialize)]
struct ClaimResponse {
    task: Option<AgentTask>,
}

#[derive(Serialize)]
struct CompleteResponse {
    completed: bool,
}

#[derive(Serialize)]
struct StatusResponse {
    name: &'static str,
    version: &'static str,
    minecraft_target: &'static str,
    minecraft_connected: bool,
    connected_agents: usize,
    autonomy: String,
    provider: String,
    model: String,
    world: Option<WorldSnapshot>,
}

pub async fn serve(config: Arc<Config>, brain: Arc<Brain>, store: Arc<Store>, status: Arc<RuntimeStatus>) -> anyhow::Result<()> {
    let state = ApiState { config: config.clone(), brain, store, status };
    let app = Router::new()
        .route("/", get(dashboard))
        .route("/api/health", get(health))
        .route("/api/status", get(status_handler))
        .route("/api/chat", post(chat))
        .route("/api/plan", post(plan))
        .route("/api/execute", post(execute))
        .route("/api/goals", get(list_goals).post(create_goal))
        .route("/api/goals/:goal_id/tasks", get(list_goal_tasks))
        .route("/api/tasks/claim", post(claim_task))
        .route("/api/tasks/:task_id/complete", post(complete_task))
        .route("/api/tasks/:task_id/heartbeat", post(heartbeat_task))
        .with_state(state)
        .layer(DefaultBodyLimit::max(16 * 1024))
        .layer(TraceLayer::new_for_http());

    let listener = tokio::net::TcpListener::bind(config.bind).await?;
    tracing::info!(address = %listener.local_addr()?, "dashboard available");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn dashboard() -> Html<&'static str> {
    Html(include_str!("../web/index.html"))
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({"ok":true,"service":"mindcraft-native"}))
}

async fn status_handler(State(state): State<ApiState>, headers: HeaderMap) -> Result<Json<StatusResponse>, StatusCode> {
    authorize(&state, &headers)?;
    let world = state.status.world.lock().await.clone();
    Ok(Json(StatusResponse {
        name: "Mindcraft Native",
        version: env!("CARGO_PKG_VERSION"),
        minecraft_target: "1.21.1",
        minecraft_connected: state.status.minecraft_connected.load(std::sync::atomic::Ordering::Relaxed),
        connected_agents: state.status.connected_agents.load(std::sync::atomic::Ordering::Relaxed),
        autonomy: state.config.mode.clone(),
        provider: state.config.provider.clone(),
        model: state.config.model.clone(),
        world,
    }))
}

async fn chat(State(state): State<ApiState>, headers: HeaderMap, Json(request): Json<ChatRequest>) -> Result<Json<ChatResponse>, (StatusCode, String)> {
    authorize(&state, &headers).map_err(|code| (code, "unauthorized".into()))?;
    if request.message.trim().is_empty() || request.message.len() > 12_000 {
        return Err((StatusCode::BAD_REQUEST, "message must contain 1-12000 bytes".into()));
    }
    state.store.append_message("user", &request.message)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let history = state.store.recent_messages(12)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let context = history.into_iter().map(|(role, text)| format!("{role}: {text}")).collect::<Vec<_>>().join("\n");
    let world = state.status.world.lock().await.clone();
    let world_context = world.map(|w| format!("\nCurrent bot state: position {:?}, health {}/20, food {}/20, saturation {}, inventory: {:?}.", w.position, w.health, w.food, w.saturation, w.inventory)).unwrap_or_default();
    let prompt = format!("Recent conversation:\n{context}{world_context}\n\nRespond to the latest user message.");
    let reply = tokio::time::timeout(Duration::from_secs(50), state.brain.reply(&prompt))
        .await.map_err(|_| (StatusCode::GATEWAY_TIMEOUT, "AI provider timed out".into()))?
        .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?;
    state.store.append_message("assistant", &reply)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(ChatResponse { reply }))
}

async fn plan(State(state): State<ApiState>, headers: HeaderMap, Json(request): Json<ChatRequest>) -> Result<Json<PlanResponse>, (StatusCode, String)> {
    authorize(&state, &headers).map_err(|code| (code, "unauthorized".into()))?;
    if request.message.trim().is_empty() || request.message.len() > 4_000 {
        return Err((StatusCode::BAD_REQUEST, "planning request must contain 1-4000 bytes".into()));
    }
    let proposed_action = tokio::time::timeout(Duration::from_secs(50), state.brain.plan(&request.message))
        .await.map_err(|_| (StatusCode::GATEWAY_TIMEOUT, "planner timed out".into()))?
        .map_err(|e| (StatusCode::UNPROCESSABLE_ENTITY, e.to_string()))?;

    if state.config.mode == "autonomous"
        && state.status.minecraft_connected.load(std::sync::atomic::Ordering::Relaxed)
        && state.brain.authorize_action(&proposed_action, false).is_ok() {
        if let Ok(command) = command_from_action(proposed_action.clone()) {
            if let Some(sender) = state.status.command_tx.lock().await.clone() {
                sender.try_send(command).map_err(|_| (StatusCode::SERVICE_UNAVAILABLE, "Minecraft command queue is full or disconnected".into()))?;
                return Ok(Json(PlanResponse {
                    proposed_action,
                    execution: "AUTONOMOUS: command queued without manual approval",
                    auto_queued: true,
                }));
            }
        }
    }

    let execution = if state.config.mode == "autonomous" {
        "Minecraft client unavailable or action has no executor; review or retry after connection"
    } else {
        "proposal only; explicitly approve it to execute"
    };
    Ok(Json(PlanResponse { proposed_action, execution, auto_queued: false }))
}

async fn execute(State(state): State<ApiState>, headers: HeaderMap, Json(request): Json<ExecuteRequest>) -> Result<Json<ExecuteResponse>, (StatusCode, String)> {
    authorize(&state, &headers).map_err(|code| (code, "unauthorized".into()))?;
    state.brain.authorize_action(&request.action, true)
        .map_err(|e| (StatusCode::FORBIDDEN, e.to_string()))?;
    let command = command_from_action(request.action)?;
    if !state.status.minecraft_connected.load(std::sync::atomic::Ordering::Relaxed) {
        return Err((StatusCode::SERVICE_UNAVAILABLE, "Minecraft bot is not connected".into()));
    }
    let sender = state.status.command_tx.lock().await.clone()
        .ok_or((StatusCode::SERVICE_UNAVAILABLE, "Minecraft command queue is unavailable".into()))?;
    sender.try_send(command).map_err(|_| (StatusCode::SERVICE_UNAVAILABLE, "Minecraft command queue is full or disconnected".into()))?;
    Ok(Json(ExecuteResponse { accepted: true, message: "command queued for the Minecraft runtime" }))
}

fn command_from_action(action: PlannedAction) -> Result<BotCommand, (StatusCode, String)> {
    match action.action.to_lowercase().as_str() {
        "say" => {
            let text = action.target.unwrap_or(action.reason);
            if text.trim().is_empty() || text.len() > 256 || text.trim_start().starts_with('/') || text.chars().any(|c| c.is_control()) {
                return Err((StatusCode::BAD_REQUEST, "say accepts 1-256 bytes of plain chat text only; slash commands and control characters are blocked".into()));
            }
            Ok(BotCommand::Say(text))
        }
        "stop" => Ok(BotCommand::Stop),
        "jump" => Ok(BotCommand::Jump),
        "sneak" => {
            let enabled = action.target.ok_or((StatusCode::BAD_REQUEST, "sneak target must be true or false".into()))?;
            match enabled.to_lowercase().as_str() {
                "true" | "on" => Ok(BotCommand::Sneak(true)),
                "false" | "off" => Ok(BotCommand::Sneak(false)),
                _ => Err((StatusCode::BAD_REQUEST, "sneak target must be true/on or false/off".into())),
            }
        }
        "use_item" => Ok(BotCommand::UseItem),
        "craft" => {
            let recipe = action.target.unwrap_or_default().to_lowercase().replace(' ', "_");
            if !["planks", "sticks", "crafting_table"].contains(&recipe.as_str()) {
                return Err((StatusCode::BAD_REQUEST, "craft target must be planks, sticks, or crafting_table".into()));
            }
            Ok(BotCommand::Craft(recipe))
        }
        "attack" => {
            let target = action.target.unwrap_or_default();
            if !(3..=16).contains(&target.len()) || !target.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                return Err((StatusCode::BAD_REQUEST, "attack target must be an online Minecraft username".into()));
            }
            Ok(BotCommand::AttackPlayer(target))
        }
        "follow" => {
            let username = action.target.ok_or((StatusCode::BAD_REQUEST, "target must be a Minecraft username".into()))?;
            if !(3..=16).contains(&username.len()) || !username.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                return Err((StatusCode::BAD_REQUEST, "target must be a valid Minecraft username".into()));
            }
            Ok(BotCommand::Follow(username))
        }
        "equip" => {
            let slot = action.target.ok_or((StatusCode::BAD_REQUEST, "target must be a hotbar slot from 0 to 8".into()))?
                .parse::<u8>().map_err(|_| (StatusCode::BAD_REQUEST, "target must be a hotbar slot from 0 to 8".into()))?;
            if slot > 8 { return Err((StatusCode::BAD_REQUEST, "hotbar slot must be between 0 and 8".into())); }
            Ok(BotCommand::Equip(slot))
        }
        "withdraw" => {
            let target = action.target.unwrap_or_default();
            let parts = target.splitn(4, ',').map(str::trim).collect::<Vec<_>>();
            if parts.len() != 4 {
                return Err((StatusCode::BAD_REQUEST, "withdraw target must be x,y,z,item_id".into()));
            }
            let x = parts[0].parse::<i32>().map_err(|_| (StatusCode::BAD_REQUEST, "invalid x coordinate".into()))?;
            let y = parts[1].parse::<i32>().map_err(|_| (StatusCode::BAD_REQUEST, "invalid y coordinate".into()))?;
            let z = parts[2].parse::<i32>().map_err(|_| (StatusCode::BAD_REQUEST, "invalid z coordinate".into()))?;
            let item = parts[3].to_lowercase();
            if y < -64 || y > 319 || [x, y, z].iter().any(|value| value.unsigned_abs() > 29_999_984)
                || item.is_empty() || item.len() > 64
                || !item.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ':') {
                return Err((StatusCode::BAD_REQUEST, "withdraw target must contain valid world coordinates and an item id".into()));
            }
            Ok(BotCommand::Withdraw(x, y, z, item))
        }
        "look" | "goto" | "mine" | "place" | "deposit" => {
            let coordinates = action.target.ok_or((StatusCode::BAD_REQUEST, "target must be x,y,z".into()))?;
            let parsed = coordinates.split(',').map(str::trim).map(str::parse::<i32>).collect::<Result<Vec<_>, _>>()
                .map_err(|_| (StatusCode::BAD_REQUEST, "target must be integer coordinates x,y,z".into()))?;
            if parsed.len() != 3 || parsed[1] < -64 || parsed[1] > 319 || parsed.iter().any(|v| v.unsigned_abs() > 29_999_984) {
                return Err((StatusCode::BAD_REQUEST, "coordinates outside supported world bounds".into()));
            }
            let (x, y, z) = (parsed[0], parsed[1], parsed[2]);
            match action.action.to_lowercase().as_str() {
                "look" => Ok(BotCommand::Look(x, y, z)),
                "goto" => Ok(BotCommand::Goto(x, y, z)),
                "place" => Ok(BotCommand::Place(x, y, z)),
                "deposit" => Ok(BotCommand::Deposit(x, y, z)),
                _ => Ok(BotCommand::Mine(x, y, z)),
            }
        }
        other => Err((StatusCode::NOT_IMPLEMENTED, format!("executor for '{other}' is not available yet"))),
    }
}

async fn create_goal(State(state): State<ApiState>, headers: HeaderMap, Json(request): Json<GoalRequest>) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    authorize(&state, &headers).map_err(|code| (code, "unauthorized".into()))?;
    if request.description.trim().is_empty() || request.description.len() > 4_000 {
        return Err((StatusCode::BAD_REQUEST, "goal must contain 1-4000 bytes".into()));
    }
    let drafts = tokio::time::timeout(Duration::from_secs(50), state.brain.decompose_goal(&request.description))
        .await.map_err(|_| (StatusCode::GATEWAY_TIMEOUT, "strategic planner timed out".into()))?
        .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?;
    let task_specs = drafts.iter()
        .map(|draft| (draft.role.clone(), draft.description.clone(), draft.priority))
        .collect::<Vec<_>>();
    let (goal_id, task_ids) = state.store.create_goal_with_tasks(&request.description, &task_specs)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let tasks = drafts.into_iter().zip(task_ids)
        .map(|(draft, id)| serde_json::json!({
            "id":id,"role":draft.role,"description":draft.description,
            "priority":draft.priority,"status":"pending"
        }))
        .collect::<Vec<_>>();
    Ok(Json(serde_json::json!({"goal_id":goal_id,"description":request.description,"tasks":tasks})))
}

async fn list_goals(State(state): State<ApiState>, headers: HeaderMap) -> Result<Json<Vec<Goal>>, StatusCode> {
    authorize(&state, &headers)?;
    let goals = state.store.list_goals().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(goals))
}

async fn list_goal_tasks(State(state): State<ApiState>, headers: HeaderMap, Path(goal_id): Path<i64>) -> Result<Json<Vec<AgentTask>>, (StatusCode, String)> {
    authorize(&state, &headers).map_err(|code| (code, "unauthorized".into()))?;
    let tasks = state.store.list_tasks(goal_id).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(tasks))
}

async fn claim_task(State(state): State<ApiState>, headers: HeaderMap, Json(request): Json<AgentClaimRequest>) -> Result<Json<ClaimResponse>, (StatusCode, String)> {
    authorize(&state, &headers).map_err(|code| (code, "unauthorized".into()))?;
    let roles = ["planner", "scout", "gatherer", "builder", "crafter", "guardian"];
    if request.agent_id.trim().is_empty() || request.agent_id.len() > 64 || !roles.contains(&request.role.as_str()) {
        return Err((StatusCode::BAD_REQUEST, "agent_id must be 1-64 bytes and role must be a supported specialist role".into()));
    }
    let task = state.store.claim_task(&request.agent_id, &request.role)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(ClaimResponse { task }))
}

async fn heartbeat_task(State(state): State<ApiState>, headers: HeaderMap, Path(task_id): Path<i64>, Json(request): Json<CompleteTaskRequest>) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    authorize(&state, &headers).map_err(|code| (code, "unauthorized".into()))?;
    if request.agent_id.trim().is_empty() || request.agent_id.len() > 64 {
        return Err((StatusCode::BAD_REQUEST, "agent_id must contain 1-64 bytes".into()));
    }
    let renewed = state.store.renew_task_lease(task_id, &request.agent_id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if !renewed {
        return Err((StatusCode::CONFLICT, "task lease is no longer owned by this agent".into()));
    }
    Ok(Json(serde_json::json!({"renewed":true})))
}

async fn complete_task(State(state): State<ApiState>, headers: HeaderMap, Path(task_id): Path<i64>, Json(request): Json<CompleteTaskRequest>) -> Result<Json<CompleteResponse>, (StatusCode, String)> {
    authorize(&state, &headers).map_err(|code| (code, "unauthorized".into()))?;
    if request.agent_id.trim().is_empty() || request.agent_id.len() > 64 {
        return Err((StatusCode::BAD_REQUEST, "agent_id must contain 1-64 bytes".into()));
    }
    let completed = state.store.complete_task(task_id, &request.agent_id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if !completed {
        return Err((StatusCode::CONFLICT, "task is not claimed by this agent or is already complete".into()));
    }
    Ok(Json(CompleteResponse { completed }))
}

fn authorize(state: &ApiState, headers: &HeaderMap) -> Result<(), StatusCode> {
    let provided = headers.get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .ok_or(StatusCode::UNAUTHORIZED)?;
    if provided.as_bytes().len() != state.config.api_token.as_bytes().len()
        || !bool::from(provided.as_bytes().ct_eq(state.config.api_token.as_bytes())) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(())
}


#[cfg(test)]
mod action_tests {
    use super::*;

    fn action(name: &str, target: Option<&str>) -> PlannedAction {
        PlannedAction {
            action: name.into(),
            target: target.map(str::to_owned),
            reason: String::new(),
            risk: "medium".into(),
        }
    }

    #[test]
    fn crafting_is_limited_to_supported_2x2_recipes() {
        assert!(matches!(
            command_from_action(action("craft", Some("crafting_table"))),
            Ok(BotCommand::Craft(_))
        ));
        assert!(command_from_action(action("craft", Some("diamond_sword"))).is_err());
    }

    #[test]
    fn withdrawal_requires_coordinates_and_a_valid_item_id() {
        assert!(matches!(
            command_from_action(action("withdraw", Some("10,64,-3,minecraft:oak_log"))),
            Ok(BotCommand::Withdraw(10, 64, -3, _))
        ));
        assert!(command_from_action(action("withdraw", Some("10,64,-3,../secrets"))).is_err());
        assert!(command_from_action(action("withdraw", Some("10,64,-3"))).is_err());
    }

    #[test]
    fn attack_requires_a_minecraft_username() {
        assert!(matches!(
            command_from_action(action("attack", Some("Notch"))),
            Ok(BotCommand::AttackPlayer(_))
        ));
        assert!(command_from_action(action("attack", Some("bad name"))).is_err());
    }

    #[test]
    fn chat_never_accepts_slash_commands() {
        assert!(command_from_action(action("say", Some("/op player"))).is_err());
        assert!(command_from_action(action("say", Some("hello teammate"))).is_ok());
    }
}
