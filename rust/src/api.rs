use crate::{brain::{Brain, PlannedAction}, config::Config, minecraft::{recipe_spec, AgentCommand, BotCommand}, store::{AgentTask, Goal, Store}};
use axum::{
    extract::{DefaultBodyLimit, Path, State},
    http::{header, HeaderMap, HeaderValue, Method, StatusCode},
    response::Html,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::{atomic::AtomicBool, Arc}};
use subtle::ConstantTimeEq;
use tokio::sync::{broadcast, Mutex};
use tower_http::{cors::CorsLayer, trace::TraceLayer};
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
    pub command_tx: Mutex<Option<broadcast::Sender<AgentCommand>>>,
    pub world: Mutex<Option<WorldSnapshot>>,
    pub agents: Mutex<HashMap<String, WorldSnapshot>>,
}

impl Default for RuntimeStatus {
    fn default() -> Self {
        Self {
            minecraft_connected: AtomicBool::new(false),
            connected_agents: std::sync::atomic::AtomicUsize::new(0),
            command_tx: Mutex::new(None),
            world: Mutex::new(None),
            agents: Mutex::new(HashMap::new()),
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
struct PlanRequest { message: String, #[serde(default)] agent: Option<String> }

#[derive(Deserialize)]
struct ExecuteRequest { action: PlannedAction, #[serde(default)] agent: Option<String> }

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
    agents: Vec<WorldSnapshot>,
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
        .layer(CorsLayer::new()
            .allow_origin([HeaderValue::from_static("tauri://localhost"), HeaderValue::from_static("http://tauri.localhost")])
            .allow_methods([Method::GET, Method::POST])
            .allow_headers([header::AUTHORIZATION, header::CONTENT_TYPE]))
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
    let mut agents: Vec<WorldSnapshot> = state.status.agents.lock().await.values().cloned().collect();
    agents.sort_by(|left, right| left.agent.cmp(&right.agent));
    Ok(Json(StatusResponse {
        name: "Mindcraft Native",
        version: env!("CARGO_PKG_VERSION"),
        minecraft_target: "26.1",
        minecraft_connected: state.status.minecraft_connected.load(std::sync::atomic::Ordering::Relaxed),
        connected_agents: state.status.connected_agents.load(std::sync::atomic::Ordering::Relaxed),
        autonomy: state.config.mode.clone(),
        provider: state.config.provider.clone(),
        model: state.config.model.clone(),
        world,
        agents,
    }))
}

async fn chat(State(state): State<ApiState>, headers: HeaderMap, Json(request): Json<ChatRequest>) -> Result<Json<ChatResponse>, (StatusCode, String)> {
    authorize(&state, &headers).map_err(|code| (code, "unauthorized".into()))?;
    if request.message.trim().is_empty() || request.message.len() > 12_000 {
        return Err((StatusCode::BAD_REQUEST, "message must contain 1-12000 bytes".into()));
    }
    state.store.append_message("user", &request.message)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let context = state.store.recent_context(12, 12_000)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
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

async fn plan(State(state): State<ApiState>, headers: HeaderMap, Json(request): Json<PlanRequest>) -> Result<Json<PlanResponse>, (StatusCode, String)> {
    authorize(&state, &headers).map_err(|code| (code, "unauthorized".into()))?;
    if request.message.trim().is_empty() || request.message.len() > 4_000 {
        return Err((StatusCode::BAD_REQUEST, "planning request must contain 1-4000 bytes".into()));
    }
    let target = resolve_agent_target(&state.config, request.agent.clone())?;
    let world = if let Some(username) = target.as_deref() {
        state.status.agents.lock().await.get(username).cloned()
    } else {
        state.status.world.lock().await.clone()
    };
    let world_context = world.map(|world| format!(
        "\nCurrent Minecraft state for {}: position {:?}, health {}/20, food {}/20, saturation {}, inventory {:?}.",
        world.agent, world.position, world.health, world.food, world.saturation, world.inventory
    )).unwrap_or_default();
    let planning_request = format!("Player request: {}{world_context}", request.message);
    let proposed_action = tokio::time::timeout(Duration::from_secs(50), state.brain.plan(&planning_request))
        .await.map_err(|_| (StatusCode::GATEWAY_TIMEOUT, "planner timed out".into()))?
        .map_err(|e| (StatusCode::UNPROCESSABLE_ENTITY, e.to_string()))?;

    if state.config.mode == "autonomous" && state.brain.authorize_action(&proposed_action, false).is_ok() {
        if let Ok(command) = command_from_action(proposed_action.clone()) {
            if let Some(sender) = state.status.command_tx.lock().await.clone() {
                ensure_agent_connected(&state, target.as_deref()).await?;
                sender.send(AgentCommand { target: target.clone(), command }).map_err(|_| (StatusCode::SERVICE_UNAVAILABLE, "Minecraft command stream is disconnected".into()))?;
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
    let target = resolve_agent_target(&state.config, request.agent)?;
    ensure_agent_connected(&state, target.as_deref()).await?;
    let sender = state.status.command_tx.lock().await.clone()
        .ok_or((StatusCode::SERVICE_UNAVAILABLE, "Minecraft bot is not connected".into()))?;
    sender.send(AgentCommand { target, command }).map_err(|_| (StatusCode::SERVICE_UNAVAILABLE, "Minecraft command stream is disconnected".into()))?;
    Ok(Json(ExecuteResponse { accepted: true, message: "command queued for the Minecraft runtime" }))
}

async fn ensure_agent_connected(state: &ApiState, target: Option<&str>) -> Result<(), (StatusCode, String)> {
    let connected = if let Some(username) = target {
        state.status.agents.lock().await.contains_key(username)
    } else {
        state.status.world.lock().await.is_some()
    };
    if connected { Ok(()) } else {
        Err((StatusCode::SERVICE_UNAVAILABLE, "selected Minecraft bot is not currently connected".into()))
    }
}

fn resolve_agent_target(config: &Config, target: Option<String>) -> Result<Option<String>, (StatusCode, String)> {
    let Some(target) = target.filter(|value| !value.trim().is_empty()) else { return Ok(None); };
    if config.minecraft_auth != "offline" {
        return Err((StatusCode::BAD_REQUEST, "named agent routing is available only for configured offline-mode team members; Microsoft mode routes to the primary client".into()));
    }
    config.minecraft_accounts.iter().find(|name| name.eq_ignore_ascii_case(&target))
        .cloned().map(Some)
        .ok_or((StatusCode::BAD_REQUEST, "agent must match one of the configured offline bot usernames".into()))
}

pub(crate) fn command_from_action(action: PlannedAction) -> Result<BotCommand, (StatusCode, String)> {
    match action.action.to_lowercase().as_str() {
        "say" => {
            let text = action.target.unwrap_or(action.reason);
            if text.trim().is_empty() || text.chars().count() > 256
                || text.trim_start().starts_with('/') || text.chars().any(|c| c.is_control()) {
                return Err((StatusCode::BAD_REQUEST, "say accepts 1-256 characters of plain chat text; slash commands and control characters are blocked".into()));
            }
            Ok(BotCommand::Say(text))
        }
        "stop" => Ok(BotCommand::Stop),
        "jump" => Ok(BotCommand::Jump),
        "explore" => {
            let radius = action.target.as_deref().unwrap_or("32").parse::<u16>()
                .map_err(|_| (StatusCode::BAD_REQUEST, "explore radius must be an integer from 8 to 128 blocks".into()))?;
            if !(8..=128).contains(&radius) {
                return Err((StatusCode::BAD_REQUEST, "explore radius must be from 8 to 128 blocks".into()));
            }
            Ok(BotCommand::Explore(radius))
        }
        "craft" => {
            let raw = action.target.ok_or((StatusCode::BAD_REQUEST, "target must be a recipe name, optionally followed by @x,y,z for a crafting table".into()))?;
            let mut parts = raw.split('@');
            let recipe = parts.next().unwrap_or_default().trim().to_ascii_lowercase();
            let table = match parts.next() {
                Some(coordinates) => {
                    let parsed = coordinates.split(',').map(str::trim).map(str::parse::<i32>).collect::<Result<Vec<_>, _>>()
                        .map_err(|_| (StatusCode::BAD_REQUEST, "crafting-table target must be x,y,z".into()))?;
                    if parsed.len() != 3 || parsed[1] < -64 || parsed[1] > 320 || parsed.iter().any(|v| v.unsigned_abs() > 30_000_000) {
                        return Err((StatusCode::BAD_REQUEST, "crafting-table coordinates are outside supported world bounds".into()));
                    }
                    Some((parsed[0], parsed[1], parsed[2]))
                }
                None => None,
            };
            if parts.next().is_some() {
                return Err((StatusCode::BAD_REQUEST, "craft target accepts at most one @x,y,z crafting-table coordinate".into()));
            }
            let Some((requires_table, _, _)) = recipe_spec(&recipe) else {
                return Err((StatusCode::NOT_IMPLEMENTED, "that recipe is not in the runtime's explicit crafting allowlist".into()));
            };
            if requires_table && table.is_none() {
                return Err((StatusCode::BAD_REQUEST, "this recipe requires a crafting table; append @x,y,z to the recipe name".into()));
            }
            Ok(BotCommand::Craft(recipe, table))
        }
        "container_transfer" => {
            let coordinates = action.target.ok_or((StatusCode::BAD_REQUEST, "target must be x,y,z,slot for a container transfer".into()))?;
            let parsed = coordinates.split(',').map(str::trim).map(str::parse::<i32>).collect::<Result<Vec<_>, _>>()
                .map_err(|_| (StatusCode::BAD_REQUEST, "target must be integer coordinates x,y,z,slot".into()))?;
            if parsed.len() != 4 || parsed[1] < -64 || parsed[1] > 320
                || parsed[..3].iter().any(|v| v.unsigned_abs() > 30_000_000)
                || !(0..=89).contains(&parsed[3]) {
                return Err((StatusCode::BAD_REQUEST, "container transfer requires valid world coordinates and a slot from 0 to 89".into()));
            }
            Ok(BotCommand::ContainerTransfer(parsed[0], parsed[1], parsed[2], parsed[3] as u16))
        }
        "use_item" => Ok(BotCommand::UseItem),
        "attack" => {
            let target = action.target.unwrap_or_default();
            if !target.eq_ignore_ascii_case("crosshair") {
                return Err((StatusCode::BAD_REQUEST, "attack target must be 'crosshair'; aim at a visible entity first".into()));
            }
            Ok(BotCommand::AttackCrosshair)
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
        "look" | "goto" | "mine" | "place" => {
            let coordinates = action.target.ok_or((StatusCode::BAD_REQUEST, "target must be x,y,z".into()))?;
            let parsed = coordinates.split(',').map(str::trim).map(str::parse::<i32>).collect::<Result<Vec<_>, _>>()
                .map_err(|_| (StatusCode::BAD_REQUEST, "target must be integer coordinates x,y,z".into()))?;
            if parsed.len() != 3 || parsed[1] < -64 || parsed[1] > 320 || parsed.iter().any(|v| v.unsigned_abs() > 30_000_000) {
                return Err((StatusCode::BAD_REQUEST, "coordinates outside supported world bounds".into()));
            }
            let (x, y, z) = (parsed[0], parsed[1], parsed[2]);
            match action.action.to_lowercase().as_str() {
                "look" => Ok(BotCommand::Look(x, y, z)),
                "goto" => Ok(BotCommand::Goto(x, y, z)),
                "place" => Ok(BotCommand::Place(x, y, z)),
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
    let goal_id = state.store.create_goal(&request.description)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let mut tasks = Vec::with_capacity(drafts.len());
    for draft in drafts {
        let id = state.store.add_task(goal_id, &draft.role, &draft.description, draft.priority)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        tasks.push(serde_json::json!({"id":id,"role":draft.role,"description":draft.description,"priority":draft.priority,"status":"pending"}));
    }
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
mod tests {
    use super::*;
    use crate::brain::PlannedAction;

    fn action(name: &str, target: Option<&str>) -> PlannedAction {
        PlannedAction {
            action: name.into(),
            target: target.map(str::to_owned),
            reason: "test".into(),
            risk: "low".into(),
        }
    }

    #[test]
    fn chat_commands_cannot_invoke_server_commands_or_control_characters() {
        assert!(command_from_action(action("say", Some("hello team"))).is_ok());
        assert!(command_from_action(action("say", Some("/op player"))).is_err());
        assert!(command_from_action(action("say", Some("hello\n/op player"))).is_err());
        assert!(command_from_action(action("say", Some("   "))).is_err());
    }

    #[test]
    fn chat_length_is_bounded_to_minecraft_message_limit() {
        let max_message = "a".repeat(256);
        let oversized_message = "a".repeat(257);
        assert!(command_from_action(action("say", Some(max_message.as_str()))).is_ok());
        assert!(command_from_action(action("say", Some(oversized_message.as_str()))).is_err());
    }

    #[test]
    fn container_transfer_rejects_slots_outside_standard_menu_bounds() {
        assert!(command_from_action(action("container_transfer", Some("0,64,0,89"))).is_ok());
        assert!(command_from_action(action("container_transfer", Some("0,64,0,90"))).is_err());
        assert!(command_from_action(action("container_transfer", Some("0,64,0,-1"))).is_err());
    }

    #[test]
    fn attack_is_limited_to_the_visible_crosshair_target() {
        assert!(matches!(
            command_from_action(action("attack", Some("crosshair"))),
            Ok(BotCommand::AttackCrosshair)
        ));
        assert!(command_from_action(action("attack", Some("nearest"))).is_err());
        assert!(command_from_action(action("attack", None)).is_err());
    }

    #[test]
    fn coordinate_actions_reject_malformed_and_out_of_world_targets() {
        assert!(matches!(
            command_from_action(action("goto", Some("12,64,-8"))),
            Ok(BotCommand::Goto(12, 64, -8))
        ));
        for target in ["1,2", "1,2,3,4", "1,321,3", "1,-65,3", "30000001,64,0", "x,64,0"] {
            assert!(command_from_action(action("mine", Some(target))).is_err(), "accepted invalid target: {target}");
        }
    }

    #[test]
    fn follow_and_equip_targets_are_strictly_validated() {
        assert!(matches!(
            command_from_action(action("follow", Some("Player_123"))),
            Ok(BotCommand::Follow(name)) if name == "Player_123"
        ));
        for name in ["ab", "player name", "../player", "player!"] {
            assert!(command_from_action(action("follow", Some(name))).is_err());
        }
        assert!(matches!(
            command_from_action(action("equip", Some("8"))),
            Ok(BotCommand::Equip(8))
        ));
        for slot in ["-1", "9", "1.5", "x"] {
            assert!(command_from_action(action("equip", Some(slot))).is_err());
        }
    }

    fn routing_config(auth: &str) -> Config {
        Config {
            bind: "127.0.0.1:8787".parse().unwrap(),
            api_token: "0123456789abcdef0123456789abcdef".into(),
            api_token_was_generated: false,
            mode: "balanced".into(), provider: "ollama".into(), model: "test".into(),
            api_key: String::new(), base_url: "http://127.0.0.1:11434/v1".into(),
            minecraft_server: Some("localhost".into()), minecraft_username: "Mindcraft".into(),
            minecraft_accounts: vec!["Mindcraft".into(), "Scout".into(), "Builder".into()],
            minecraft_auth: auth.into(), microsoft_email: None,
            database: std::path::PathBuf::from(":memory:"),
        }
    }

    #[test]
    fn crafting_recipes_and_table_coordinates_are_validated() {
        assert!(matches!(command_from_action(action("craft", Some("sticks"))), Ok(BotCommand::Craft(name, None)) if name == "sticks"));
        assert!(matches!(command_from_action(action("craft", Some("wooden_pickaxe@10,64,-4"))), Ok(BotCommand::Craft(name, Some((10,64,-4)))) if name == "wooden_pickaxe"));
        assert!(matches!(command_from_action(action("craft", Some("diamond_pickaxe@10,64,-4"))), Ok(BotCommand::Craft(name, Some((10,64,-4)))) if name == "diamond_pickaxe"));
        assert!(matches!(command_from_action(action("craft", Some("door@10,64,-4"))), Ok(BotCommand::Craft(name, Some((10,64,-4)))) if name == "door"));
        for target in ["wooden_pickaxe", "diamond_pickaxe", "unknown_recipe", "furnace@1,321,2", "chest@1,64,2@3,64,4"] {
            assert!(command_from_action(action("craft", Some(target))).is_err());
        }
    }

    #[test]
    fn container_transfer_coordinates_and_slot_are_bounded() {
        assert!(matches!(command_from_action(action("container_transfer", Some("1,64,-2,27"))), Ok(BotCommand::ContainerTransfer(1,64,-2,27))));
        for target in ["1,64,2", "1,64,2,101", "1,321,2,4", "x,64,2,4"] {
            assert!(command_from_action(action("container_transfer", Some(target))).is_err());
        }
    }

    #[test]
    fn exploration_radius_is_bounded() {
        assert!(matches!(command_from_action(action("explore", None)), Ok(BotCommand::Explore(32))));
        assert!(matches!(command_from_action(action("explore", Some("64"))), Ok(BotCommand::Explore(64))));
        for radius in ["0", "7", "129", "-1", "far"] {
            assert!(command_from_action(action("explore", Some(radius))).is_err());
        }
    }

    #[test]
    fn agent_routing_is_limited_to_configured_offline_team_members() {
        let config = routing_config("offline");
        assert_eq!(resolve_agent_target(&config, Some("sCoUt".into())).unwrap().as_deref(), Some("Scout"));
        assert_eq!(resolve_agent_target(&config, None).unwrap(), None);
        assert!(resolve_agent_target(&config, Some("unknown".into())).is_err());
        assert!(resolve_agent_target(&routing_config("microsoft"), Some("Mindcraft".into())).is_err());
    }
}
