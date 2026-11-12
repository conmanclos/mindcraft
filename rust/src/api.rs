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
    pub position: [f64; 3],
    pub health: f32,
}

pub struct RuntimeStatus {
    pub minecraft_connected: AtomicBool,
    pub command_tx: Mutex<Option<mpsc::Sender<BotCommand>>>,
    pub world: Mutex<Option<WorldSnapshot>>,
}

impl Default for RuntimeStatus {
    fn default() -> Self {
        Self {
            minecraft_connected: AtomicBool::new(false),
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
    let world_context = world.map(|w| format!("\nCurrent bot state: position {:?}, health {}/20.", w.position, w.health)).unwrap_or_default();
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
    Ok(Json(PlanResponse { proposed_action, execution: "proposal only; explicitly approve it to execute" }))
}

async fn execute(State(state): State<ApiState>, headers: HeaderMap, Json(request): Json<ExecuteRequest>) -> Result<Json<ExecuteResponse>, (StatusCode, String)> {
    authorize(&state, &headers).map_err(|code| (code, "unauthorized".into()))?;
    state.brain.authorize_action(&request.action, true)
        .map_err(|e| (StatusCode::FORBIDDEN, e.to_string()))?;

    let command = match request.action.action.to_lowercase().as_str() {
        "say" => {
            let text = request.action.target.unwrap_or(request.action.reason);
            if text.trim().is_empty() || text.trim_start().starts_with('/') || text.chars().any(|c| c.is_control()) {
                return Err((StatusCode::BAD_REQUEST, "say accepts plain chat text only; slash commands and control characters are blocked".into()));
            }
            BotCommand::Say(text)
        },
        "stop" => BotCommand::Stop,
        "use_item" => BotCommand::UseItem,
        "equip" => {
            let slot = request.action.target.ok_or((StatusCode::BAD_REQUEST, "target must be a hotbar slot from 0 to 8".into()))?
                .parse::<u8>().map_err(|_| (StatusCode::BAD_REQUEST, "target must be a hotbar slot from 0 to 8".into()))?;
            if slot > 8 { return Err((StatusCode::BAD_REQUEST, "hotbar slot must be between 0 and 8".into())); }
            BotCommand::Equip(slot)
        },
        "look" | "goto" | "mine" => {
            let coordinates = request.action.target.ok_or((StatusCode::BAD_REQUEST, "target must be x,y,z".into()))?;
            let parsed = coordinates.split(',').map(str::trim).map(str::parse::<i32>).collect::<Result<Vec<_>, _>>()
                .map_err(|_| (StatusCode::BAD_REQUEST, "target must be integer coordinates x,y,z".into()))?;
            if parsed.len() != 3 || parsed[1] < -64 || parsed[1] > 320 || parsed.iter().any(|v| v.unsigned_abs() > 30_000_000) {
                return Err((StatusCode::BAD_REQUEST, "coordinates outside supported world bounds".into()));
            }
            let pos = (parsed[0], parsed[1], parsed[2]);
            match request.action.action.to_lowercase().as_str() {
                "look" => BotCommand::Look(pos.0, pos.1, pos.2),
                "goto" => BotCommand::Goto(pos.0, pos.1, pos.2),
                _ => BotCommand::Mine(pos.0, pos.1, pos.2),
            }
        }
        other => return Err((StatusCode::NOT_IMPLEMENTED, format!("executor for '{other}' is not available yet"))),
    };

    let sender = state.status.command_tx.lock().await.clone()
        .ok_or((StatusCode::SERVICE_UNAVAILABLE, "Minecraft bot is not connected".into()))?;
    sender.send(command).await.map_err(|_| (StatusCode::SERVICE_UNAVAILABLE, "Minecraft command queue is closed".into()))?;
    Ok(Json(ExecuteResponse { accepted: true, message: "command queued for the Minecraft runtime" }))
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
