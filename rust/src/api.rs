use crate::{brain::{Brain, PlannedAction}, config::Config, minecraft::{recipe_spec, smelt_spec, AgentCommand, BotCommand, BuildBlock, SmeltJob}, store::{AgentTask, Goal, Store}};
use axum::{
    extract::{DefaultBodyLimit, Path, Request, State},
    http::{header, HeaderMap, HeaderValue, Method, StatusCode},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::{collections::{hash_map::DefaultHasher, HashMap, HashSet}, hash::{Hash, Hasher}, sync::{atomic::AtomicBool, Arc}, time::Instant};
use subtle::ConstantTimeEq;
use tokio::sync::{broadcast, Mutex};
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use std::{io::Read, time::Duration};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use flate2::read::GzDecoder;
use fastnbt::ByteArray;

#[derive(Clone, Serialize)]
pub struct WorldSnapshot {
    pub agent: String,
    pub role: String,
    pub dimension: String,
    pub biome: Option<String>,
    pub position: [f64; 3],
    pub health: f32,
    pub food: u32,
    pub saturation: f32,
    pub inventory: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConnectionDiagnostics {
    pub state: String,
    pub last_error: Option<String>,
    pub reconnect_attempts: u64,
    pub retry_in_seconds: Option<u64>,
    pub last_connected_unix: Option<u64>,
    pub last_disconnected_unix: Option<u64>,
}

impl Default for ConnectionDiagnostics {
    fn default() -> Self {
        Self {
            state: "not_configured".into(),
            last_error: None,
            reconnect_attempts: 0,
            retry_in_seconds: None,
            last_connected_unix: None,
            last_disconnected_unix: None,
        }
    }
}

#[derive(Clone)]
struct AgentCredential {
    token: String,
    expires_at: Instant,
}

const AGENT_CREDENTIAL_TTL: Duration = Duration::from_secs(60 * 60);

pub struct RuntimeStatus {
    pub minecraft_connected: AtomicBool,
    pub connected_agents: std::sync::atomic::AtomicUsize,
    pub command_tx: Mutex<Option<broadcast::Sender<AgentCommand>>>,
    pub world: Mutex<Option<WorldSnapshot>>,
    pub agents: Mutex<HashMap<String, WorldSnapshot>>,
    pub online_players: Mutex<HashSet<String>>,
    pub agent_tokens: Mutex<HashMap<String, AgentCredential>>,
    pub connection: Mutex<ConnectionDiagnostics>,
}

impl Default for RuntimeStatus {
    fn default() -> Self {
        Self {
            minecraft_connected: AtomicBool::new(false),
            connected_agents: std::sync::atomic::AtomicUsize::new(0),
            command_tx: Mutex::new(None),
            world: Mutex::new(None),
            agents: Mutex::new(HashMap::new()),
            online_players: Mutex::new(HashSet::new()),
            agent_tokens: Mutex::new(HashMap::new()),
            connection: Mutex::new(ConnectionDiagnostics::default()),
        }
    }
}

#[derive(Clone)]
struct ApiState {
    config: Arc<Config>,
    brain: Arc<Brain>,
    store: Arc<Store>,
    status: Arc<RuntimeStatus>,
    rate_limits: Arc<Mutex<HashMap<String, RateWindow>>>,
    // Chat history is a single ordered conversation. Serialize chat turns so
    // concurrent dashboard requests cannot splice one user's prompt into
    // another request's context or interleave assistant responses.
    chat_lock: Arc<Mutex<()>>,
}

#[derive(Clone, Copy)]
struct RateWindow {
    started: Instant,
    requests: u32,
}

const API_RATE_LIMIT: u32 = 180;
const API_RATE_WINDOW: Duration = Duration::from_secs(60);

fn rate_limit_bucket_key(provided: Option<&str>, authenticated: bool) -> String {
    if !authenticated {
        return "anonymous".to_owned();
    }
    let mut hasher = DefaultHasher::new();
    provided.unwrap_or_default().hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

fn consume_rate_window(window: &mut RateWindow, now: Instant) -> bool {
    if now.duration_since(window.started) >= API_RATE_WINDOW {
        window.started = now;
        window.requests = 0;
    }
    if window.requests >= API_RATE_LIMIT {
        return false;
    }
    window.requests += 1;
    true
}

async fn no_store_responses(request: Request, next: Next) -> Response {
    let mut response = next.run(request).await;
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-store, max-age=0"),
    );
    response
}

async fn rate_limit(State(state): State<ApiState>, request: Request, next: Next) -> Response {
    // Only authenticated credentials receive individual buckets. Otherwise an
    // unauthenticated caller can fill the bounded map with arbitrary bearer
    // strings and deny service to the real dashboard token.
    let now = Instant::now();
    let provided = request.headers().get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    let authenticated = if let Some(token) = provided {
        if constant_time_equal(token, &state.config.api_token) {
            true
        } else {
            let mut tokens = state.status.agent_tokens.lock().await;
            tokens.retain(|_, credential| now < credential.expires_at);
            tokens.values().any(|credential| agent_credential_matches(credential, token, now))
        }
    } else {
        false
    };

    // All missing, malformed, expired, and invalid credentials share one
    // anonymous bucket. The authenticated key remains a one-way hash so raw
    // bearer tokens are never retained in the limiter map.
    let key = rate_limit_bucket_key(provided, authenticated);

    let mut windows = state.rate_limits.lock().await;
    windows.retain(|_, window| now.duration_since(window.started) < API_RATE_WINDOW * 2);
    let window = windows.entry(key).or_insert(RateWindow { started: now, requests: 0 });
    if !consume_rate_window(window, now) {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(serde_json::json!({"error":"API request rate limit exceeded; retry after the current 60-second window"})),
        ).into_response();
    }
    drop(windows);
    next.run(request).await
}

#[derive(Deserialize)]
pub struct ChatRequest { pub message: String }

#[derive(Deserialize)]
struct GoalRequest { description: String }

#[derive(Deserialize)]
struct MemoryRequest {
    kind: String,
    subject: String,
    content: String,
    #[serde(default = "default_memory_confidence")]
    confidence: f64,
    #[serde(default = "default_memory_importance")]
    importance: i32,
    #[serde(default = "default_memory_source")]
    source: String,
}

fn default_memory_confidence() -> f64 { 1.0 }
fn default_memory_importance() -> i32 { 50 }
fn default_memory_source() -> String { "user".into() }

#[derive(Deserialize)]
struct AgentClaimRequest { agent_id: String, role: String }

#[derive(Deserialize)]
struct CompleteTaskRequest { agent_id: String, verification_note: String }

#[derive(Deserialize)]
struct FailTaskRequest { agent_id: String, reason: String }

#[derive(Deserialize)]
struct PlanRequest { message: String, #[serde(default)] agent: Option<String> }

#[derive(Deserialize)]
struct ExecuteRequest { action: PlannedAction, #[serde(default)] agent: Option<String> }

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SchematicBuildRequest {
    origin: [i32; 3],
    blocks: Vec<SchematicBlock>,
    #[serde(default)]
    rotation: u16,
    #[serde(default)]
    mirror_x: bool,
    #[serde(default)]
    mirror_z: bool,
    #[serde(default)]
    agent: Option<String>,
}

#[derive(Deserialize)]
struct SpongeSchematicBuildRequest {
    origin: [i32; 3],
    data_base64: String,
    #[serde(default)]
    rotation: u16,
    #[serde(default)]
    mirror_x: bool,
    #[serde(default)]
    mirror_z: bool,
    #[serde(default)]
    agent: Option<String>,
}

#[derive(Deserialize)]
struct SpongeSchematicNbt {
    #[serde(rename = "Version", default)]
    version: i32,
    #[serde(rename = "Width")]
    width: i16,
    #[serde(rename = "Height")]
    height: i16,
    #[serde(rename = "Length")]
    length: i16,
    #[serde(rename = "Offset", default)]
    offset: Option<fastnbt::IntArray>,
    #[serde(rename = "Palette", default)]
    palette: Option<HashMap<String, i32>>,
    #[serde(rename = "BlockData", default)]
    block_data: Option<ByteArray>,
    #[serde(rename = "Blocks", default)]
    blocks: Option<SpongeBlocksNbt>,
}

#[derive(Deserialize)]
struct SpongeBlocksNbt {
    #[serde(rename = "Palette")]
    palette: HashMap<String, i32>,
    #[serde(rename = "Data")]
    data: ByteArray,
}

const MAX_SPONGE_BASE64_BYTES: usize = 1_900_000;
const MAX_SPONGE_NBT_BYTES: usize = 4 * 1024 * 1024;

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
struct AgentCredentialResponse {
    agent_id: String,
    token: String,
    expires_in_seconds: u64,
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
    online_players: Vec<String>,
    connection: ConnectionDiagnostics,
}

pub async fn serve(config: Arc<Config>, brain: Arc<Brain>, store: Arc<Store>, status: Arc<RuntimeStatus>) -> anyhow::Result<()> {
    let state = ApiState {
        config: config.clone(),
        brain,
        store,
        status,
        rate_limits: Arc::new(Mutex::new(HashMap::new())),
        chat_lock: Arc::new(Mutex::new(())),
    };
    let app = Router::new()
        .route("/", get(dashboard))
        .route("/api/health", get(health))
        .route("/api/status", get(status_handler))
        .route("/api/chat", post(chat))
        .route("/api/plan", post(plan))
        .route("/api/execute", post(execute))
        .route("/api/build", post(build_schematic))
        .route("/api/build/sponge", post(build_sponge_schematic).layer(DefaultBodyLimit::max(2_100_000)))
        .route("/api/memory", get(memory_list).post(memory_upsert))
        .route("/api/memory/:memory_id", axum::routing::delete(memory_delete))
        .route("/api/goals", get(list_goals).post(create_goal))
        .route("/api/goals/:goal_id/tasks", get(list_goal_tasks))
        .route("/api/goals/:goal_id/cancel", post(cancel_goal))
        .route("/api/goals/:goal_id/retry", post(retry_goal))
        .route("/api/agents/:agent_id/credential", post(issue_agent_credential).delete(revoke_agent_credential))
        .route("/api/tasks/review", get(list_review_tasks))
        .route("/api/tasks/claim", post(claim_task))
        .route("/api/tasks/:task_id/complete", post(complete_task))
        .route("/api/tasks/:task_id/release", post(release_task))
        .route("/api/tasks/:task_id/heartbeat", post(heartbeat_task))
        .route("/api/tasks/:task_id/fail", post(fail_task))
        .with_state(state.clone())
        .layer(middleware::from_fn_with_state(state, rate_limit))
        .layer(DefaultBodyLimit::max(2_100_000))
        .layer(CorsLayer::new()
            .allow_origin([HeaderValue::from_static("tauri://localhost"), HeaderValue::from_static("http://tauri.localhost")])
            .allow_methods([Method::GET, Method::POST, Method::DELETE])
            .allow_headers([header::AUTHORIZATION, header::CONTENT_TYPE]))
        .layer(TraceLayer::new_for_http())
        .layer(middleware::from_fn(no_store_responses));

    if let (Some(cert_path), Some(key_path)) = (&config.tls_cert_path, &config.tls_key_path) {
        let tls = axum_server::tls_rustls::RustlsConfig::from_pem_file(cert_path, key_path).await?;
        tracing::info!(address = %config.bind, "dashboard available over HTTPS with native Rustls");
        axum_server::bind_rustls(config.bind, tls)
            .serve(app.into_make_service())
            .await?;
    } else {
        let listener = tokio::net::TcpListener::bind(config.bind).await?;
        tracing::info!(address = %listener.local_addr()?, "dashboard available on loopback HTTP");
        axum::serve(listener, app).await?;
    }
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
    let mut online_players = state.status.online_players.lock().await.iter().cloned().collect::<Vec<_>>();
    online_players.sort_unstable_by_key(|name| name.to_ascii_lowercase());
    let connection = state.status.connection.lock().await.clone();
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
        online_players,
        connection,
    }))
}

async fn chat(State(state): State<ApiState>, headers: HeaderMap, Json(request): Json<ChatRequest>) -> Result<Json<ChatResponse>, (StatusCode, String)> {
    authorize(&state, &headers).map_err(|code| (code, "unauthorized".into()))?;
    if request.message.trim().is_empty() || request.message.len() > 12_000 {
        return Err((StatusCode::BAD_REQUEST, "message must contain 1-12000 bytes".into()));
    }
    // Keep the conversation snapshot stable for the full provider round trip.
    // Failed or timed-out generations are not persisted as half-finished turns.
    let _chat_turn = state.chat_lock.lock().await;
    let context = state.store.recent_context(12, 6_000)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let world = state.status.world.lock().await.clone();
    let world_context = world.map(|w| format!("\nPrimary bot state: dimension {}, position {:?}, health {}/20, food {}/20, saturation {}, inventory: {:?}.", w.dimension, w.position, w.health, w.food, w.saturation, w.inventory)).unwrap_or_default();
    let mut agents = state.status.agents.lock().await.values().cloned().collect::<Vec<_>>();
    agents.sort_by(|left, right| left.agent.cmp(&right.agent));
    let team_context = format!(
        "\nConnected bot team: {}.",
        if agents.is_empty() {
            "none".to_owned()
        } else {
            agents.iter().map(|agent| format!(
                "{} ({}) at {:?} in {}, health {:.1}/20, food {}/20",
                agent.agent, agent.role, agent.position, agent.dimension, agent.health, agent.food
            )).collect::<Vec<_>>().join("; ")
        }
    );
    let mut online_players = state.status.online_players.lock().await.iter().cloned().collect::<Vec<_>>();
    online_players.sort_unstable_by_key(|name| name.to_ascii_lowercase());
    let player_context = format!("\nPlayers currently listed online: {}.", if online_players.is_empty() { "none".to_owned() } else { online_players.join(", ") });
    let memory_context = state.store.relevant_memory_context(&request.message, 4_000)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let prompt = format!("Recent conversation:\n{context}{world_context}{team_context}{player_context}\n{memory_context}\n\nLatest user message:\n{}", request.message);
    let reply = tokio::time::timeout(Duration::from_secs(50), state.brain.reply(&prompt))
        .await.map_err(|_| (StatusCode::GATEWAY_TIMEOUT, "AI provider timed out".into()))?
        .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?;
    state.store.append_exchange(&request.message, &reply)
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
        "\nCurrent Minecraft state for {} in dimension {}, biome {}: position {:?}, health {}/20, food {}/20, saturation {}, inventory {:?}.",
        world.agent, world.dimension, world.biome.as_deref().unwrap_or("unknown"), world.position, world.health, world.food, world.saturation, world.inventory
    )).unwrap_or_default();
    let mut connected_agents = state.status.agents.lock().await.values().cloned().collect::<Vec<_>>();
    connected_agents.sort_by(|left, right| left.agent.cmp(&right.agent));
    let connected_agents = connected_agents.iter()
        .map(|agent| format!(
            "{} ({}) at {:?} in {}, biome {}, health {:.1}/20, food {}/20",
            agent.agent, agent.role, agent.position, agent.dimension,
            agent.biome.as_deref().unwrap_or("unknown"), agent.health, agent.food
        ))
        .collect::<Vec<_>>();
    let team_context = format!(
        "\nConnected Minecraft team ({}): {}.",
        connected_agents.len(),
        if connected_agents.is_empty() { "none".to_owned() } else { connected_agents.join(", ") }
    );
    let mut online_players = state.status.online_players.lock().await.iter().cloned().collect::<Vec<_>>();
    online_players.sort_unstable_by_key(|name| name.to_ascii_lowercase());
    let player_context = format!(
        "\nOther players currently listed online: {}.",
        if online_players.is_empty() { "none".to_owned() } else { online_players.join(", ") }
    );
    let memory_context = state.store.relevant_memory_context(&request.message, 3_000)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let planning_request = format!("Player request: {}{world_context}{team_context}{player_context}\n{memory_context}", request.message);
    let proposed_action = tokio::time::timeout(Duration::from_secs(50), state.brain.plan(&planning_request))
        .await.map_err(|_| (StatusCode::GATEWAY_TIMEOUT, "planner timed out".into()))?
        .map_err(|e| (StatusCode::UNPROCESSABLE_ENTITY, e.to_string()))?;
    // Validate the concrete executor payload before presenting a proposal. This
    // prevents the UI from offering approval for malformed coordinates, recipes,
    // or other actions that the runtime cannot actually dispatch.
    let planned_command = command_from_action(proposed_action.clone())?;

    if state.config.mode == "autonomous" && state.brain.authorize_action(&proposed_action, false).is_ok() {
        if proposed_action.action.eq_ignore_ascii_case("smelt_parallel") {
            let BotCommand::SmeltQueue(jobs) = planned_command else {
                return Err((StatusCode::INTERNAL_SERVER_ERROR, "parallel smelting planner returned an unexpected command".into()));
            };
            dispatch_parallel_smelt_jobs(&state, jobs, target.clone()).await?;
            return Ok(Json(PlanResponse {
                proposed_action,
                execution: "AUTONOMOUS: parallel furnace jobs dispatched to separate connected bots",
                auto_queued: true,
            }));
        }
        if let Some(sender) = state.status.command_tx.lock().await.clone() {
            ensure_agent_connected(&state, target.as_deref()).await?;
            sender.send(AgentCommand { target: target.clone(), command: planned_command }).map_err(|_| (StatusCode::SERVICE_UNAVAILABLE, "Minecraft command stream is disconnected".into()))?;
            return Ok(Json(PlanResponse {
                proposed_action,
                execution: "AUTONOMOUS: command queued without manual approval",
                auto_queued: true,
            }));
        }
    }

    let execution = if state.config.mode == "autonomous" {
        "Minecraft client unavailable or action has no executor; review or retry after connection"
    } else {
        "proposal only; explicitly approve it to execute"
    };
    Ok(Json(PlanResponse { proposed_action, execution, auto_queued: false }))
}


async fn build_schematic(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(request): Json<SchematicBuildRequest>,
) -> Result<Json<ExecuteResponse>, (StatusCode, String)> {
    authorize(&state, &headers).map_err(|code| (code, "unauthorized".into()))?;
    state.brain.authorize_action(
        &PlannedAction {
            action: "build".into(),
            target: None,
            reason: "explicit authenticated schematic build request".into(),
            risk: "high".into(),
        },
        true,
    ).map_err(|error| (StatusCode::FORBIDDEN, error.to_string()))?;
    let document = SchematicDocument {
        blocks: request.blocks,
        rotation: request.rotation,
        mirror_x: request.mirror_x,
        mirror_z: request.mirror_z,
    };
    let json = serde_json::to_string(&document)
        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
    let origin = format!("{},{},{}", request.origin[0], request.origin[1], request.origin[2]);
    let raw = format!("schematic:{json}|{origin}");
    let blocks = parse_schematic_spec(&raw)?;
    let target = resolve_agent_target(&state.config, request.agent)?;
    ensure_agent_connected(&state, target.as_deref()).await?;
    let sender = state.status.command_tx.lock().await.clone()
        .ok_or((StatusCode::SERVICE_UNAVAILABLE, "Minecraft bot is not connected".into()))?;
    sender.send(AgentCommand { target, command: BotCommand::BuildMixed(blocks) })
        .map_err(|_| (StatusCode::SERVICE_UNAVAILABLE, "Minecraft command stream is disconnected".into()))?;
    Ok(Json(ExecuteResponse { accepted: true, message: "mixed-material schematic queued for the Minecraft runtime" }))
}

fn decode_sponge_varints(data: &[i8], expected: usize) -> Result<Vec<u32>, (StatusCode, String)> {
    let mut output = Vec::with_capacity(expected);
    let bytes = data.iter().map(|value| *value as u8).collect::<Vec<_>>();
    let mut cursor = 0usize;
    while cursor < bytes.len() && output.len() < expected {
        let mut value = 0u32;
        let mut shift = 0u32;
        let mut complete = false;
        for _ in 0..5 {
            let byte = *bytes.get(cursor)
                .ok_or((StatusCode::BAD_REQUEST, "Sponge schematic BlockData ended inside a VarInt".into()))?;
            cursor += 1;
            if shift == 28 && byte & 0xf0 != 0 {
                return Err((StatusCode::BAD_REQUEST, "Sponge schematic contains an overflowing 32-bit palette VarInt".into()));
            }
            value |= u32::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                complete = true;
                break;
            }
            shift += 7;
        }
        if !complete {
            return Err((StatusCode::BAD_REQUEST, "Sponge schematic contains an invalid or overlong block palette VarInt".into()));
        }
        output.push(value);
    }
    if output.len() != expected || cursor != bytes.len() {
        return Err((StatusCode::BAD_REQUEST, "Sponge schematic BlockData length does not match its declared dimensions".into()));
    }
    Ok(output)
}

fn schematic_axis_supported(block: &str) -> bool {
    block.ends_with("_log")
        || block.ends_with("_wood")
        || block.ends_with("_stem")
        || block.ends_with("_hyphae")
        || matches!(
            block,
            "bamboo_block"
                | "stripped_bamboo_block"
                | "basalt"
                | "polished_basalt"
                | "smooth_basalt"
                | "bone_block"
                | "hay_block"
                | "quartz_pillar"
                | "purpur_pillar"
                | "chain"
        )
}

fn sponge_block_item(state: &str) -> Result<Option<(String, Option<String>, Option<bool>)>, (StatusCode, String)> {
    let (block_state, axis, slab_top) = if let Some((block_state, raw_properties)) = state.split_once('[') {
        let properties = raw_properties.strip_suffix(']')
            .filter(|properties| !properties.contains('[') && !properties.contains(']'))
            .ok_or((StatusCode::BAD_REQUEST, "Sponge schematic contains malformed block-state properties".into()))?;
        let mut axis = None;
        let mut slab_top = None;
        let block_id = block_state.rsplit(':').next().unwrap_or(block_state);
        for entry in properties.split(',') {
            let (key, value) = entry.split_once('=')
                .ok_or((StatusCode::BAD_REQUEST, "Sponge block-state properties must use key=value syntax".into()))?;
            match key {
                "axis" if axis.is_none() && matches!(value, "x" | "y" | "z") => {
                    axis = Some(value.to_owned());
                }
                "type" if slab_top.is_none() && block_id.ends_with("_slab") && matches!(value, "top" | "bottom") => {
                    slab_top = Some(value == "top");
                }
                // These explicit false values match the default placement state.
                "waterlogged" | "open" | "powered" | "triggered" | "extended" | "snowy"
                    if value == "false" => {}
                _ => {
                    return Err((StatusCode::NOT_IMPLEMENTED, format!(
                        "Sponge block state '{state}' uses a property the placement executor cannot reproduce"
                    )));
                }
            }
        }
        (block_state, axis, slab_top)
    } else {
        if state.contains(']') {
            return Err((StatusCode::BAD_REQUEST, "Sponge schematic contains malformed block-state properties".into()));
        }
        (state, None, None)
    };
    let (namespace, block) = block_state.split_once(':').unwrap_or(("minecraft", block_state));
    if namespace != "minecraft" {
        return Err((StatusCode::NOT_IMPLEMENTED, format!("Sponge schematic block namespace '{namespace}' is not supported")));
    }
    if !block.chars().all(|character| character.is_ascii_lowercase() || character.is_ascii_digit() || character == '_')
        || block.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "Sponge schematic contains an invalid block-state identifier".into()));
    }
    if axis.is_some() && !schematic_axis_supported(block) {
        return Err((StatusCode::NOT_IMPLEMENTED, format!("Sponge block '{block}' does not support the axis property in the current placement executor")));
    }
    if matches!(block, "air" | "cave_air" | "void_air") {
        return Ok(None);
    }
    if matches!(block, "water" | "lava" | "bubble_column" | "fire" | "soul_fire" | "nether_portal" | "end_portal" | "end_gateway") {
        return Err((StatusCode::NOT_IMPLEMENTED, format!("Sponge schematic block '{block}' requires fluid, fire, or portal-state placement and cannot be reproduced by the current item-placement executor")));
    }
    let item = if block == "wall_torch" {
        "torch".to_owned()
    } else if block == "redstone_wall_torch" {
        "redstone_torch".to_owned()
    } else if block == "soul_wall_torch" {
        "soul_torch".to_owned()
    } else if block == "wall_sign" {
        "oak_sign".to_owned()
    } else if let Some(prefix) = block.strip_suffix("_wall_sign") {
        format!("{prefix}_sign")
    } else if block == "wall_hanging_sign" {
        "oak_hanging_sign".to_owned()
    } else if let Some(prefix) = block.strip_suffix("_wall_hanging_sign") {
        format!("{prefix}_hanging_sign")
    } else if block == "wall_banner" {
        "white_banner".to_owned()
    } else if let Some(prefix) = block.strip_suffix("_wall_banner") {
        format!("{prefix}_banner")
    } else {
        block.to_owned()
    };
    Ok(Some((item, axis, slab_top)))
}

fn decode_sponge_schematic(encoded: &str) -> Result<Vec<SchematicBlock>, (StatusCode, String)> {
    if encoded.is_empty() || encoded.len() > MAX_SPONGE_BASE64_BYTES {
        return Err((StatusCode::PAYLOAD_TOO_LARGE, "Sponge schematic Base64 payload must be 1-1,900,000 bytes".into()));
    }
    let compressed = BASE64_STANDARD.decode(encoded)
        .map_err(|_| (StatusCode::BAD_REQUEST, "Sponge schematic data_base64 is not valid standard Base64".into()))?;
    let nbt_bytes = if compressed.starts_with(&[0x1f, 0x8b]) {
        let mut decoder = GzDecoder::new(compressed.as_slice());
        let mut decoded = Vec::new();
        decoder.take((MAX_SPONGE_NBT_BYTES + 1) as u64).read_to_end(&mut decoded)
            .map_err(|_| (StatusCode::BAD_REQUEST, "Sponge schematic gzip stream is invalid".into()))?;
        decoded
    } else {
        compressed
    };
    if nbt_bytes.is_empty() || nbt_bytes.len() > MAX_SPONGE_NBT_BYTES {
        return Err((StatusCode::PAYLOAD_TOO_LARGE, "decompressed Sponge schematic NBT exceeds the 4 MiB safety limit".into()));
    }
    let root: SpongeSchematicNbt = fastnbt::from_bytes(&nbt_bytes)
        .map_err(|error| (StatusCode::BAD_REQUEST, format!("Sponge schematic NBT could not be decoded: {error}")))?;
    if !(2..=3).contains(&root.version) {
        return Err((StatusCode::NOT_IMPLEMENTED, "only Sponge schematic format versions 2 and 3 are supported".into()));
    }
    let width = usize::try_from(root.width).ok().filter(|value| (1..=33).contains(value))
        .ok_or((StatusCode::BAD_REQUEST, "Sponge schematic width must be 1-33 blocks".into()))?;
    let height = usize::try_from(root.height).ok().filter(|value| (1..=33).contains(value))
        .ok_or((StatusCode::BAD_REQUEST, "Sponge schematic height must be 1-33 blocks".into()))?;
    let length = usize::try_from(root.length).ok().filter(|value| (1..=33).contains(value))
        .ok_or((StatusCode::BAD_REQUEST, "Sponge schematic length must be 1-33 blocks".into()))?;
    let volume = width.checked_mul(height).and_then(|value| value.checked_mul(length))
        .filter(|value| *value <= 35_937)
        .ok_or((StatusCode::BAD_REQUEST, "Sponge schematic dimensions exceed the supported volume".into()))?;
    let offset = root.offset.map(fastnbt::IntArray::into_inner).unwrap_or_else(|| vec![0, 0, 0]);
    if offset.len() != 3 || offset.iter().any(|value| value.unsigned_abs() > 32) {
        return Err((StatusCode::BAD_REQUEST, "Sponge schematic Offset must contain three values within -32..32".into()));
    }
    let (palette, block_data) = if let Some(blocks) = root.blocks {
        (blocks.palette, blocks.data)
    } else {
        (
            root.palette.ok_or((StatusCode::BAD_REQUEST, "Sponge schematic has no Palette compound".into()))?,
            root.block_data.ok_or((StatusCode::BAD_REQUEST, "Sponge schematic has no BlockData byte array".into()))?,
        )
    };
    if palette.is_empty() || palette.len() > 4096 {
        return Err((StatusCode::BAD_REQUEST, "Sponge schematic palette must contain 1-4096 entries".into()));
    }
    let mut palette_by_id = HashMap::with_capacity(palette.len());
    for (state, id) in palette {
        if id < 0 || palette_by_id.insert(id as u32, state).is_some() {
            return Err((StatusCode::BAD_REQUEST, "Sponge schematic palette contains a negative or duplicate palette ID".into()));
        }
    }
    let block_ids = decode_sponge_varints(&block_data, volume)?;
    let mut blocks = Vec::new();
    for (index, palette_id) in block_ids.into_iter().enumerate() {
        let state = palette_by_id.get(&palette_id)
            .ok_or((StatusCode::BAD_REQUEST, "Sponge schematic BlockData references an unknown palette ID".into()))?;
        let Some((material, axis, slab_top)) = sponge_block_item(state)? else { continue; };
        if blocks.len() >= 256 {
            return Err((StatusCode::PAYLOAD_TOO_LARGE, "Sponge schematic contains more than 256 non-air blocks; split it into smaller builds".into()));
        }
        let x = index % width;
        let z = (index / width) % length;
        let y = index / (width * length);
        blocks.push(SchematicBlock { x: x as i32 + offset[0], y: y as i32 + offset[1], z: z as i32 + offset[2], material, axis, slab_top });
    }
    if blocks.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "Sponge schematic contains no placeable non-air blocks".into()));
    }
    Ok(blocks)
}

async fn build_sponge_schematic(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(request): Json<SpongeSchematicBuildRequest>,
) -> Result<Json<ExecuteResponse>, (StatusCode, String)> {
    authorize(&state, &headers).map_err(|code| (code, "unauthorized".into()))?;
    state.brain.authorize_action(
        &PlannedAction {
            action: "build".into(),
            target: None,
            reason: "explicit authenticated Sponge schematic import".into(),
            risk: "high".into(),
        },
        true,
    ).map_err(|error| (StatusCode::FORBIDDEN, error.to_string()))?;
    if request.origin[1] < -64 || request.origin[1] > 320
        || request.origin.iter().any(|value| value.unsigned_abs() > 30_000_000) {
        return Err((StatusCode::BAD_REQUEST, "Sponge schematic origin is outside supported world coordinates".into()));
    }
    let document = SchematicDocument {
        blocks: decode_sponge_schematic(&request.data_base64)?,
        rotation: request.rotation,
        mirror_x: request.mirror_x,
        mirror_z: request.mirror_z,
    };
    let json = serde_json::to_string(&document)
        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
    let origin = format!("{},{},{}", request.origin[0], request.origin[1], request.origin[2]);
    let blocks = parse_schematic_spec(&format!("schematic:{json}|{origin}"))?;
    let target = resolve_agent_target(&state.config, request.agent)?;
    ensure_agent_connected(&state, target.as_deref()).await?;
    let sender = state.status.command_tx.lock().await.clone()
        .ok_or((StatusCode::SERVICE_UNAVAILABLE, "Minecraft bot is not connected".into()))?;
    sender.send(AgentCommand { target, command: BotCommand::BuildMixed(blocks) })
        .map_err(|_| (StatusCode::SERVICE_UNAVAILABLE, "Minecraft command stream is disconnected".into()))?;
    Ok(Json(ExecuteResponse { accepted: true, message: "bounded Sponge schematic converted and queued for the Minecraft runtime" }))
}

async fn execute(State(state): State<ApiState>, headers: HeaderMap, Json(request): Json<ExecuteRequest>) -> Result<Json<ExecuteResponse>, (StatusCode, String)> {
    authorize(&state, &headers).map_err(|code| (code, "unauthorized".into()))?;
    let action_name = request.action.action.to_ascii_lowercase();
    state.brain.authorize_action(&request.action, true)
        .map_err(|e| (StatusCode::FORBIDDEN, e.to_string()))?;
    let command = command_from_action(request.action)?;
    if action_name == "smelt_parallel" {
        let BotCommand::SmeltQueue(jobs) = command else {
            return Err((StatusCode::INTERNAL_SERVER_ERROR, "parallel smelting parser returned an unexpected command".into()));
        };
        dispatch_parallel_smelt_jobs(&state, jobs, request.agent).await?;
        return Ok(Json(ExecuteResponse { accepted: true, message: "parallel furnace jobs dispatched to separate connected bots" }));
    }
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

async fn dispatch_parallel_smelt_jobs(
    state: &ApiState,
    jobs: Vec<SmeltJob>,
    explicit_agent: Option<String>,
) -> Result<(), (StatusCode, String)> {
    if explicit_agent.as_deref().is_some_and(|agent| !agent.trim().is_empty()) {
        return Err((StatusCode::BAD_REQUEST, "smelt_parallel assigns one furnace job per connected bot; omit the agent field".into()));
    }
    let connected = {
        let snapshots = state.status.agents.lock().await;
        state.config.minecraft_accounts.iter().filter_map(|configured| {
            snapshots.keys().find(|connected| connected.eq_ignore_ascii_case(configured)).cloned()
        }).collect::<Vec<_>>()
    };
    if connected.len() < jobs.len() {
        return Err((
            StatusCode::CONFLICT,
            format!("smelt_parallel needs {} connected bots for {} distinct furnace jobs; only {} are connected; connect more bots or use smelt_queue to stage multiple furnaces on one bot", jobs.len(), jobs.len(), connected.len()),
        ));
    }
    let sender = state.status.command_tx.lock().await.clone()
        .ok_or((StatusCode::SERVICE_UNAVAILABLE, "Minecraft command stream is disconnected".into()))?;
    for (job, agent) in jobs.into_iter().zip(connected.into_iter()) {
        sender.send(AgentCommand {
            target: Some(agent),
            command: BotCommand::Smelt(job.input, (job.x, job.y, job.z), job.count),
        }).map_err(|_| (StatusCode::SERVICE_UNAVAILABLE, "Minecraft command stream is disconnected".into()))?;
    }
    Ok(())
}

fn resolve_agent_target(config: &Config, target: Option<String>) -> Result<Option<String>, (StatusCode, String)> {
    let Some(target) = target.filter(|value| !value.trim().is_empty()) else { return Ok(None); };
    config.minecraft_accounts.iter().find(|name| name.eq_ignore_ascii_case(&target))
        .cloned().map(Some)
        .ok_or((StatusCode::BAD_REQUEST, "agent must match one of the configured Minecraft bot usernames".into()))
}

fn parse_build_targets(raw: &str) -> Result<Vec<(i32, i32, i32)>, (StatusCode, String)> {
    let entries = raw.split(';').map(str::trim).filter(|entry| !entry.is_empty()).collect::<Vec<_>>();
    if entries.is_empty() || entries.len() > 256 {
        return Err((StatusCode::BAD_REQUEST, "build target must contain 1-256 semicolon-separated x,y,z block positions".into()));
    }
    let mut unique = HashSet::new();
    let mut positions = Vec::with_capacity(entries.len());
    for entry in entries {
        let values = entry.split(',').map(str::trim).map(str::parse::<i32>)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| (StatusCode::BAD_REQUEST, "each build position must use integer x,y,z coordinates".into()))?;
        if values.len() != 3 || values[1] < -64 || values[1] > 320
            || values.iter().any(|value| value.unsigned_abs() > 30_000_000) {
            return Err((StatusCode::BAD_REQUEST, "build positions must be inside supported Minecraft world bounds".into()));
        }
        let position = (values[0], values[1], values[2]);
        if !unique.insert(position) {
            return Err((StatusCode::BAD_REQUEST, "build plan cannot contain duplicate block positions".into()));
        }
        positions.push(position);
    }
    // Place lower layers first so a block in this plan can support a later
    // block above it. The executor still refuses unsupported side placements.
    positions.sort_by_key(|(x, y, z)| (*y, *x, *z));
    Ok(positions)
}

fn parse_blueprint_origin(raw: &str) -> Result<(i32, i32, i32), (StatusCode, String)> {
    let parsed = raw.split(',').map(str::trim).map(str::parse::<i32>)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| (StatusCode::BAD_REQUEST, "blueprint origin must be integer x,y,z coordinates".into()))?;
    if parsed.len() != 3 || parsed[1] < -64 || parsed[1] > 320
        || parsed.iter().any(|value| value.unsigned_abs() > 30_000_000) {
        return Err((StatusCode::BAD_REQUEST, "blueprint origin is outside supported Minecraft world bounds".into()));
    }
    Ok((parsed[0], parsed[1], parsed[2]))
}

fn expand_blueprint(name: &str, origin: (i32, i32, i32)) -> Result<Vec<(i32, i32, i32)>, (StatusCode, String)> {
    let (ox, oy, oz) = origin;
    // Every built-in blueprint fits within a 16-block positive offset.
    // Validate the margin before adding offsets so malicious edge-of-world
    // origins cannot overflow i32 in debug or release builds.
    if ox < -30_000_000 || ox > 29_999_984
        || oz < -30_000_000 || oz > 29_999_984
        || !(-64..=304).contains(&oy) {
        return Err((StatusCode::BAD_REQUEST, "blueprint origin must leave a 16-block margin inside supported Minecraft world bounds".into()));
    }
    let mut positions = Vec::new();
    match name.trim().to_ascii_lowercase().as_str() {
        "platform_5x5" => {
            for x in 0..5 { for z in 0..5 { positions.push((ox + x, oy, oz + z)); } }
        }
        "wall_7x3" => {
            for x in 0..7 { for y in 0..3 { positions.push((ox + x, oy + y, oz)); } }
        }
        "starter_hut" => {
            // Compact 3x3 shelter shell: floor plus two wall layers.
            // Leave a two-block opening in the front-center for a door.
            for x in 0..3 { for z in 0..3 { positions.push((ox + x, oy, oz + z)); } }
            for y in 1..=2 {
                for x in 0..3 {
                    for z in 0..3 {
                        let perimeter = x == 0 || x == 2 || z == 0 || z == 2;
                        let doorway = z == 0 && x == 1;
                        if perimeter && !doorway { positions.push((ox + x, oy + y, oz + z)); }
                    }
                }
            }
        }
        "starter_house_4x4" => {
            // A larger one-material shelter: 4x4 floor, two-high perimeter,
            // a two-block front doorway, and a flat 4x4 roof (54 blocks).
            for x in 0..4 { for z in 0..4 { positions.push((ox + x, oy, oz + z)); } }
            for y in 1..=2 {
                for x in 0..4 {
                    for z in 0..4 {
                        let perimeter = x == 0 || x == 3 || z == 0 || z == 3;
                        let doorway = z == 0 && x == 1;
                        if perimeter && !doorway { positions.push((ox + x, oy + y, oz + z)); }
                    }
                }
            }
            for x in 0..4 { for z in 0..4 { positions.push((ox + x, oy + 3, oz + z)); } }
        }
        "bridge_7x3" => {
            // Flat seven-by-three crossing; the executor still validates
            // every support face and stops if a span cannot be supported.
            for x in 0..7 { for z in 0..3 { positions.push((ox + x, oy, oz + z)); } }
        }
        "platform_11x11" => {
            for x in 0..11 { for z in 0..11 { positions.push((ox + x, oy, oz + z)); } }
        }
        "bridge_15x3" => {
            for x in 0..15 { for z in 0..3 { positions.push((ox + x, oy, oz + z)); } }
        }
        "starter_house_5x5" => {
            for x in 0..5 { for z in 0..5 { positions.push((ox + x, oy, oz + z)); } }
            for y in 1..=3 {
                for x in 0..5 {
                    for z in 0..5 {
                        let perimeter = x == 0 || x == 4 || z == 0 || z == 4;
                        let doorway = z == 0 && x == 2 && y <= 2;
                        if perimeter && !doorway { positions.push((ox + x, oy + y, oz + z)); }
                    }
                }
            }
            for x in 0..5 { for z in 0..5 { positions.push((ox + x, oy + 4, oz + z)); } }
        }
        "watchtower_5x5" => {
            for x in 0..5 { for z in 0..5 { positions.push((ox + x, oy, oz + z)); } }
            for y in 1..=3 {
                for x in 0..5 {
                    for z in 0..5 {
                        let perimeter = x == 0 || x == 4 || z == 0 || z == 4;
                        let doorway = z == 0 && x == 2 && y <= 2;
                        if perimeter && !doorway { positions.push((ox + x, oy + y, oz + z)); }
                    }
                }
            }
            for x in 0..5 { for z in 0..5 {
                if x == 0 || x == 4 || z == 0 || z == 4 {
                    positions.push((ox + x, oy + 4, oz + z));
                }
            }}
            for x in 0..5 { for z in 0..5 { positions.push((ox + x, oy + 5, oz + z)); } }
        }
        "perimeter_wall_11x11x3" => {
            for y in 0..3 {
                for x in 0..11 {
                    for z in 0..11 {
                        let perimeter = x == 0 || x == 10 || z == 0 || z == 10;
                        let doorway = z == 0 && x == 5 && y < 2;
                        if perimeter && !doorway { positions.push((ox + x, oy + y, oz + z)); }
                    }
                }
            }
        }
        "fortified_courtyard_11x11x3" => {
            for x in 0..11 { for z in 0..11 { positions.push((ox + x, oy, oz + z)); } }
            for y in 1..=3 {
                for x in 0..11 {
                    for z in 0..11 {
                        let perimeter = x == 0 || x == 10 || z == 0 || z == 10;
                        let doorway = z == 0 && x == 5 && y < 3;
                        if perimeter && !doorway { positions.push((ox + x, oy + y, oz + z)); }
                    }
                }
            }
        }
        "watchtower_3x3" => {
            // Compact 3x3 tower with a two-block doorway and a full roof.
            for x in 0..3 { for z in 0..3 { positions.push((ox + x, oy, oz + z)); } }
            for y in 1..=2 {
                for x in 0..3 {
                    for z in 0..3 {
                        let perimeter = x == 0 || x == 2 || z == 0 || z == 2;
                        let doorway = z == 0 && x == 1;
                        if perimeter && !doorway { positions.push((ox + x, oy + y, oz + z)); }
                    }
                }
            }
            for x in 0..3 { for z in 0..3 {
                if x == 0 || x == 2 || z == 0 || z == 2 {
                    positions.push((ox + x, oy + 3, oz + z));
                }
            }}
            for x in 0..3 { for z in 0..3 { positions.push((ox + x, oy + 4, oz + z)); } }
        }
        "perimeter_wall_7x7x2" => {
            // Two-block-high square enclosure with a two-block front opening.
            for y in 0..2 {
                for x in 0..7 {
                    for z in 0..7 {
                        let perimeter = x == 0 || x == 6 || z == 0 || z == 6;
                        let doorway = z == 0 && x == 3;
                        if perimeter && !(doorway && y < 2) {
                            positions.push((ox + x, oy + y, oz + z));
                        }
                    }
                }
            }
        }
        _ => return Err((StatusCode::BAD_REQUEST, "unknown blueprint; choose platform_5x5, platform_11x11, wall_7x3, starter_hut, starter_house_4x4, starter_house_5x5, bridge_7x3, bridge_15x3, watchtower_3x3, watchtower_5x5, perimeter_wall_7x7x2, perimeter_wall_11x11x3, or fortified_courtyard_11x11x3".into())),
    }
    positions.sort_by_key(|(x, y, z)| (*y, *x, *z));
    if positions.len() > 256 {
        return Err((StatusCode::BAD_REQUEST, "expanded blueprint exceeds the 256-block build limit".into()));
    }
    Ok(positions)
}


#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SchematicDocument {
    blocks: Vec<SchematicBlock>,
    #[serde(default)]
    rotation: u16,
    #[serde(default)]
    mirror_x: bool,
    #[serde(default)]
    mirror_z: bool,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SchematicBlock {
    x: i32,
    y: i32,
    z: i32,
    material: String,
    #[serde(default)]
    axis: Option<String>,
    #[serde(default)]
    slab_top: Option<bool>,
}

/// Parse a compact JSON schematic with relative coordinates and an absolute
/// origin. The 256-block and 32-block relative bounds keep model/API payloads
/// bounded and prevent accidental world-scale construction plans.
fn parse_schematic_spec(raw: &str) -> Result<Vec<BuildBlock>, (StatusCode, String)> {
    let spec = raw.strip_prefix("schematic:")
        .ok_or((StatusCode::BAD_REQUEST, "schematic target must start with schematic:".into()))?;
    if spec.len() > 48_000 {
        return Err((StatusCode::BAD_REQUEST, "schematic JSON exceeds the 48000-byte limit".into()));
    }
    let (json, origin_raw) = spec.split_once('|')
        .ok_or((StatusCode::BAD_REQUEST, "schematic syntax is schematic:{\"blocks\":[...]}|origin_x,origin_y,origin_z".into()))?;
    if origin_raw.contains('|') {
        return Err((StatusCode::BAD_REQUEST, "schematic accepts exactly one origin separator".into()));
    }
    let (ox, oy, oz) = parse_blueprint_origin(origin_raw)?;
    let document: SchematicDocument = serde_json::from_str(json)
        .map_err(|_| (StatusCode::BAD_REQUEST, "schematic must be valid JSON with a blocks array".into()))?;
    if document.blocks.is_empty() || document.blocks.len() > 256 {
        return Err((StatusCode::BAD_REQUEST, "schematic must contain 1-256 blocks".into()));
    }
    if ![0, 90, 180, 270].contains(&document.rotation) {
        return Err((StatusCode::BAD_REQUEST, "schematic rotation must be 0, 90, 180, or 270 degrees".into()));
    }
    let rotation = document.rotation;
    let mirror_x = document.mirror_x;
    let mirror_z = document.mirror_z;
    let mut seen = HashSet::new();
    let mut blocks = Vec::with_capacity(document.blocks.len());
    for block in document.blocks {
        let material = block.material.trim().to_ascii_lowercase();
        if material.is_empty() || material.len() > 32
            || !material.chars().all(|character| character.is_ascii_alphanumeric() || character == '_')
            || material == "air" {
            return Err((StatusCode::BAD_REQUEST, "each schematic material must be an exact non-air item name using letters, numbers, and underscores".into()));
        }
        let mut axis = block.axis.map(|value| value.trim().to_ascii_lowercase());
        if axis.as_deref().is_some_and(|value| !matches!(value, "x" | "y" | "z")) {
            return Err((StatusCode::BAD_REQUEST, "schematic axis must be x, y, or z".into()));
        }
        if axis.is_some() && !schematic_axis_supported(&material) {
            return Err((StatusCode::BAD_REQUEST, format!("schematic material '{material}' does not support an axis property")));
        }
        let slab_top = block.slab_top;
        if slab_top.is_some() && !material.ends_with("_slab") {
            return Err((StatusCode::BAD_REQUEST, format!("schematic material '{material}' does not support a top/bottom slab state")));
        }
        if axis.is_some() && slab_top.is_some() {
            return Err((StatusCode::BAD_REQUEST, "a schematic block cannot specify both axis and slab_top".into()));
        }
        if matches!(rotation, 90 | 270) {
            axis = axis.map(|value| match value.as_str() {
                "x" => "z".to_owned(),
                "z" => "x".to_owned(),
                _ => value,
            });
        }
        if [block.x, block.y, block.z].iter().any(|value| value.unsigned_abs() > 32) {
            return Err((StatusCode::BAD_REQUEST, "schematic-relative coordinates must stay within 32 blocks of the origin".into()));
        }
        let mut relative_x = if mirror_x { -block.x } else { block.x };
        let mut relative_z = if mirror_z { -block.z } else { block.z };
        (relative_x, relative_z) = match rotation {
            0 => (relative_x, relative_z),
            90 => (-relative_z, relative_x),
            180 => (-relative_x, -relative_z),
            270 => (relative_z, -relative_x),
            _ => unreachable!("rotation was validated above"),
        };
        let x = ox.checked_add(relative_x).ok_or((StatusCode::BAD_REQUEST, "schematic x coordinate overflow".into()))?;
        let y = oy.checked_add(block.y).ok_or((StatusCode::BAD_REQUEST, "schematic y coordinate overflow".into()))?;
        let z = oz.checked_add(relative_z).ok_or((StatusCode::BAD_REQUEST, "schematic z coordinate overflow".into()))?;
        if !(-64..=320).contains(&y) || [x, y, z].iter().any(|value| value.unsigned_abs() > 30_000_000) {
            return Err((StatusCode::BAD_REQUEST, "schematic block is outside supported Minecraft world bounds".into()));
        }
        if !seen.insert((x, y, z)) {
            return Err((StatusCode::BAD_REQUEST, "schematic cannot contain duplicate block coordinates".into()));
        }
        blocks.push(BuildBlock { material, x, y, z, axis, slab_top });
    }
    blocks.sort_by_key(|block| (block.y, block.x, block.z));
    Ok(blocks)
}

fn parse_build_spec(raw: &str) -> Result<(Option<String>, Vec<(i32, i32, i32)>), (StatusCode, String)> {
    let (material, coordinates) = match raw.split_once('@') {
        Some((material, coordinates)) if !material.trim().to_ascii_lowercase().starts_with("blueprint:") => {
            let material = material.trim().to_ascii_lowercase();
            if material.is_empty() || material.len() > 32
                || !material.chars().all(|character| character.is_ascii_alphanumeric() || character == '_') {
                return Err((StatusCode::BAD_REQUEST, "build material must be an exact item name using letters, numbers, and underscores".into()));
            }
            (Some(material), coordinates)
        }
        _ => (None, raw),
    };
    let blueprint_spec = coordinates.strip_prefix("blueprint:")
        .or_else(|| raw.strip_prefix("blueprint:"));
    if let Some(spec) = blueprint_spec {
        let (name, origin) = spec.split_once('@')
            .ok_or((StatusCode::BAD_REQUEST, "blueprint syntax is blueprint:name@x,y,z, optionally prefixed with material@".into()))?;
        if origin.contains('@') {
            return Err((StatusCode::BAD_REQUEST, "blueprint accepts one origin coordinate".into()));
        }
        let positions = expand_blueprint(name, parse_blueprint_origin(origin)?)?;
        return Ok((material, positions));
    }
    if coordinates.contains('@') {
        return Err((StatusCode::BAD_REQUEST, "build accepts one material@ prefix or one blueprint origin".into()));
    }
    Ok((material, parse_build_targets(coordinates)?))
}


fn parse_smelt_job(raw: &str) -> Result<SmeltJob, (StatusCode, String)> {
    let (input_spec, coordinates) = raw.split_once('@')
        .ok_or((StatusCode::BAD_REQUEST, "smelt job must use input[*count]@x,y,z syntax".into()))?;
    if coordinates.contains('@') {
        return Err((StatusCode::BAD_REQUEST, "smelt job accepts exactly one furnace coordinate".into()));
    }
    let (input, count) = match input_spec.split_once('*') {
        Some((input, count)) => {
            if count.contains('*') {
                return Err((StatusCode::BAD_REQUEST, "smelt batch count must be a single integer from 1 to 64".into()));
            }
            let count = count.trim().parse::<u8>()
                .map_err(|_| (StatusCode::BAD_REQUEST, "smelt batch count must be an integer from 1 to 64".into()))?;
            if !(1..=64).contains(&count) {
                return Err((StatusCode::BAD_REQUEST, "smelt batch count must be from 1 to 64".into()));
            }
            (input, count)
        }
        None => (input_spec, 1),
    };
    let input = input.trim().to_ascii_lowercase();
    if smelt_spec(&input).is_none() {
        return Err((StatusCode::NOT_IMPLEMENTED, "that input is not in the runtime's explicit smelting allowlist".into()));
    }
    let parsed = coordinates.split(',').map(str::trim).map(str::parse::<i32>)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| (StatusCode::BAD_REQUEST, "smelt furnace target must be x,y,z".into()))?;
    if parsed.len() != 3 || parsed[1] < -64 || parsed[1] > 320
        || parsed.iter().any(|value| value.unsigned_abs() > 30_000_000) {
        return Err((StatusCode::BAD_REQUEST, "smelt furnace coordinates are outside supported world bounds".into()));
    }
    Ok(SmeltJob { input, x: parsed[0], y: parsed[1], z: parsed[2], count })
}

pub(crate) fn command_from_action(action: PlannedAction) -> Result<BotCommand, (StatusCode, String)> {
    match action.action.to_lowercase().as_str() {
        "say" => {
            let text = action.target.unwrap_or(action.reason);
            if text.trim().is_empty() || text.encode_utf16().count() > 240
                || text.trim_start().starts_with('/') || text.chars().any(|c| c.is_control()) {
                return Err((StatusCode::BAD_REQUEST, "say accepts 1-240 UTF-16 code units of plain chat text; slash commands and control characters are blocked".into()));
            }
            Ok(BotCommand::Say(text))
        }
        "stop" | "stay" => Ok(BotCommand::Stop),
        "jump" => Ok(BotCommand::Jump),
        "craftable" => Ok(BotCommand::Craftable),
        "craft_plan" => {
            let recipe = action.target.ok_or((StatusCode::BAD_REQUEST, "target must be a supported recipe name".into()))?;
            let recipe = recipe.trim().to_ascii_lowercase();
            if recipe.is_empty() || recipe.len() > 64
                || !recipe.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'_') {
                return Err((StatusCode::BAD_REQUEST, "craft_plan requires a 1-64 character recipe identifier".into()));
            }
            Ok(BotCommand::CraftPlan(recipe))
        }
        "go_to_surface" => Ok(BotCommand::GoToSurface),
        "go_to_bed" => Ok(BotCommand::GoToBed),
        "goto_player" => {
            let name = action.target.ok_or((StatusCode::BAD_REQUEST, "target must be a player name".into()))?;
            let name = name.trim();
            if name.is_empty() || name.len() > 16 || !name.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'_') {
                return Err((StatusCode::BAD_REQUEST, "goto_player requires a 1-16 character Minecraft username".into()));
            }
            Ok(BotCommand::GotoPlayer(name.to_owned()))
        }
        "look_at_player" => {
            let name = action.target.ok_or((StatusCode::BAD_REQUEST, "target must be a player name".into()))?;
            let name = name.trim();
            if name.is_empty() || name.len() > 16 || !name.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'_') {
                return Err((StatusCode::BAD_REQUEST, "look_at_player requires a 1-16 character Minecraft username".into()));
            }
            Ok(BotCommand::LookAtPlayer(name.to_owned()))
        }
        "saved_places" => Ok(BotCommand::SavedPlaces),
        "remember_here" | "go_to_remembered_place" => {
            let name = action.target.ok_or((StatusCode::BAD_REQUEST, "target must be a saved-place name".into()))?;
            let name = name.trim();
            if name.is_empty() || name.len() > 80
                || name.chars().any(|character| character.is_control())
                || !name.chars().all(|character| character.is_ascii_alphanumeric() || matches!(character, ' ' | '_' | '-')) {
                return Err((StatusCode::BAD_REQUEST, "saved-place names must be 1-80 safe ASCII letters, numbers, spaces, underscores, or hyphens".into()));
            }
            if action.action.eq_ignore_ascii_case("remember_here") {
                Ok(BotCommand::RememberHere(name.to_owned()))
            } else {
                Ok(BotCommand::GoToRememberedPlace(name.to_owned()))
            }
        }
        "move_away" => {
            let distance = action.target.as_deref().unwrap_or("8").trim().parse::<u8>()
                .map_err(|_| (StatusCode::BAD_REQUEST, "move_away distance must be an integer from 1 to 32 blocks".into()))?;
            if !(1..=32).contains(&distance) {
                return Err((StatusCode::BAD_REQUEST, "move_away distance must be from 1 to 32 blocks".into()));
            }
            Ok(BotCommand::MoveAway(distance))
        }
        "locate_entity" => {
            let raw = action.target.ok_or((StatusCode::BAD_REQUEST, "target must use entity_name[@range] syntax".into()))?;
            let (name, range) = match raw.rsplit_once('@') {
                Some((name, range)) => {
                    let range = range.trim().parse::<u16>()
                        .map_err(|_| (StatusCode::BAD_REQUEST, "locate_entity range must be an integer from 4 to 128 blocks".into()))?;
                    if !(4..=128).contains(&range) {
                        return Err((StatusCode::BAD_REQUEST, "locate_entity range must be from 4 to 128 blocks".into()));
                    }
                    (name.trim(), range)
                }
                None => (raw.trim(), 32),
            };
            let entity_id = name.rsplit(':').next().unwrap_or(name);
            if name.is_empty() || name.len() > 64 || entity_id.is_empty()
                || !entity_id.bytes().any(|byte| byte.is_ascii_alphanumeric())
                || !name.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b':')) {
                return Err((StatusCode::BAD_REQUEST, "locate_entity requires an entity type or player name".into()));
            }
            Ok(BotCommand::LocateEntity(name.to_ascii_lowercase(), range))
        }
        "entities" => {
            let range = action.target.as_deref().unwrap_or("32").trim().parse::<u16>()
                .map_err(|_| (StatusCode::BAD_REQUEST, "entities range must be an integer from 4 to 128 blocks".into()))?;
            if !(4..=128).contains(&range) {
                return Err((StatusCode::BAD_REQUEST, "entities range must be from 4 to 128 blocks".into()));
            }
            Ok(BotCommand::Entities(range))
        }
        "nearby_blocks" => {
            let range = action.target.as_deref().unwrap_or("4").trim().parse::<u8>()
                .map_err(|_| (StatusCode::BAD_REQUEST, "nearby_blocks range must be an integer from 1 to 8 blocks".into()))?;
            if !(1..=8).contains(&range) {
                return Err((StatusCode::BAD_REQUEST, "nearby_blocks range must be from 1 to 8 blocks".into()));
            }
            Ok(BotCommand::NearbyBlocks(range))
        }
        "locate" => {
            let raw = action.target.ok_or((StatusCode::BAD_REQUEST, "target must use block_name[@range] syntax, for example diamond_ore@128".into()))?;
            let (block_name, range) = match raw.split_once('@') {
                Some((block, range)) if !range.contains('@') => {
                    let range = range.trim().parse::<u16>()
                        .map_err(|_| (StatusCode::BAD_REQUEST, "locate range must be an integer from 8 to 512 blocks".into()))?;
                    if !(8..=512).contains(&range) {
                        return Err((StatusCode::BAD_REQUEST, "locate range must be from 8 to 512 blocks".into()));
                    }
                    (block.trim(), range)
                }
                Some(_) => return Err((StatusCode::BAD_REQUEST, "locate accepts at most one @range suffix".into())),
                None => (raw.trim(), 128),
            };
            if block_name.is_empty() || block_name.len() > 64
                || !block_name.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b':')) {
                return Err((StatusCode::BAD_REQUEST, "locate block name must be a Minecraft block ID such as diamond_ore or minecraft:diamond_ore".into()));
            }
            Ok(BotCommand::Locate(block_name.to_ascii_lowercase(), range))
        }
        "collect" => {
            let raw = action.target.ok_or((StatusCode::BAD_REQUEST, "target must use block_name[*count][@range] syntax, for example oak_log*8@64".into()))?;
            let (raw_block, range) = match raw.split_once('@') {
                Some((block, range)) if !range.contains('@') => {
                    let range = range.trim().parse::<u16>()
                        .map_err(|_| (StatusCode::BAD_REQUEST, "collect range must be an integer from 8 to 512 blocks".into()))?;
                    if !(8..=512).contains(&range) {
                        return Err((StatusCode::BAD_REQUEST, "collect range must be from 8 to 512 blocks".into()));
                    }
                    (block.trim(), range)
                }
                Some(_) => return Err((StatusCode::BAD_REQUEST, "collect accepts at most one @range suffix".into())),
                None => (raw.trim(), 128),
            };
            let (block_name, count) = match raw_block.split_once('*') {
                Some((block, count)) if !count.contains('*') => {
                    let count = count.trim().parse::<u8>()
                        .map_err(|_| (StatusCode::BAD_REQUEST, "collect count must be an integer from 1 to 64".into()))?;
                    if !(1..=64).contains(&count) {
                        return Err((StatusCode::BAD_REQUEST, "collect count must be from 1 to 64".into()));
                    }
                    (block.trim(), count)
                }
                Some(_) => return Err((StatusCode::BAD_REQUEST, "collect accepts one *count suffix".into())),
                None => (raw_block.trim(), 1),
            };
            if block_name.is_empty() || block_name.len() > 64
                || !block_name.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b':')) {
                return Err((StatusCode::BAD_REQUEST, "collect block name must be a Minecraft block ID such as oak_log or minecraft:oak_log".into()));
            }
            Ok(BotCommand::Collect(block_name.to_ascii_lowercase(), count, range))
        }
        "explore" => {
            let radius = action.target.as_deref().unwrap_or("32").parse::<u16>()
                .map_err(|_| (StatusCode::BAD_REQUEST, "explore radius must be an integer from 8 to 128 blocks".into()))?;
            if !(8..=128).contains(&radius) {
                return Err((StatusCode::BAD_REQUEST, "explore radius must be from 8 to 128 blocks".into()));
            }
            Ok(BotCommand::Explore(radius))
        }
        "build" => {
            let raw = action.target.ok_or((StatusCode::BAD_REQUEST, "target must be coordinates, a built-in blueprint, or a JSON schematic".into()))?;
            if raw.starts_with("schematic:") {
                Ok(BotCommand::BuildMixed(parse_schematic_spec(&raw)?))
            } else {
                let (material, positions) = parse_build_spec(&raw)?;
                Ok(BotCommand::Build(material, positions))
            }
        }
        "craft" => {
            let raw = action.target.ok_or((StatusCode::BAD_REQUEST, "target must be recipe[*count], optionally followed by @x,y,z for a crafting table".into()))?;
            let mut parts = raw.split('@');
            let raw_recipe = parts.next().unwrap_or_default().trim();
            let (recipe, count) = match raw_recipe.split_once('*') {
                Some((recipe, count)) => {
                    if count.contains('*') {
                        return Err((StatusCode::BAD_REQUEST, "craft batch count must be a single integer from 1 to 16".into()));
                    }
                    let count = count.trim().parse::<u8>()
                        .map_err(|_| (StatusCode::BAD_REQUEST, "craft batch count must be an integer from 1 to 16".into()))?;
                    if !(1..=16).contains(&count) {
                        return Err((StatusCode::BAD_REQUEST, "craft batch count must be from 1 to 16".into()));
                    }
                    (recipe.trim().to_ascii_lowercase(), count)
                }
                None => (raw_recipe.to_ascii_lowercase(), 1),
            };
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
                return Err((StatusCode::BAD_REQUEST, "that recipe requires a crafting-table coordinate in the form recipe[*count]@x,y,z".into()));
            }
            Ok(BotCommand::Craft(recipe, table, count))
        }
        "smelt" => {
            let raw = action.target.ok_or((StatusCode::BAD_REQUEST, "target must use input[*count]@x,y,z syntax".into()))?;
            let job = parse_smelt_job(&raw)?;
            Ok(BotCommand::Smelt(job.input, (job.x, job.y, job.z), job.count))
        }
        "smelt_collect" => {
            let raw = action.target.ok_or((StatusCode::BAD_REQUEST, "target must use input[*count]@x,y,z syntax for a previously loaded furnace batch".into()))?;
            let job = parse_smelt_job(&raw)?;
            Ok(BotCommand::SmeltCollect(job.input, (job.x, job.y, job.z), job.count))
        }
        "smelt_queue" | "smelt_parallel" => {
            let raw = action.target.ok_or((StatusCode::BAD_REQUEST, "target must contain 2-4 semicolon-separated input[*count]@x,y,z furnace jobs".into()))?;
            let entries = raw.split(';').map(str::trim).filter(|entry| !entry.is_empty()).collect::<Vec<_>>();
            if !(2..=4).contains(&entries.len()) {
                return Err((StatusCode::BAD_REQUEST, "smelt_queue and smelt_parallel require 2-4 furnace jobs".into()));
            }
            let mut jobs = Vec::with_capacity(entries.len());
            let mut positions = HashSet::new();
            for entry in entries {
                let job = parse_smelt_job(entry)?;
                if !positions.insert((job.x, job.y, job.z)) {
                    return Err((StatusCode::BAD_REQUEST, "smelting job lists cannot target the same furnace more than once".into()));
                }
                jobs.push(job);
            }
            Ok(BotCommand::SmeltQueue(jobs))
        }
        "container_sort" => {
            let raw = action.target.ok_or((StatusCode::BAD_REQUEST, "target must be a storage container coordinate x,y,z".into()))?;
            let parsed = raw.split(',').map(str::trim).map(str::parse::<i32>)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| (StatusCode::BAD_REQUEST, "storage coordinates must be integers".into()))?;
            if parsed.len() != 3 || parsed[1] < -64 || parsed[1] > 320
                || parsed.iter().any(|value| value.unsigned_abs() > 30_000_000) {
                return Err((StatusCode::BAD_REQUEST, "storage coordinates are outside supported world bounds".into()));
            }
            Ok(BotCommand::ContainerSort(parsed[0], parsed[1], parsed[2]))
        }
        "container_deposit" => {
            let raw = action.target.ok_or((StatusCode::BAD_REQUEST, "target must be x,y,z or x,y,z,exact_item_name for a filtered container deposit".into()))?;
            let parts = raw.split(',').map(str::trim).collect::<Vec<_>>();
            if parts.len() != 3 && parts.len() != 4 {
                return Err((StatusCode::BAD_REQUEST, "container deposit accepts x,y,z with an optional exact item-name filter".into()));
            }
            let parsed = parts[..3].iter().map(|value| value.parse::<i32>())
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| (StatusCode::BAD_REQUEST, "container deposit coordinates must be integers".into()))?;
            if parsed[1] < -64 || parsed[1] > 320
                || parsed.iter().any(|value| value.unsigned_abs() > 30_000_000) {
                return Err((StatusCode::BAD_REQUEST, "container deposit requires valid world coordinates".into()));
            }
            let filter = if parts.len() == 4 {
                let item = parts[3].to_ascii_lowercase();
                if item.is_empty() || item == "air" || item.len() > 32
                    || !item.chars().all(|character| character.is_ascii_alphanumeric() || character == '_') {
                    return Err((StatusCode::BAD_REQUEST, "deposit item filter must be an exact item name using letters, numbers, and underscores".into()));
                }
                Some(item)
            } else { None };
            Ok(BotCommand::ContainerDeposit(parsed[0], parsed[1], parsed[2], filter))
        }
        "container_transfer" => {
            let coordinates = action.target.ok_or((StatusCode::BAD_REQUEST, "target must be x,y,z,slot, start-end, or all".into()))?;
            let mut parts = coordinates.split(',');
            let parse_coordinate = |value: Option<&str>| value
                .ok_or((StatusCode::BAD_REQUEST, "target must be x,y,z,slot, start-end, or all".into()))?
                .trim().parse::<i32>()
                .map_err(|_| (StatusCode::BAD_REQUEST, "container coordinates must be integers".into()));
            let x = parse_coordinate(parts.next())?;
            let y = parse_coordinate(parts.next())?;
            let z = parse_coordinate(parts.next())?;
            let selector = parts.next().map(str::trim)
                .ok_or((StatusCode::BAD_REQUEST, "container transfer requires a slot, range, or all selector".into()))?;
            if parts.next().is_some() || y < -64 || y > 320
                || [x, y, z].iter().any(|value| value.unsigned_abs() > 30_000_000) {
                return Err((StatusCode::BAD_REQUEST, "container transfer requires valid world coordinates and exactly one slot selector".into()));
            }
            let (slot_range, item_filter) = if selector.eq_ignore_ascii_case("all") {
                (None, None)
            } else if let Some(raw_item) = selector.strip_prefix("item:") {
                let item = raw_item.trim().to_ascii_lowercase();
                if item.is_empty() || item == "air" || item.len() > 32
                    || !item.chars().all(|character| character.is_ascii_alphanumeric() || character == '_') {
                    return Err((StatusCode::BAD_REQUEST, "item filter must use an exact non-air item name with letters, numbers, and underscores".into()));
                }
                (None, Some(item))
            } else {
                let (first, last) = match selector.split_once('-') {
                    Some((first, last)) => (first.trim(), last.trim()),
                    None => (selector, selector),
                };
                let first = first.parse::<u16>()
                    .map_err(|_| (StatusCode::BAD_REQUEST, "selector must be 0-89, a range such as 0-26, all, or item:exact_name".into()))?;
                let last = last.parse::<u16>()
                    .map_err(|_| (StatusCode::BAD_REQUEST, "selector must be 0-89, a range such as 0-26, all, or item:exact_name".into()))?;
                if first > last || last > 89 {
                    return Err((StatusCode::BAD_REQUEST, "slot range must be ordered and stay within 0-89".into()));
                }
                (Some((first, last)), None)
            };
            Ok(BotCommand::ContainerTransfer(x, y, z, slot_range, item_filter))
        }
        "optimize_armor" => Ok(BotCommand::OptimizeArmor),
        "inventory_sort" => Ok(BotCommand::InventorySort),
        "inventory_compact" => Ok(BotCommand::InventoryCompact),
        "hotbar_restock" => {
            let raw = action.target.ok_or((StatusCode::BAD_REQUEST, "target must be an item name or supported selector such as iron_ingot, planks, log, or food".into()))?;
            let selector = raw.trim().strip_prefix("minecraft:").unwrap_or(raw.trim()).to_ascii_lowercase();
            if selector.is_empty() || selector.len() > 64
                || !selector.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'_') {
                return Err((StatusCode::BAD_REQUEST, "hotbar_restock requires a 1-64 character item name or selector".into()));
            }
            Ok(BotCommand::HotbarRestock(selector))
        },
        "trade" => {
            let raw = action.target.ok_or((StatusCode::BAD_REQUEST, "target must be x,y,z to inspect a nearby merchant or x,y,z,index to execute one offer".into()))?;
            let parts = raw.split(',').map(str::trim).collect::<Vec<_>>();
            if parts.len() != 3 && parts.len() != 4 {
                return Err((StatusCode::BAD_REQUEST, "trade target must be x,y,z or x,y,z,offer_index".into()));
            }
            let coordinates = parts[..3].iter().map(|value| value.parse::<i32>())
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| (StatusCode::BAD_REQUEST, "merchant coordinates must be integers".into()))?;
            if coordinates[1] < -64 || coordinates[1] > 320
                || coordinates.iter().any(|value| value.unsigned_abs() > 30_000_000) {
                return Err((StatusCode::BAD_REQUEST, "merchant coordinates are outside supported world bounds".into()));
            }
            let offer_index = if parts.len() == 4 {
                let index = parts[3].parse::<u8>()
                    .map_err(|_| (StatusCode::BAD_REQUEST, "merchant offer index must be an integer from 0 to 255".into()))?;
                Some(index)
            } else { None };
            Ok(BotCommand::Trade(coordinates[0], coordinates[1], coordinates[2], offer_index))
        }
        "enter_portal" => {
            let raw = action.target.ok_or((StatusCode::BAD_REQUEST, "target must be an existing Nether portal, End portal, or End gateway block coordinate x,y,z".into()))?;
            let parsed = raw.split(',').map(str::trim).map(str::parse::<i32>)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| (StatusCode::BAD_REQUEST, "portal target must be integer x,y,z coordinates".into()))?;
            if parsed.len() != 3 || parsed[1] < -64 || parsed[1] > 320
                || parsed.iter().any(|value| value.unsigned_abs() > 30_000_000) {
                return Err((StatusCode::BAD_REQUEST, "portal coordinates are outside supported world bounds".into()));
            }
            Ok(BotCommand::EnterPortal(parsed[0], parsed[1], parsed[2]))
        }
        "use_item" => Ok(BotCommand::UseItem),
        "eat" => Ok(BotCommand::Eat),
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
        "look" | "goto" | "mine" | "place" | "till" | "plant" | "harvest" | "clear_furnace" | "container_view" => {
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
                "till" => Ok(BotCommand::Till(x, y, z)),
                "plant" => Ok(BotCommand::Plant(x, y, z)),
                "harvest" => Ok(BotCommand::Harvest(x, y, z)),
                "clear_furnace" => Ok(BotCommand::ClearFurnace(x, y, z)),
                "container_view" => Ok(BotCommand::ContainerView(x, y, z)),
                _ => Ok(BotCommand::Mine(x, y, z)),
            }
        }
        other => Err((StatusCode::NOT_IMPLEMENTED, format!("executor for '{other}' is not available yet"))),
    }
}

async fn memory_list(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> Result<Json<Vec<crate::store::MemoryRecord>>, StatusCode> {
    authorize(&state, &headers)?;
    let memories = state.store.list_memories(None, 200)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(memories))
}

async fn memory_upsert(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(request): Json<MemoryRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    authorize(&state, &headers).map_err(|code| (code, "unauthorized".into()))?;
    let kind = request.kind.trim().to_ascii_lowercase();
    if !["semantic", "episodic", "preference", "world"].contains(&kind.as_str()) {
        return Err((StatusCode::BAD_REQUEST, "memory kind must be semantic, episodic, preference, or world".into()));
    }
    let subject = request.subject.trim();
    let content = request.content.trim();
    let source = request.source.trim();
    if subject.is_empty() || subject.len() > 120 || content.is_empty() || content.len() > 2_000
        || source.is_empty() || source.len() > 80
        || subject.chars().any(char::is_control) || source.chars().any(char::is_control) {
        return Err((StatusCode::BAD_REQUEST, "memory subject/source must be 1-120/1-80 safe characters and content must be 1-2000 bytes".into()));
    }
    if !request.confidence.is_finite() || !(0.0..=1.0).contains(&request.confidence)
        || !(0..=100).contains(&request.importance) {
        return Err((StatusCode::BAD_REQUEST, "memory confidence must be 0.0-1.0 and importance must be 0-100".into()));
    }
    let id = state.store.upsert_memory(
        &kind, subject, content, request.confidence, request.importance, source,
    ).map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({"id":id,"saved":true})))
}

async fn memory_delete(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(memory_id): Path<i64>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    authorize(&state, &headers).map_err(|code| (code, "unauthorized".into()))?;
    if memory_id <= 0 {
        return Err((StatusCode::BAD_REQUEST, "memory ID must be positive".into()));
    }
    let deleted = state.store.delete_memory(memory_id)
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    if !deleted {
        return Err((StatusCode::NOT_FOUND, "memory record was not found".into()));
    }
    Ok(Json(serde_json::json!({"deleted":true})))
}

async fn create_goal(State(state): State<ApiState>, headers: HeaderMap, Json(request): Json<GoalRequest>) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    authorize(&state, &headers).map_err(|code| (code, "unauthorized".into()))?;
    if request.description.trim().is_empty() || request.description.len() > 4_000 {
        return Err((StatusCode::BAD_REQUEST, "goal must contain 1-4000 bytes".into()));
    }
    let memory_context = state.store.relevant_memory_context(&request.description, 3_000)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let connected_agents = state.status.agents.lock().await.values()
        .map(|agent| format!("{} ({})", agent.agent, agent.role))
        .collect::<Vec<_>>();
    let team_context = format!(
        "Connected Minecraft team ({}): {}.",
        connected_agents.len(),
        if connected_agents.is_empty() { "none".to_owned() } else { connected_agents.join(", ") }
    );
    let mut online_players = state.status.online_players.lock().await.iter().cloned().collect::<Vec<_>>();
    online_players.sort_unstable_by_key(|name| name.to_ascii_lowercase());
    let player_context = format!(
        "Other players currently listed online: {}.",
        if online_players.is_empty() { "none".to_owned() } else { online_players.join(", ") }
    );
    let planning_context = format!("{}\n{team_context}\n{player_context}\n{memory_context}", request.description);
    let drafts = tokio::time::timeout(Duration::from_secs(50), state.brain.decompose_goal(&planning_context))
        .await.map_err(|_| (StatusCode::GATEWAY_TIMEOUT, "strategic planner timed out".into()))?
        .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?;
    let task_specs = drafts.iter().map(|draft| {
        (draft.role.clone(), draft.description.clone(), draft.priority)
    }).collect::<Vec<_>>();
    let (goal_id, task_ids) = state.store.create_goal_with_tasks(&request.description, &task_specs)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let tasks = drafts.into_iter().zip(task_ids).map(|(draft, id)| {
        serde_json::json!({"id":id,"role":draft.role,"description":draft.description,"priority":draft.priority,"status":"pending"})
    }).collect::<Vec<_>>();
    Ok(Json(serde_json::json!({"goal_id":goal_id,"description":request.description,"tasks":tasks})))
}

async fn list_goals(State(state): State<ApiState>, headers: HeaderMap) -> Result<Json<Vec<Goal>>, StatusCode> {
    authorize(&state, &headers)?;
    let goals = state.store.list_goals().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(goals))
}

async fn retry_goal(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(goal_id): Path<i64>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    authorize(&state, &headers).map_err(|code| (code, "unauthorized".into()))?;
    if goal_id <= 0 {
        return Err((StatusCode::BAD_REQUEST, "goal ID must be positive".into()));
    }
    let retried = state.store.retry_goal(goal_id)
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    if !retried {
        return Err((StatusCode::CONFLICT, "goal does not exist or is not failed".into()));
    }
    Ok(Json(serde_json::json!({"goal_id":goal_id,"retried":true,"message":"Failed and cancelled tasks were requeued; verified completed tasks were preserved."})))
}

async fn cancel_goal(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(goal_id): Path<i64>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    authorize(&state, &headers).map_err(|code| (code, "unauthorized".into()))?;
    if goal_id <= 0 {
        return Err((StatusCode::BAD_REQUEST, "goal ID must be positive".into()));
    }
    let cancelled = state.store.cancel_goal(goal_id)
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    if !cancelled {
        return Err((StatusCode::CONFLICT, "goal does not exist or is no longer active".into()));
    }
    Ok(Json(serde_json::json!({"goal_id":goal_id,"cancelled":true})))
}

async fn list_goal_tasks(State(state): State<ApiState>, headers: HeaderMap, Path(goal_id): Path<i64>) -> Result<Json<Vec<AgentTask>>, (StatusCode, String)> {
    authorize(&state, &headers).map_err(|code| (code, "unauthorized".into()))?;
    let tasks = state.store.list_tasks(goal_id).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(tasks))
}

async fn issue_agent_credential(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(agent_id): Path<String>,
) -> Result<Json<AgentCredentialResponse>, (StatusCode, String)> {
    authorize(&state, &headers).map_err(|code| (code, "unauthorized".into()))?;
    if !valid_agent_id(&agent_id) {
        return Err((StatusCode::BAD_REQUEST, "agent ID must be 1-64 safe ASCII characters".into()));
    }
    if !state.config.minecraft_accounts.iter().any(|name| name.eq_ignore_ascii_case(&agent_id)) {
        return Err((StatusCode::BAD_REQUEST, "agent ID must match a configured Minecraft team member".into()));
    }
    let token = uuid::Uuid::new_v4().to_string();
    let mut tokens = state.status.agent_tokens.lock().await;
    tokens.insert(agent_id.clone(), AgentCredential {
        token: token.clone(),
        expires_at: Instant::now() + AGENT_CREDENTIAL_TTL,
    });
    Ok(Json(AgentCredentialResponse {
        agent_id,
        token,
        expires_in_seconds: AGENT_CREDENTIAL_TTL.as_secs(),
    }))
}

async fn revoke_agent_credential(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(agent_id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    authorize(&state, &headers).map_err(|code| (code, "unauthorized".into()))?;
    if !valid_agent_id(&agent_id) {
        return Err((StatusCode::BAD_REQUEST, "agent ID must contain 1-64 safe ASCII characters".into()));
    }
    let revoked = state.status.agent_tokens.lock().await.remove(&agent_id).is_some();
    Ok(Json(serde_json::json!({"agent_id":agent_id,"revoked":revoked})))
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers.get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
}

fn constant_time_equal(left: &str, right: &str) -> bool {
    left.as_bytes().len() == right.as_bytes().len()
        && bool::from(left.as_bytes().ct_eq(right.as_bytes()))
}

fn agent_credential_matches(credential: &AgentCredential, provided: &str, now: Instant) -> bool {
    now < credential.expires_at && constant_time_equal(provided, &credential.token)
}

async fn authorize_agent(state: &ApiState, headers: &HeaderMap, agent_id: &str) -> Result<(), StatusCode> {
    if authorize(state, headers).is_ok() {
        return Ok(());
    }
    let provided = bearer_token(headers).ok_or(StatusCode::UNAUTHORIZED)?;
    let tokens = state.status.agent_tokens.lock().await;
    let expected = tokens.get(agent_id).ok_or(StatusCode::UNAUTHORIZED)?;
    if agent_credential_matches(expected, provided, Instant::now()) {
        Ok(())
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}

async fn list_review_tasks(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> Result<Json<Vec<AgentTask>>, StatusCode> {
    authorize(&state, &headers)?;
    let tasks = state.store.list_review_tasks().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(tasks))
}

async fn claim_task(State(state): State<ApiState>, headers: HeaderMap, Json(request): Json<AgentClaimRequest>) -> Result<Json<ClaimResponse>, (StatusCode, String)> {
    let roles = ["planner", "scout", "gatherer", "builder", "crafter", "guardian"];
    if !valid_agent_id(&request.agent_id) || !roles.contains(&request.role.as_str()) {
        return Err((StatusCode::BAD_REQUEST, "agent_id must be 1-64 ASCII letters, numbers, dots, underscores, or hyphens, and role must be supported".into()));
    }
    authorize_agent(&state, &headers, &request.agent_id).await
        .map_err(|code| (code, "unauthorized for this agent".into()))?;
    let task = state.store.claim_task(&request.agent_id, &request.role)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(ClaimResponse { task }))
}

async fn heartbeat_task(State(state): State<ApiState>, headers: HeaderMap, Path(task_id): Path<i64>, Json(request): Json<CompleteTaskRequest>) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    if !valid_agent_id(&request.agent_id) {
        return Err((StatusCode::BAD_REQUEST, "agent_id must be 1-64 ASCII letters, numbers, dots, underscores, or hyphens".into()));
    }
    authorize_agent(&state, &headers, &request.agent_id).await
        .map_err(|code| (code, "unauthorized for this agent".into()))?;
    let renewed = state.store.renew_task_lease(task_id, &request.agent_id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if !renewed {
        return Err((StatusCode::CONFLICT, "task lease is no longer owned by this agent".into()));
    }
    Ok(Json(serde_json::json!({"renewed":true})))
}


async fn fail_task(State(state): State<ApiState>, headers: HeaderMap, Path(task_id): Path<i64>, Json(request): Json<FailTaskRequest>) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    if !valid_agent_id(&request.agent_id) {
        return Err((StatusCode::BAD_REQUEST, "agent_id must be 1-64 ASCII letters, numbers, dots, underscores, or hyphens".into()));
    }
    if request.reason.trim().is_empty() || request.reason.len() > 1000 {
        return Err((StatusCode::BAD_REQUEST, "reason must contain 1-1000 bytes".into()));
    }
    authorize_agent(&state, &headers, &request.agent_id).await
        .map_err(|code| (code, "unauthorized for this agent".into()))?;
    let outcome = state.store.fail_task(task_id, &request.agent_id, &request.reason)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    match outcome {
        Some(status) => Ok(Json(serde_json::json!({"recorded":true,"status":status}))),
        None => Err((StatusCode::CONFLICT, "task is not claimed or awaiting review under this agent".into())),
    }
}

async fn release_task(State(state): State<ApiState>, headers: HeaderMap, Path(task_id): Path<i64>, Json(request): Json<CompleteTaskRequest>) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    if !valid_agent_id(&request.agent_id) {
        return Err((StatusCode::BAD_REQUEST, "agent_id must be 1-64 ASCII letters, numbers, dots, underscores, or hyphens".into()));
    }
    authorize_agent(&state, &headers, &request.agent_id).await
        .map_err(|code| (code, "unauthorized for this agent".into()))?;
    let released = state.store.release_task(task_id, &request.agent_id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if !released {
        return Err((StatusCode::CONFLICT, "task is not owned by this agent or is no longer releasable".into()));
    }
    Ok(Json(serde_json::json!({"released":true})))
}

async fn complete_task(State(state): State<ApiState>, headers: HeaderMap, Path(task_id): Path<i64>, Json(request): Json<CompleteTaskRequest>) -> Result<Json<CompleteResponse>, (StatusCode, String)> {
    if !valid_agent_id(&request.agent_id) {
        return Err((StatusCode::BAD_REQUEST, "agent_id must be 1-64 ASCII letters, numbers, dots, underscores, or hyphens".into()));
    }
    if request.verification_note.trim().is_empty() || request.verification_note.len() > 500 {
        return Err((StatusCode::BAD_REQUEST, "verification_note must describe the observed in-game result in 1-500 bytes".into()));
    }
    authorize_agent(&state, &headers, &request.agent_id).await
        .map_err(|code| (code, "unauthorized for this agent".into()))?;
    let completed = state.store.complete_task_verified(task_id, &request.agent_id, &request.verification_note)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if !completed {
        return Err((StatusCode::CONFLICT, "task is not owned by this agent or is already complete".into()));
    }
    Ok(Json(CompleteResponse { completed }))
}

fn valid_agent_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= 64
        && value.chars().all(|character| character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-'))
}

fn authorize(state: &ApiState, headers: &HeaderMap) -> Result<(), StatusCode> {
    let provided = bearer_token(headers).ok_or(StatusCode::UNAUTHORIZED)?;
    if !constant_time_equal(provided, &state.config.api_token) {
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
    fn unauthenticated_requests_share_one_rate_limit_bucket() {
        assert_eq!(rate_limit_bucket_key(None, false), "anonymous");
        assert_eq!(rate_limit_bucket_key(Some("Bearer invalid-a"), false), "anonymous");
        assert_eq!(rate_limit_bucket_key(Some("Bearer invalid-b"), false), "anonymous");
        assert_ne!(
            rate_limit_bucket_key(Some("master-token"), true),
            rate_limit_bucket_key(Some("agent-token"), true)
        );
    }

    #[test]
    fn agent_credentials_expire_and_cannot_be_used_for_another_token() {
        let now = Instant::now();
        let credential = AgentCredential {
            token: "agent-secret".into(),
            expires_at: now + Duration::from_secs(60),
        };
        assert!(agent_credential_matches(&credential, "agent-secret", now));
        assert!(!agent_credential_matches(&credential, "wrong", now));
        assert!(!agent_credential_matches(&credential, "agent-secret", now + Duration::from_secs(61)));
    }

    #[test]
    fn agent_token_comparison_is_constant_time_and_requires_exact_match() {
        assert!(constant_time_equal("agent-secret", "agent-secret"));
        assert!(!constant_time_equal("agent-secret", "other-secret"));
        assert!(!constant_time_equal("agent-secret", "agent-secret-longer"));
    }

    #[test]
    fn api_rate_window_enforces_limit_and_resets_after_sixty_seconds() {
        let now = Instant::now();
        let mut window = RateWindow { started: now, requests: 0 };
        for _ in 0..API_RATE_LIMIT {
            assert!(consume_rate_window(&mut window, now));
        }
        assert!(!consume_rate_window(&mut window, now));
        assert!(consume_rate_window(&mut window, now + API_RATE_WINDOW));
        assert_eq!(window.requests, 1);
    }

    #[test]
    fn task_worker_ids_are_bounded_and_safe_for_persistence() {
        assert!(valid_agent_id("Scout"));
        assert!(valid_agent_id("builder-02"));
        assert!(valid_agent_id("worker.team_1"));
        let too_long = "a".repeat(65);
        for invalid in ["", " ", " worker", "worker ", "worker\n", "a/b", too_long.as_str()] {
            assert!(!valid_agent_id(invalid), "accepted {invalid:?}");
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
        let max_message = "a".repeat(240);
        let oversized_message = "a".repeat(241);
        let max_supplementary_message = "🙂".repeat(120);
        let oversized_supplementary_message = "🙂".repeat(121);
        assert!(command_from_action(action("say", Some(max_message.as_str()))).is_ok());
        assert!(command_from_action(action("say", Some(oversized_message.as_str()))).is_err());
        assert!(command_from_action(action("say", Some(max_supplementary_message.as_str()))).is_ok());
        assert!(command_from_action(action("say", Some(oversized_supplementary_message.as_str()))).is_err());
    }

    #[test]
    fn smelting_requires_an_allowlisted_input_batch_and_valid_furnace_coordinate() {
        assert!(command_from_action(action("smelt", Some("raw_iron@0,64,0"))).is_ok());
        assert!(command_from_action(action("smelt", Some("raw_iron*8@0,64,0"))).is_ok());
        assert!(command_from_action(action("smelt", Some("raw_iron*64@0,64,0"))).is_ok());
        assert!(command_from_action(action("smelt", Some("raw_iron*1@0,64,0"))).is_ok());
        assert!(command_from_action(action("smelt", Some("raw_iron*0@0,64,0"))).is_err());
        assert!(command_from_action(action("smelt", Some("raw_iron*65@0,64,0"))).is_err());
        assert!(command_from_action(action("smelt", Some("raw_iron*many@0,64,0"))).is_err());
        assert!(command_from_action(action("smelt", Some("beef@0,64,0"))).is_ok());
        assert!(command_from_action(action("smelt", Some("unknown_item@0,64,0"))).is_err());
        assert!(command_from_action(action("smelt", Some("raw_iron"))).is_err());
        assert!(command_from_action(action("smelt", Some("raw_iron@0,999,0"))).is_err());
        assert!(command_from_action(action("smelt", Some("raw_iron@0,64,0@1,2,3"))).is_err());
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
            api_key: String::new(), base_url: "http://127.0.0.1:11434/v1".into(), azure_api_version: "2024-10-21".into(),
            minecraft_server: Some("localhost".into()), minecraft_username: "Mindcraft".into(),
            minecraft_accounts: vec!["Mindcraft".into(), "Scout".into(), "Builder".into()],
            minecraft_auth: auth.into(), microsoft_email: None,
            trusted_chat_players: Vec::new(),
            database: std::path::PathBuf::from(":memory:"),
        }
    }

    #[test]
    fn build_action_creates_a_bottom_up_bounded_command() {
        let command = command_from_action(action("build", Some("1,66,1;1,64,1;1,65,1"))).unwrap();
        assert!(matches!(
            command,
            BotCommand::Build(None, positions)
                if positions == vec![(1, 64, 1), (1, 65, 1), (1, 66, 1)]
        ));
        assert!(matches!(
            command_from_action(action("build", Some("cobblestone@1,64,1;2,64,1"))).unwrap(),
            BotCommand::Build(Some(material), positions)
                if material == "cobblestone" && positions.len() == 2
        ));
        assert!(command_from_action(action("build", Some("redstone@1,64,1"))).is_ok());
        assert!(command_from_action(action("build", Some("bad material@1,64,1"))).is_err());
        assert!(command_from_action(action("build", Some("stone@1,64,1@2,64,1"))).is_err());
    }

    #[test]
    fn multi_block_build_targets_are_bounded_unique_and_world_validated() {
        assert_eq!(
            parse_build_targets("1,64,1;2,64,1").unwrap(),
            vec![(1, 64, 1), (2, 64, 1)]
        );
        assert_eq!(
            parse_build_targets("1,66,1;1,64,1;1,65,1").unwrap(),
            vec![(1, 64, 1), (1, 65, 1), (1, 66, 1)]
        );
        assert!(parse_build_targets("").is_err());
        assert!(parse_build_targets("1,64,1;1,64,1").is_err());
        assert!(parse_build_targets("1,321,1").is_err());
        assert!(parse_build_targets("1,64").is_err());
        let too_many = (0..65).map(|x| format!("{x},64,1")).collect::<Vec<_>>().join(";");
        assert!(parse_build_targets(&too_many).is_err());
    }

    #[test]
    fn built_in_blueprints_expand_to_bounded_bottom_up_coordinates() {
        let (material, platform) = parse_build_spec("blueprint:platform_5x5@10,64,-5").unwrap();
        assert!(material.is_none());
        assert_eq!(platform.len(), 25);
        assert_eq!(platform.first(), Some(&(10, 64, -5)));
        assert_eq!(platform.last(), Some(&(14, 64, -1)));

        let (material, hut) = parse_build_spec("oak_planks@blueprint:starter_hut@0,70,0").unwrap();
        assert_eq!(material.as_deref(), Some("oak_planks"));
        assert_eq!(hut.len(), 23);
        assert!(hut.iter().all(|(_, y, _)| *y >= 70 && *y <= 72));

        assert_eq!(parse_build_spec("blueprint:wall_7x3@0,64,0").unwrap().1.len(), 21);
        assert!(parse_build_spec("blueprint:unknown@0,64,0").is_err());
        assert!(parse_build_spec("blueprint:starter_hut@0,321,0").is_err());
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
        assert!(matches!(command_from_action(action("container_deposit", Some("1,64,-2"))), Ok(BotCommand::ContainerDeposit(1,64,-2))));
        assert!(command_from_action(action("container_deposit", Some("1,64,-2,3"))).is_err());
        assert!(matches!(command_from_action(action("container_transfer", Some("1,64,-2,27"))), Ok(BotCommand::ContainerTransfer(1,64,-2,Some((27,27))))));
        assert!(matches!(command_from_action(action("container_transfer", Some("1,64,-2,0-26"))), Ok(BotCommand::ContainerTransfer(1,64,-2,Some((0,26))))));
        assert!(matches!(command_from_action(action("container_transfer", Some("1,64,-2,all"))), Ok(BotCommand::ContainerTransfer(1,64,-2,None))));
        for target in ["1,64,2", "1,64,2,101", "1,64,2,27-12", "1,64,2,90-91", "1,321,2,4", "x,64,2,4", "1,64,2,all,extra"] {
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
