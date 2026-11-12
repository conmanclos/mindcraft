use crate::{api::RuntimeStatus, brain::Brain};
use anyhow::Result;
use azalea::{pathfinder::{PathfinderClientExt, goals::BlockPosGoal}, prelude::*, swarm::SwarmBuilder};
use std::{sync::{atomic::{AtomicBool, Ordering}, Arc}, time::{Duration, Instant}};
use tokio::sync::{mpsc, Mutex};

#[derive(Clone, Copy)]
enum PendingBlockActionKind { Mine, Place }

#[derive(Clone, Copy)]
struct PendingBlockAction {
    kind: PendingBlockActionKind,
    position: BlockPos,
    attempts: u8,
}

#[derive(Debug)]
pub enum BotCommand {
    Say(String),
    Stop,
    AttackCrosshair,
    Follow(String),
    Look(i32, i32, i32),
    Goto(i32, i32, i32),
    Mine(i32, i32, i32),
    Place(i32, i32, i32),
    Equip(u8),
}

#[derive(Clone, Component, Default)]
struct BotState {
    brain: Option<Arc<Brain>>,
    status: Option<Arc<RuntimeStatus>>,
    last_reply: Option<Arc<Mutex<Instant>>>,
    commands: Option<Arc<Mutex<mpsc::Receiver<BotCommand>>>>,
    connected: Option<Arc<AtomicBool>>,
    last_reflex: Option<Arc<Mutex<Instant>>>,
    follow_target: Option<Arc<Mutex<Option<String>>>>,
    is_following: Option<Arc<AtomicBool>>,
    last_follow_repath: Option<Arc<Mutex<Instant>>>,
    last_snapshot: Option<Arc<Mutex<Instant>>>,
    pending_block_action: Option<Arc<Mutex<Option<PendingBlockAction>>>>,
    swarm_restart: Option<Arc<AtomicBool>>,
    is_primary: bool,
}

pub async fn connect(
    server: String,
    accounts: Vec<String>,
    auth_mode: String,
    microsoft_email: Option<String>,
    brain: Arc<Brain>,
    status: Arc<RuntimeStatus>,
) -> Result<()> {
    tracing::info!(%server, account_count = accounts.len(), auth_mode = %auth_mode, "starting Minecraft agent swarm");
    loop {
        let accounts_to_join = if auth_mode == "microsoft" {
            let email = microsoft_email.as_deref().ok_or_else(|| anyhow::anyhow!("MINDCRAFT_MICROSOFT_EMAIL is required for Microsoft authentication"))?;
            tracing::info!("starting Azalea Microsoft device-code authentication");
            match Account::microsoft(email).await {
                Ok(account) => vec![account],
                Err(error) => {
                    tracing::error!(%error, "Microsoft authentication failed; dashboard remains available and login will retry in 30 seconds");
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    continue;
                }
            }
        } else {
            accounts.iter().cloned().map(Account::offline).collect::<Vec<_>>()
        };
        if accounts_to_join.is_empty() {
            anyhow::bail!("at least one Minecraft account is required");
        }

        let (command_tx, command_rx) = mpsc::channel(64);
        *status.command_tx.lock().await = Some(command_tx);
        let shared_receiver = Arc::new(Mutex::new(command_rx));
        let shared_reply_cooldown = Arc::new(Mutex::new(Instant::now() - Duration::from_secs(10)));
        let shared_reflex_cooldown = Arc::new(Mutex::new(Instant::now() - Duration::from_secs(20)));
        let shared_follow_target = Arc::new(Mutex::new(None));
        let shared_pending_block_action = Arc::new(Mutex::new(None));
        let shared_swarm_restart = Arc::new(AtomicBool::new(false));

        let mut builder = SwarmBuilder::new().set_handler(handle);
        for (index, account) in accounts_to_join.into_iter().enumerate() {
            let is_primary = index == 0;
            let command_receiver = if is_primary { Some(shared_receiver.clone()) } else { None };
            let state = make_state(
                brain.clone(), status.clone(), command_receiver, shared_reply_cooldown.clone(),
                shared_follow_target.clone(), shared_reflex_cooldown.clone(),
                shared_pending_block_action.clone(), shared_swarm_restart.clone(), is_primary,
            );
            builder = builder.add_account_with_state(account, state);
        }

        let result = builder.join_delay(Duration::from_millis(500)).start(server.as_str()).await;
        status.minecraft_connected.store(false, Ordering::Relaxed);
        status.connected_agents.store(0, Ordering::Relaxed);
        *status.command_tx.lock().await = None;
        *status.world.lock().await = None;
        tracing::warn!("Azalea swarm exited: {result:?}; reconnecting in 5 seconds");
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

fn make_state(
    brain: Arc<Brain>,
    status: Arc<RuntimeStatus>,
    commands: Option<Arc<Mutex<mpsc::Receiver<BotCommand>>>>,
    last_reply: Arc<Mutex<Instant>>,
    follow_target: Arc<Mutex<Option<String>>>,
    last_reflex: Arc<Mutex<Instant>>,
    pending_block_action: Arc<Mutex<Option<PendingBlockAction>>>,
    swarm_restart: Arc<AtomicBool>,
    is_primary: bool,
) -> BotState {
    BotState {
        brain: Some(brain),
        status: Some(status),
        last_reply: Some(last_reply),
        commands,
        connected: Some(Arc::new(AtomicBool::new(false))),
        last_reflex: Some(last_reflex),
        follow_target: Some(follow_target),
        is_following: Some(Arc::new(AtomicBool::new(false))),
        last_follow_repath: Some(Arc::new(Mutex::new(Instant::now() - Duration::from_secs(5)))),
        last_snapshot: Some(Arc::new(Mutex::new(Instant::now() - Duration::from_secs(2)))),
        pending_block_action: Some(pending_block_action),
        swarm_restart: Some(swarm_restart),
        is_primary,
    }
}

async fn handle(bot: Client, event: Event, state: BotState) -> eyre::Result<()> {
    match event {
        Event::Spawn => {
            if let Some(connected) = &state.connected {
                if !connected.swap(true, Ordering::Relaxed) {
                    if let Some(status) = &state.status {
                        let count = status.connected_agents.fetch_add(1, Ordering::Relaxed) + 1;
                        status.minecraft_connected.store(count > 0, Ordering::Relaxed);
                    }
                }
            }
            tracing::info!(username = %bot.username(), "Minecraft agent spawned and ready");
        }
        Event::Disconnect(_) | Event::ConnectionFailed(_) => {
            if let Some(connected) = &state.connected {
                if connected.swap(false, Ordering::Relaxed) {
                    if let Some(status) = &state.status {
                        let count = status.connected_agents.fetch_sub(1, Ordering::Relaxed).saturating_sub(1);
                        status.minecraft_connected.store(count > 0, Ordering::Relaxed);
                    }
                }
            }
            if let Some(restart) = &state.swarm_restart {
                restart.store(true, Ordering::Relaxed);
            }
            tracing::warn!(username = %bot.username(), "Minecraft agent disconnected or failed to connect; requesting a full swarm restart");
            bot.exit();
        }
        Event::Tick => {
            if state.swarm_restart.as_ref().is_some_and(|restart| restart.load(Ordering::Relaxed)) {
                bot.exit();
                return Ok(());
            }
            if bot.exists() {
                let health = bot.health();
                let hunger = bot.hunger();
                if state.is_primary {
                    if let Some(status) = &state.status {
                        let should_snapshot = if let Some(last) = &state.last_snapshot {
                            let mut last = last.lock().await;
                            if last.elapsed() >= Duration::from_secs(1) {
                                *last = Instant::now();
                                true
                            } else { false }
                        } else { false };
                        if should_snapshot {
                            let position = bot.position();
                            let inventory = bot.menu().slots().iter().enumerate()
                                .filter(|(_, item)| !item.is_empty())
                                .map(|(slot, item)| {
                                    let description: String = format!("{item:?}").chars().take(80).collect();
                                    format!("{slot}: {description}")
                                })
                                .collect::<Vec<_>>();
                            *status.world.lock().await = Some(crate::api::WorldSnapshot {
                                agent: bot.username(),
                                position: [position.x as f64, position.y as f64, position.z as f64],
                                health,
                                food: hunger.food,
                                saturation: hunger.saturation,
                                inventory,
                            });
                        }
                    }
                }
                if let Some(reflex_action) = crate::reflex::evaluate(health, hunger.food) {
                    let should_warn = if let Some(cooldown) = &state.last_reflex {
                        let mut last = cooldown.lock().await;
                        if last.elapsed() >= Duration::from_secs(15) {
                            *last = Instant::now();
                            true
                        } else { false }
                    } else { false };
                    match reflex_action {
                        crate::reflex::ReflexAction::StopAndWarn => {
                            bot.stop_pathfinding();
                            bot.walk(WalkDirection::None);
                            if should_warn {
                                bot.chat("Reflex safety: health is critical. I stopped moving; recover before continuing.");
                                tracing::warn!(username = %bot.username(), health, "critical-health reflex activated");
                            }
                        }
                        crate::reflex::ReflexAction::WarnLowFood => {
                            if should_warn {
                                bot.chat("Reflex safety: food is low. Find food before continuing.");
                                tracing::warn!(username = %bot.username(), food = hunger.food, "low-food reflex activated");
                            }
                        }
                    }
                }
            }
            if state.is_primary {
                if let Some(pending_lock) = &state.pending_block_action {
                    let pending = pending_lock.lock().await.take();
                    if let Some(mut pending) = pending {
                        match bot.hit_result() {
                            HitResult::Block(hit) if !hit.miss && hit.block_pos == pending.position => {
                                match pending.kind {
                                    PendingBlockActionKind::Mine => bot.start_mining(pending.position),
                                    PendingBlockActionKind::Place => bot.block_interact(pending.position),
                                }
                            }
                            _ => {
                                pending.attempts = pending.attempts.saturating_add(1);
                                if pending.attempts >= 20 {
                                    bot.chat("I couldn't get a clear line of sight to that block. The action was cancelled.");
                                } else {
                                    if pending.attempts % 3 == 0 {
                                        bot.look_at(pending.position.center());
                                    }
                                    *pending_lock.lock().await = Some(pending);
                                }
                            }
                        }
                    }
                }
            }
            let mut queued = Vec::new();
            if let Some(receiver) = &state.commands {
                let mut receiver = receiver.lock().await;
                while let Ok(command) = receiver.try_recv() { queued.push(command); }
            }
            let follow_target = if let Some(target_lock) = &state.follow_target {
                target_lock.lock().await.clone()
            } else { None };
            if let Some(username) = follow_target {
                let should_repath = if let Some(cooldown) = &state.last_follow_repath {
                    let mut last = cooldown.lock().await;
                    if last.elapsed() >= Duration::from_millis(1200) {
                        *last = Instant::now();
                        true
                    } else { false }
                } else { false };
                if should_repath {
                    let goal = bot.player_uuid_by_username(&username)
                        .and_then(|uuid| bot.entity_by_uuid(uuid))
                        .map(|entity| entity.position())
                        .map(|position| BlockPos::new(position.x.floor() as i32 + 1, position.y.floor() as i32, position.z.floor() as i32));
                    if let Some(goal) = goal {
                        bot.start_goto(BlockPosGoal(goal));
                        if let Some(following) = &state.is_following { following.store(true, Ordering::Relaxed); }
                    } else {
                        bot.stop_pathfinding();
                    }
                }
            } else if let Some(following) = &state.is_following {
                if following.swap(false, Ordering::Relaxed) {
                    bot.stop_pathfinding();
                    bot.walk(WalkDirection::None);
                }
            }
            for command in queued {
                match command {
                    BotCommand::Say(text) => {
                        let safe: String = text.chars().take(240).collect();
                        if !safe.trim().is_empty() { bot.chat(safe); }
                    }
                    BotCommand::AttackCrosshair => {
                        if !bot.has_attack_cooldown() {
                            match bot.hit_result() {
                                HitResult::Entity(hit) => {
                                    let target_position = bot.entity_ref_for(hit.entity).position();
                                    let own_position = bot.position();
                                    let dx = target_position.x - own_position.x;
                                    let dy = target_position.y - own_position.y;
                                    let dz = target_position.z - own_position.z;
                                    if dx * dx + dy * dy + dz * dz <= 12.25 {
                                        bot.look_at(target_position);
                                        bot.attack(hit.entity);
                                    } else {
                                        bot.chat("Target is outside safe melee range.");
                                    }
                                }
                                _ => bot.chat("No visible entity is in my crosshair."),
                            }
                        }
                    }
                    BotCommand::Stop => {
                        if let Some(pending) = &state.pending_block_action { *pending.lock().await = None; }
                        if let Some(target) = &state.follow_target { *target.lock().await = None; }
                        if let Some(following) = &state.is_following { following.store(false, Ordering::Relaxed); }
                        bot.stop_pathfinding();
                        bot.walk(WalkDirection::None);
                    }
                    BotCommand::Follow(username) => {
                        if let Some(pending) = &state.pending_block_action { *pending.lock().await = None; }
                        if let Some(target) = &state.follow_target { *target.lock().await = Some(username); }
                    }
                    BotCommand::Look(x, y, z) => {
                        if let Some(pending) = &state.pending_block_action { *pending.lock().await = None; }
                        if let Some(target) = &state.follow_target { *target.lock().await = None; }
                        if let Some(following) = &state.is_following { following.store(false, Ordering::Relaxed); }
                        bot.stop_pathfinding();
                        bot.look_at(BlockPos::new(x, y, z).center());
                    }
                    BotCommand::Goto(x, y, z) => {
                        if let Some(pending) = &state.pending_block_action { *pending.lock().await = None; }
                        if let Some(target) = &state.follow_target { *target.lock().await = None; }
                        if let Some(following) = &state.is_following { following.store(false, Ordering::Relaxed); }
                        bot.start_goto(BlockPosGoal(BlockPos::new(x, y, z)));
                    }
                    BotCommand::Mine(x, y, z) => {
                        if let Some(target) = &state.follow_target { *target.lock().await = None; }
                        if let Some(following) = &state.is_following { following.store(false, Ordering::Relaxed); }
                        bot.stop_pathfinding();
                        let position = BlockPos::new(x, y, z);
                        if let Some(pending) = &state.pending_block_action {
                            *pending.lock().await = Some(PendingBlockAction {
                                kind: PendingBlockActionKind::Mine, position, attempts: 0,
                            });
                        }
                        bot.look_at(position.center());
                    }
                    BotCommand::Place(x, y, z) => {
                        if let Some(target) = &state.follow_target { *target.lock().await = None; }
                        if let Some(following) = &state.is_following { following.store(false, Ordering::Relaxed); }
                        bot.stop_pathfinding();
                        let position = BlockPos::new(x, y, z);
                        if let Some(pending) = &state.pending_block_action {
                            *pending.lock().await = Some(PendingBlockAction {
                                kind: PendingBlockActionKind::Place, position, attempts: 0,
                            });
                        }
                        bot.look_at(position.center());
                    }
                    BotCommand::Equip(slot) => bot.set_selected_hotbar_slot(slot),
                }
            }
        }
        Event::Chat(message) => {
            let sender = message.sender();
            let content = message.content();
            tracing::info!(bot = %bot.username(), sender = ?sender, %content, "Minecraft chat");

            let own_username = bot.username();
            let from_self = sender.as_deref() == Some(own_username.as_str());
            let addressed_to_bot = sender.is_some() && content.to_lowercase().contains("mindcraft");
            if state.is_primary && !from_self && addressed_to_bot && !content.trim().is_empty() {
                let allowed = if let Some(cooldown) = &state.last_reply {
                    let mut last = cooldown.lock().await;
                    if last.elapsed() >= Duration::from_secs(4) {
                        *last = Instant::now();
                        true
                    } else { false }
                } else { false };

                if allowed {
                    if let Some(brain) = &state.brain {
                        let prompt = format!(
                            "A Minecraft player addressed you in server chat. Respond as a concise teammate. Do not claim to have performed any in-game action. Message: {content}"
                        );
                        match brain.reply(&prompt).await {
                            Ok(reply) => {
                                let safe_reply: String = reply.chars().take(240).collect();
                                if !safe_reply.trim().is_empty()
                                    && !safe_reply.trim_start().starts_with('/')
                                    && !safe_reply.chars().any(|c| c.is_control()) {
                                    bot.chat(safe_reply);
                                }
                            }
                            Err(error) => {
                                tracing::error!(%error, "AI chat response failed");
                                bot.chat("I couldn't reach my AI provider just now.");
                            }
                        }
                    }
                }
            }
        }
        _ => {}
    }
    Ok(())
}
