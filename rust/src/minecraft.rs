use crate::{api::RuntimeStatus, brain::Brain};
use anyhow::Result;
use azalea::{
    BlockPos,
    pathfinder::{goals::{BlockPosGoal, RadiusGoal}, PathfinderClientExt},
    prelude::*,
    swarm::{Swarm, SwarmBuilder, SwarmEvent},
};
use azalea::{
    entity::{indexing::EntityUuidIndex, Position},
    interact::HitResultComponent,
    inventory::SetSelectedHotbarSlotEvent,
    movement::{StartWalkEvent, WalkDirection},
    world::MinecraftEntityId,
};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, Mutex};

#[derive(Clone, Copy)]
enum PendingBlockActionKind {
    Mine,
    Place,
}

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
    AttackPlayer(String),
    Follow(String),
    Look(i32, i32, i32),
    Goto(i32, i32, i32),
    Mine(i32, i32, i32),
    Place(i32, i32, i32),
    Equip(u8),
    Jump,
    UseItem,
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
    is_primary: bool,
}

#[derive(Clone, Default, Resource)]
struct SwarmRuntimeState {
    brain: Option<Arc<Brain>>,
    status: Option<Arc<RuntimeStatus>>,
    commands: Option<Arc<Mutex<mpsc::Receiver<BotCommand>>>>,
    last_reply: Option<Arc<Mutex<Instant>>>,
    follow_target: Option<Arc<Mutex<Option<String>>>>,
    pending_block_action: Option<Arc<Mutex<Option<PendingBlockAction>>>>,
    primary_username: Option<String>,
}

impl SwarmRuntimeState {
    fn bot_state(&self, is_primary: bool) -> BotState {
        BotState {
            brain: self.brain.clone(),
            status: self.status.clone(),
            last_reply: self.last_reply.clone(),
            commands: if is_primary { self.commands.clone() } else { None },
            connected: Some(Arc::new(AtomicBool::new(false))),
            last_reflex: Some(Arc::new(Mutex::new(Instant::now() - Duration::from_secs(20)))),
            follow_target: self.follow_target.clone(),
            is_following: Some(Arc::new(AtomicBool::new(false))),
            last_follow_repath: Some(Arc::new(Mutex::new(Instant::now() - Duration::from_secs(5)))),
            last_snapshot: Some(Arc::new(Mutex::new(Instant::now() - Duration::from_secs(2)))),
            pending_block_action: self.pending_block_action.clone(),
            is_primary,
        }
    }
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
            let email = microsoft_email.as_deref()
                .ok_or_else(|| anyhow::anyhow!("MINDCRAFT_MICROSOFT_EMAIL is required for Microsoft authentication"))?;
            match Account::microsoft(email).await {
                Ok(account) => vec![account],
                Err(error) => {
                    tracing::error!(%error, "Microsoft authentication failed; retrying in 30 seconds");
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    continue;
                }
            }
        } else {
            accounts.iter().map(|name| Account::offline(name)).collect::<Vec<_>>()
        };

        if accounts_to_join.is_empty() {
            anyhow::bail!("at least one Minecraft account is required");
        }

        let primary_username = accounts_to_join.first().map(|account| account.username.clone());
        let (command_tx, command_rx) = mpsc::channel(64);
        *status.command_tx.lock().await = Some(command_tx);
        let runtime_state = SwarmRuntimeState {
            brain: Some(brain.clone()),
            status: Some(status.clone()),
            commands: Some(Arc::new(Mutex::new(command_rx))),
            last_reply: Some(Arc::new(Mutex::new(Instant::now() - Duration::from_secs(10)))),
            follow_target: Some(Arc::new(Mutex::new(None))),
            pending_block_action: Some(Arc::new(Mutex::new(None))),
            primary_username,
        };

        let mut builder = SwarmBuilder::new()
            .set_handler(handle)
            .set_swarm_handler(handle_swarm)
            .set_swarm_state(runtime_state.clone());

        for (index, account) in accounts_to_join.into_iter().enumerate() {
            builder = builder.add_account_with_state(account, runtime_state.bot_state(index == 0));
        }

        match builder.join_delay(Duration::from_millis(500)).start(server.as_str()).await {
            Ok(never) => match never {},
            Err(error) => {
                status.minecraft_connected.store(false, Ordering::Relaxed);
                status.connected_agents.store(0, Ordering::Relaxed);
                *status.command_tx.lock().await = None;
                *status.world.lock().await = None;
                tracing::error!(%error, "Minecraft swarm failed to start; retrying in 5 seconds");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

async fn handle_swarm(
    mut swarm: Swarm,
    event: SwarmEvent,
    state: SwarmRuntimeState,
) -> anyhow::Result<()> {
    if let SwarmEvent::Disconnect(account, join_opts) = event {
        let is_primary = state.primary_username.as_deref()
            .is_some_and(|primary| primary.eq_ignore_ascii_case(&account.username));
        tracing::warn!(username = %account.username, "Minecraft agent disconnected; reconnecting in 5 seconds");
        tokio::time::sleep(Duration::from_secs(5)).await;
        match swarm.add_with_opts(&account, state.bot_state(is_primary), &join_opts).await {
            Ok(_) => tracing::info!(username = %account.username, "Minecraft agent reconnected"),
            Err(error) => tracing::error!(%error, username = %account.username, "Minecraft reconnect attempt failed"),
        }
    }
    Ok(())
}

async fn handle(mut bot: Client, event: Event, state: BotState) -> anyhow::Result<()> {
    match event {
        Event::Login => {
            if let Some(connected) = &state.connected {
                if !connected.swap(true, Ordering::Relaxed) {
                    if let Some(status) = &state.status {
                        let count = status.connected_agents.fetch_add(1, Ordering::Relaxed) + 1;
                        status.minecraft_connected.store(count > 0, Ordering::Relaxed);
                    }
                }
            }
            tracing::info!(username = %bot.username(), "Minecraft agent logged in");
        }
        Event::Disconnect(reason) => {
            if let Some(connected) = &state.connected {
                if connected.swap(false, Ordering::Relaxed) {
                    if let Some(status) = &state.status {
                        let count = status.connected_agents.fetch_sub(1, Ordering::Relaxed).saturating_sub(1);
                        status.minecraft_connected.store(count > 0, Ordering::Relaxed);
                    }
                }
            }
            tracing::warn!(username = %bot.username(), ?reason, "Minecraft agent disconnected");
        }
        Event::Tick => {
            let mut critical_health = false;
            if bot.logged_in() {
                let health = bot.health();
                let hunger = bot.hunger();
                critical_health = health.is_finite() && health <= 6.0;

                if critical_health {
                    if let Some(pending) = &state.pending_block_action {
                        *pending.lock().await = None;
                    }
                }

                if state.is_primary {
                    if let Some(status) = &state.status {
                        let should_snapshot = if let Some(last) = &state.last_snapshot {
                            let mut last = last.lock().await;
                            if last.elapsed() >= Duration::from_secs(1) {
                                *last = Instant::now();
                                true
                            } else {
                                false
                            }
                        } else {
                            false
                        };

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
                                position: [position.x, position.y, position.z],
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
                        } else {
                            false
                        }
                    } else {
                        false
                    };

                    match reflex_action {
                        crate::reflex::ReflexAction::StopAndWarn => {
                            bot.stop_pathfinding();
                            bot.ecs.lock().send_event(StartWalkEvent {
                                entity: bot.entity,
                                direction: WalkDirection::None,
                            });
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
                        let hit = bot.get_component::<HitResultComponent>();
                        if let Some(hit) = hit.filter(|hit| !hit.miss && hit.block_pos == pending.position) {
                            match pending.kind {
                                PendingBlockActionKind::Mine => {
                                    let mut mining_bot = bot.clone();
                                    let position = pending.position;
                                    tokio::task::spawn_local(async move {
                                        mining_bot.mine(position).await;
                                    });
                                }
                                PendingBlockActionKind::Place => bot.block_interact(pending.position),
                            }
                        } else {
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

            let mut queued = Vec::new();
            if let Some(receiver) = &state.commands {
                let mut receiver = receiver.lock().await;
                while let Ok(command) = receiver.try_recv() {
                    queued.push(command);
                }
            }

            let follow_target = if let Some(target_lock) = &state.follow_target {
                target_lock.lock().await.clone()
            } else {
                None
            };

            if critical_health {
                bot.stop_pathfinding();
                bot.ecs.lock().send_event(StartWalkEvent {
                    entity: bot.entity,
                    direction: WalkDirection::None,
                });
            } else if let Some(username) = follow_target {
                let should_repath = if let Some(cooldown) = &state.last_follow_repath {
                    let mut last = cooldown.lock().await;
                    if last.elapsed() >= Duration::from_millis(1200) {
                        *last = Instant::now();
                        true
                    } else {
                        false
                    }
                } else {
                    false
                };

                if should_repath {
                    if let Some((position, _)) = find_player(&bot, &username) {
                        bot.goto_without_mining(RadiusGoal { pos: position, radius: 3.0 });
                        if let Some(following) = &state.is_following {
                            following.store(true, Ordering::Relaxed);
                        }
                    } else {
                        bot.stop_pathfinding();
                    }
                }
            } else if let Some(following) = &state.is_following {
                if following.swap(false, Ordering::Relaxed) {
                    bot.stop_pathfinding();
                    bot.ecs.lock().send_event(StartWalkEvent {
                        entity: bot.entity,
                        direction: WalkDirection::None,
                    });
                }
            }

            for command in queued {
                if critical_health && matches!(
                    &command,
                    BotCommand::Follow(_) | BotCommand::Goto(_, _, _) | BotCommand::Mine(_, _, _)
                        | BotCommand::Place(_, _, _) | BotCommand::AttackPlayer(_) | BotCommand::Jump
                ) {
                    tracing::debug!(username = %bot.username(), "blocked gameplay action while health is critical");
                    continue;
                }

                match command {
                    BotCommand::Say(text) => {
                        let safe: String = text.chars().take(240).collect();
                        if !safe.trim().is_empty() {
                            bot.chat(&safe);
                        }
                    }
                    BotCommand::AttackPlayer(username) => {
                        if !bot.has_attack_cooldown() {
                            if let Some((target_position, target_id)) = find_player(&bot, &username) {
                                let own_position = bot.position();
                                let dx = target_position.x - own_position.x;
                                let dy = target_position.y - own_position.y;
                                let dz = target_position.z - own_position.z;
                                if dx * dx + dy * dy + dz * dz <= 12.25 {
                                    bot.look_at(target_position);
                                    bot.attack(target_id);
                                } else {
                                    bot.chat("Target is outside safe melee range.");
                                }
                            } else {
                                bot.chat("I can't find that player in the loaded world.");
                            }
                        }
                    }
                    BotCommand::Stop => {
                        if let Some(pending) = &state.pending_block_action {
                            *pending.lock().await = None;
                        }
                        if let Some(target) = &state.follow_target {
                            *target.lock().await = None;
                        }
                        if let Some(following) = &state.is_following {
                            following.store(false, Ordering::Relaxed);
                        }
                        bot.stop_pathfinding();
                        bot.ecs.lock().send_event(StartWalkEvent {
                            entity: bot.entity,
                            direction: WalkDirection::None,
                        });
                    }
                    BotCommand::Follow(username) => {
                        if let Some(pending) = &state.pending_block_action {
                            *pending.lock().await = None;
                        }
                        if let Some(target) = &state.follow_target {
                            *target.lock().await = Some(username);
                        }
                    }
                    BotCommand::Look(x, y, z) => {
                        if let Some(pending) = &state.pending_block_action {
                            *pending.lock().await = None;
                        }
                        if let Some(target) = &state.follow_target {
                            *target.lock().await = None;
                        }
                        if let Some(following) = &state.is_following {
                            following.store(false, Ordering::Relaxed);
                        }
                        bot.stop_pathfinding();
                        bot.look_at(BlockPos::new(x, y, z).center());
                    }
                    BotCommand::Goto(x, y, z) => {
                        if let Some(pending) = &state.pending_block_action {
                            *pending.lock().await = None;
                        }
                        if let Some(target) = &state.follow_target {
                            *target.lock().await = None;
                        }
                        if let Some(following) = &state.is_following {
                            following.store(false, Ordering::Relaxed);
                        }
                        bot.goto_without_mining(BlockPosGoal(BlockPos::new(x, y, z)));
                    }
                    BotCommand::Mine(x, y, z) => {
                        if let Some(target) = &state.follow_target {
                            *target.lock().await = None;
                        }
                        if let Some(following) = &state.is_following {
                            following.store(false, Ordering::Relaxed);
                        }
                        bot.stop_pathfinding();
                        let position = BlockPos::new(x, y, z);
                        if let Some(pending) = &state.pending_block_action {
                            *pending.lock().await = Some(PendingBlockAction {
                                kind: PendingBlockActionKind::Mine,
                                position,
                                attempts: 0,
                            });
                        }
                        bot.look_at(position.center());
                    }
                    BotCommand::Place(x, y, z) => {
                        if let Some(target) = &state.follow_target {
                            *target.lock().await = None;
                        }
                        if let Some(following) = &state.is_following {
                            following.store(false, Ordering::Relaxed);
                        }
                        bot.stop_pathfinding();
                        let position = BlockPos::new(x, y, z);
                        if let Some(pending) = &state.pending_block_action {
                            *pending.lock().await = Some(PendingBlockAction {
                                kind: PendingBlockActionKind::Place,
                                position,
                                attempts: 0,
                            });
                        }
                        bot.look_at(position.center());
                    }
                    BotCommand::Equip(slot) => {
                        bot.ecs.lock().send_event(SetSelectedHotbarSlotEvent {
                            entity: bot.entity,
                            slot,
                        });
                    }
                    BotCommand::Jump => bot.jump(),
                    BotCommand::UseItem => {
                        if let Some(hit) = bot.get_component::<HitResultComponent>() {
                            if !hit.miss {
                                bot.block_interact(hit.block_pos);
                            } else {
                                bot.chat("Aim at a block to use the held item.");
                            }
                        } else {
                            bot.chat("The current block target is unavailable.");
                        }
                    }
                }
            }
        }
        Event::Chat(message) => {
            let (sender, content) = message.split_sender_and_content();
            tracing::info!(bot = %bot.username(), sender = ?sender, %content, "Minecraft chat");

            let own_username = bot.username();
            let from_self = sender.as_deref() == Some(own_username.as_str());
            let addressed_to_bot = sender.is_some()
                && content.to_lowercase().contains("mindcraft");

            if state.is_primary && !from_self && addressed_to_bot && !content.trim().is_empty() {
                let allowed = if let Some(cooldown) = &state.last_reply {
                    let mut last = cooldown.lock().await;
                    if last.elapsed() >= Duration::from_secs(4) {
                        *last = Instant::now();
                        true
                    } else {
                        false
                    }
                } else {
                    false
                };

                if allowed {
                    if let Some(brain) = state.brain.clone() {
                        let reply_bot = bot.clone();
                        let prompt = format!(
                            "A Minecraft player addressed you in server chat. Respond as a concise teammate. Do not claim to have performed any in-game action. Message: {content}"
                        );
                        tokio::task::spawn_local(async move {
                            match brain.reply(&prompt).await {
                                Ok(reply) => {
                                    let safe_reply: String = reply.chars().take(240).collect();
                                    if reply_bot.logged_in()
                                        && !safe_reply.trim().is_empty()
                                        && !safe_reply.trim_start().starts_with('/')
                                        && !safe_reply.chars().any(|c| c.is_control())
                                    {
                                        reply_bot.chat(&safe_reply);
                                    }
                                }
                                Err(error) => {
                                    tracing::error!(%error, "AI chat response failed");
                                    if reply_bot.logged_in() {
                                        reply_bot.chat("I couldn't reach my AI provider just now.");
                                    }
                                }
                            }
                        });
                    }
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn find_player(bot: &Client, username: &str) -> Option<(azalea::Vec3, MinecraftEntityId)> {
    let uuid = bot.tab_list().values()
        .find(|player| player.profile.name.eq_ignore_ascii_case(username))?
        .uuid;

    let ecs = bot.ecs.lock();
    let entity = ecs.resource::<EntityUuidIndex>().get(&uuid)?;
    let position = ecs.get::<Position>(entity).copied()?;
    let entity_id = ecs.get::<MinecraftEntityId>(entity).copied()?;
    Some((*position, entity_id))
}
