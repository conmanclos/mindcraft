use crate::{api::RuntimeStatus, brain::Brain};
use anyhow::Result;
use azalea::{pathfinder::{PathfinderClientExt, goals::BlockPosGoal}, prelude::*, swarm::SwarmBuilder};
use std::{sync::{atomic::{AtomicBool, Ordering}, Arc}, time::{Duration, Instant}};
use tokio::sync::{mpsc, Mutex};

#[derive(Debug)]
pub enum BotCommand {
    Say(String),
    Stop,
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
        let shared_follow_target = Arc::new(Mutex::new(None));

        let mut builder = SwarmBuilder::new().set_handler(handle);
        for account in accounts_to_join {
            let state = make_state(
                brain.clone(), status.clone(), shared_receiver.clone(), shared_reply_cooldown.clone(), shared_follow_target.clone(),
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
    commands: Arc<Mutex<mpsc::Receiver<BotCommand>>>,
    last_reply: Arc<Mutex<Instant>>,
    follow_target: Arc<Mutex<Option<String>>>,
) -> BotState {
    BotState {
        brain: Some(brain),
        status: Some(status),
        last_reply: Some(last_reply),
        commands: Some(commands),
        connected: Some(Arc::new(AtomicBool::new(false))),
        last_reflex: Some(Arc::new(Mutex::new(Instant::now() - Duration::from_secs(20)))),
        follow_target: Some(follow_target),
        is_following: Some(Arc::new(AtomicBool::new(false))),
        last_follow_repath: Some(Arc::new(Mutex::new(Instant::now() - Duration::from_secs(5)))),
        last_snapshot: Some(Arc::new(Mutex::new(Instant::now() - Duration::from_secs(2)))),
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
        Event::Disconnect(_) => {
            if let Some(connected) = &state.connected {
                if connected.swap(false, Ordering::Relaxed) {
                    if let Some(status) = &state.status {
                        let count = status.connected_agents.fetch_sub(1, Ordering::Relaxed).saturating_sub(1);
                        status.minecraft_connected.store(count > 0, Ordering::Relaxed);
                    }
                }
            }
            tracing::warn!(username = %bot.username(), "Minecraft agent disconnected; restarting the swarm");
            bot.exit();
        }
        Event::Tick => {
            if bot.exists() {
                let health = bot.health();
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
                            inventory,
                        });
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
                    if should_warn {
                        match reflex_action {
                            crate::reflex::ReflexAction::StopAndWarn => {
                                bot.stop_pathfinding();
                                bot.walk(WalkDirection::None);
                                bot.chat("Reflex safety: health is critical. I stopped moving; recover before continuing.");
                                tracing::warn!(username = %bot.username(), health, "critical-health reflex activated");
                            }
                            crate::reflex::ReflexAction::WarnLowFood => {
                                bot.chat("Reflex safety: food is low. Find food before continuing.");
                                tracing::warn!(username = %bot.username(), food = hunger.food, "low-food reflex activated");
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
                    BotCommand::Stop => {
                        if let Some(target) = &state.follow_target { *target.lock().await = None; }
                        if let Some(following) = &state.is_following { following.store(false, Ordering::Relaxed); }
                        bot.stop_pathfinding();
                        bot.walk(WalkDirection::None);
                    }
                    BotCommand::Follow(username) => {
                        if let Some(target) = &state.follow_target { *target.lock().await = Some(username); }
                    }
                    BotCommand::Look(x, y, z) => {
                        if let Some(target) = &state.follow_target { *target.lock().await = None; }
                        if let Some(following) = &state.is_following { following.store(false, Ordering::Relaxed); }
                        bot.stop_pathfinding();
                        bot.look_at(BlockPos::new(x, y, z).center());
                    }
                    BotCommand::Goto(x, y, z) => {
                        if let Some(target) = &state.follow_target { *target.lock().await = None; }
                        if let Some(following) = &state.is_following { following.store(false, Ordering::Relaxed); }
                        bot.start_goto(BlockPosGoal(BlockPos::new(x, y, z)));
                    }
                    BotCommand::Mine(x, y, z) => {
                        if let Some(target) = &state.follow_target { *target.lock().await = None; }
                        if let Some(following) = &state.is_following { following.store(false, Ordering::Relaxed); }
                        bot.stop_pathfinding();
                        bot.start_mining(BlockPos::new(x, y, z));
                    }
                    BotCommand::Place(x, y, z) => {
                        if let Some(target) = &state.follow_target { *target.lock().await = None; }
                        if let Some(following) = &state.is_following { following.store(false, Ordering::Relaxed); }
                        bot.stop_pathfinding();
                        let position = BlockPos::new(x, y, z);
                        bot.look_at(position.center());
                        bot.block_interact(position);
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
            if !from_self && addressed_to_bot && !content.trim().is_empty() {
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
