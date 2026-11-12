use crate::{api::RuntimeStatus, brain::Brain};
use anyhow::Result;
use azalea::{pathfinder::{PathfinderClientExt, goals::BlockPosGoal}, prelude::*, swarm::SwarmBuilder};
use std::{sync::{atomic::{AtomicBool, Ordering}, Arc}, time::{Duration, Instant}};
use tokio::sync::{mpsc, Mutex};

#[derive(Debug)]
pub enum BotCommand {
    Say(String),
    Stop,
    Look(i32, i32, i32),
    Goto(i32, i32, i32),
    Mine(i32, i32, i32),
    Equip(u8),
}

#[derive(Clone, Component, Default)]
struct BotState {
    brain: Option<Arc<Brain>>,
    status: Option<Arc<RuntimeStatus>>,
    last_reply: Option<Arc<Mutex<Instant>>>,
    commands: Option<Arc<Mutex<mpsc::Receiver<BotCommand>>>>,
    connected: Option<Arc<AtomicBool>>,
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

    let accounts_to_join = if auth_mode == "microsoft" {
        let email = microsoft_email.ok_or_else(|| anyhow::anyhow!("MINDCRAFT_MICROSOFT_EMAIL is required for Microsoft authentication"))?;
        tracing::info!("starting Azalea Microsoft device-code authentication");
        vec![Account::microsoft(email.as_str()).await?]
    } else {
        accounts.into_iter().map(Account::offline).collect::<Vec<_>>()
    };
    if accounts_to_join.is_empty() {
        anyhow::bail!("at least one Minecraft account is required");
    }

    let (command_tx, command_rx) = mpsc::channel(64);
    *status.command_tx.lock().await = Some(command_tx);
    let shared_receiver = Arc::new(Mutex::new(command_rx));
    let shared_reply_cooldown = Arc::new(Mutex::new(Instant::now() - Duration::from_secs(10)));

    let mut builder = SwarmBuilder::new().set_handler(handle);
    for account in accounts_to_join {
        let state = make_state(
            brain.clone(), status.clone(), shared_receiver.clone(), shared_reply_cooldown.clone(),
        );
        builder = builder.add_account_with_state(account, state);
    }

    let result = builder.join_delay(Duration::from_millis(500)).start(server.as_str()).await;
    status.minecraft_connected.store(false, Ordering::Relaxed);
    status.connected_agents.store(0, Ordering::Relaxed);
    *status.command_tx.lock().await = None;
    tracing::warn!("Azalea swarm stopped: {result:?}");
    Ok(())
}

fn make_state(
    brain: Arc<Brain>,
    status: Arc<RuntimeStatus>,
    commands: Arc<Mutex<mpsc::Receiver<BotCommand>>>,
    last_reply: Arc<Mutex<Instant>>,
) -> BotState {
    BotState {
        brain: Some(brain),
        status: Some(status),
        last_reply: Some(last_reply),
        commands: Some(commands),
        connected: Some(Arc::new(AtomicBool::new(false))),
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
            tracing::warn!(username = %bot.username(), "Minecraft agent disconnected; Azalea reconnect policy will handle retry");
        }
        Event::Tick => {
            if let Some(status) = &state.status {
                if let (Ok(position), Ok(health)) = (bot.position(), bot.health()) {
                    let inventory = bot.menu().map(|menu| {
                        menu.slots().iter().enumerate()
                            .filter(|(_, item)| !item.is_empty())
                            .map(|(slot, item)| format!("{slot}: {item:?}"))
                            .collect::<Vec<_>>()
                    }).unwrap_or_default();
                    *status.world.lock().await = Some(crate::api::WorldSnapshot {
                        agent: bot.username(),
                        position: [position.x as f64, position.y as f64, position.z as f64],
                        health,
                        inventory,
                    });
                }
            }
            let mut queued = Vec::new();
            if let Some(receiver) = &state.commands {
                let mut receiver = receiver.lock().await;
                while let Ok(command) = receiver.try_recv() { queued.push(command); }
            }
            for command in queued {
                match command {
                    BotCommand::Say(text) => {
                        let safe: String = text.chars().take(240).collect();
                        if !safe.trim().is_empty() { bot.chat(safe); }
                    }
                    BotCommand::Stop => {
                        bot.stop_pathfinding();
                        bot.walk(WalkDirection::None);
                    }
                    BotCommand::Look(x, y, z) => bot.look_at(BlockPos::new(x, y, z).center()),
                    BotCommand::Goto(x, y, z) => bot.start_goto(BlockPosGoal(BlockPos::new(x, y, z))),
                    BotCommand::Mine(x, y, z) => bot.start_mining(BlockPos::new(x, y, z)),
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
