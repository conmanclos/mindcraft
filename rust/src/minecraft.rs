use crate::{api::RuntimeStatus, brain::Brain};
use anyhow::Result;
use azalea::{pathfinder::{PathfinderClientExt, goals::BlockPosGoal}, prelude::*};
use std::{sync::Arc, time::{Duration, Instant}};
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
    bot_name: Option<String>,
    last_reply: Option<Arc<Mutex<Instant>>>,
    commands: Option<Arc<Mutex<mpsc::Receiver<BotCommand>>>>,
}

pub async fn connect(
    server: String,
    username: String,
    auth_mode: String,
    microsoft_email: Option<String>,
    brain: Arc<Brain>,
    status: Arc<RuntimeStatus>,
) -> Result<()> {
    tracing::info!(%server, %username, "connecting Minecraft client (offline account mode)");
    let (command_tx, command_rx) = mpsc::channel(64);
    *status.command_tx.lock().await = Some(command_tx);
    let account = if auth_mode == "microsoft" {
        let email = microsoft_email.ok_or_else(|| anyhow::anyhow!("MINDCRAFT_MICROSOFT_EMAIL is required for Microsoft authentication"))?;
        tracing::info!("starting Azalea Microsoft device-code authentication");
        Account::microsoft(email.as_str()).await?
    } else {
        Account::offline(username.clone())
    };
    let state = BotState {
        brain: Some(brain),
        status: Some(status.clone()),
        bot_name: Some(username),
        last_reply: Some(Arc::new(Mutex::new(Instant::now() - Duration::from_secs(10)))),
        commands: Some(Arc::new(Mutex::new(command_rx))),
    };
    let result = ClientBuilder::new()
        .set_handler(handle)
        .set_state(state)
        .start(account, server.as_str())
        .await;
    status.minecraft_connected.store(false, std::sync::atomic::Ordering::Relaxed);
    *status.command_tx.lock().await = None;
    tracing::warn!("Azalea client stopped: {result:?}");
    Ok(())
}

async fn handle(bot: Client, event: Event, state: BotState) -> eyre::Result<()> {
    match event {
        Event::Spawn => {
            if let Some(status) = &state.status {
                status.minecraft_connected.store(true, std::sync::atomic::Ordering::Relaxed);
            }
            tracing::info!(username = %bot.username(), "Minecraft bot spawned and ready");
        }
        Event::Disconnect(_) => {
            if let Some(status) = &state.status {
                status.minecraft_connected.store(false, std::sync::atomic::Ordering::Relaxed);
            }
            tracing::warn!("Minecraft connection lost; Azalea reconnect policy will handle retry");
        }
        Event::Tick => {
            if let Some(status) = &state.status {
                if let (Ok(position), Ok(health)) = (bot.position(), bot.health()) {
                    *status.world.lock().await = Some(crate::api::WorldSnapshot {
                        position: [position.x as f64, position.y as f64, position.z as f64],
                        health,
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
                    BotCommand::Look(x, y, z) => {
                        bot.look_at(BlockPos::new(x, y, z).center());
                    }
                    BotCommand::Goto(x, y, z) => {
                        bot.start_goto(BlockPosGoal(BlockPos::new(x, y, z)));
                    }
                    BotCommand::Mine(x, y, z) => {
                        bot.start_mining(BlockPos::new(x, y, z));
                    }
                    BotCommand::Equip(slot) => bot.set_selected_hotbar_slot(slot),
                }
            }
        }
        Event::Chat(message) => {
            let sender = message.sender();
            let content = message.content();
            tracing::info!(sender = ?sender, %content, "Minecraft chat");

            let from_self = sender.as_deref() == state.bot_name.as_deref();
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
                            "A Minecraft player addressed you in server chat. Respond as a concise teammate.                              Do not claim to have performed any in-game action. Message: {content}"
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
