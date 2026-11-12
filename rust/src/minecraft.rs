use crate::{api::RuntimeStatus, brain::Brain};
use anyhow::Result;
use azalea::prelude::*;
use std::{sync::Arc, time::{Duration, Instant}};
use tokio::sync::Mutex;

#[derive(Clone, Component, Default)]
struct BotState {
    brain: Option<Arc<Brain>>,
    status: Option<Arc<RuntimeStatus>>,
    bot_name: Option<String>,
    last_reply: Option<Arc<Mutex<Instant>>>,
}

pub async fn connect(
    server: String,
    username: String,
    brain: Arc<Brain>,
    status: Arc<RuntimeStatus>,
) -> Result<()> {
    tracing::info!(%server, %username, "connecting Minecraft client (offline account mode)");
    status.minecraft_connected.store(false, std::sync::atomic::Ordering::Relaxed);
    let account = Account::offline(username.clone());
    let state = BotState {
        brain: Some(brain),
        status: Some(status.clone()),
        bot_name: Some(username),
        last_reply: Some(Arc::new(Mutex::new(Instant::now() - Duration::from_secs(10)))),
    };
    let result = ClientBuilder::new()
        .set_handler(handle)
        .set_state(state)
        .start(account, server.as_str())
        .await;
    status.minecraft_connected.store(false, std::sync::atomic::Ordering::Relaxed);
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
        Event::Disconnect(_) | Event::ConnectionFailed(_) => {
            if let Some(status) = &state.status {
                status.minecraft_connected.store(false, std::sync::atomic::Ordering::Relaxed);
            }
            tracing::warn!("Minecraft connection lost; Azalea reconnect policy will handle retry");
        }
        Event::Chat(message) => {
            let sender = message.sender();
            let content = message.content();
            tracing::info!(sender = ?sender, %content, "Minecraft chat");

            let from_self = sender.as_deref() == state.bot_name.as_deref();
            let addressed_to_bot = message.is_whisper()
                || content.to_lowercase().contains("mindcraft");
            if !from_self && addressed_to_bot && !content.trim().is_empty() {
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
                    if let Some(brain) = &state.brain {
                        let prompt = format!(
                            "A Minecraft player addressed you in server chat. Respond as a concise teammate.                              Do not claim to have performed any in-game action. Message: {content}"
                        );
                        match brain.reply(&prompt).await {
                            Ok(reply) => {
                                let safe_reply: String = reply.chars().take(240).collect();
                                if !safe_reply.trim().is_empty() {
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
