use crate::{api::RuntimeStatus, brain::Brain};
use anyhow::Result;
use azalea::prelude::*;
use std::sync::Arc;

#[derive(Clone, Component, Default)]
struct BotState;

pub async fn connect(
    server: String,
    username: String,
    _brain: Arc<Brain>,
    status: Arc<RuntimeStatus>,
) -> Result<()> {
    tracing::info!(%server, %username, "connecting Minecraft client (offline account mode)");
    status.minecraft_connected.store(false, std::sync::atomic::Ordering::Relaxed);
    let account = Account::offline(username);
    let result = ClientBuilder::new()
        .set_handler(handle)
        .start(account, server.as_str())
        .await;
    tracing::warn!("Azalea client stopped: {result:?}");
    status.minecraft_connected.store(false, std::sync::atomic::Ordering::Relaxed);
    Ok(())
}

async fn handle(_bot: Client, event: Event, _state: BotState) -> eyre::Result<()> {
    match event {
        Event::Chat(message) => {
            tracing::info!(message = %message.message().to_ansi(), "Minecraft chat");
        }
        _ => {}
    }
    Ok(())
}
