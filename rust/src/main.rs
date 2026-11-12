mod api;
mod brain;
mod config;
mod minecraft;
mod store;

use anyhow::Result;
use clap::{Parser, Subcommand};
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "mindcraft", version, about = "Native Rust Minecraft AI teammate")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Start the shared runtime, dashboard, API, and optional Minecraft connection.
    Serve,
    /// Send a one-off prompt to the configured AI provider.
    Chat { prompt: String },
    /// Print a safe configuration and dependency diagnostic.
    Doctor,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let cli = Cli::parse();
    let config = Arc::new(config::Config::from_env()?);

    match cli.command.unwrap_or(Command::Serve) {
        Command::Serve => {
            let store = Arc::new(store::Store::open(&config.database)?);
            let brain = Arc::new(brain::Brain::new(config.clone()));
            let status = Arc::new(api::RuntimeStatus::default());

            if let Some(server) = config.minecraft_server.clone() {
                let bot_brain = brain.clone();
                let bot_status = status.clone();
                let bot_username = config.minecraft_username.clone();
                tokio::spawn(async move {
                    if let Err(error) = minecraft::connect(server, bot_username, bot_brain, bot_status).await {
                        tracing::error!(%error, "Minecraft runtime stopped");
                    }
                });
            } else {
                tracing::warn!("MINDCRAFT_MC_SERVER is unset; dashboard/API will start without a Minecraft connection");
            }

            tracing::info!(bind = %config.bind, mode = %config.mode, "Mindcraft runtime starting");
            if config.api_token_was_generated {
                tracing::warn!(token = %config.api_token, "Generated one-time dashboard API token; copy it now");
            }
            api::serve(config, brain, store, status).await?;
        }
        Command::Chat { prompt } => {
            let brain = brain::Brain::new(config);
            let response = brain.reply(&prompt).await?;
            println!("{response}");
        }
        Command::Doctor => {
            println!("Mindcraft native runtime");
            println!("Minecraft target: 1.21.1 (Azalea 0.10.3+mc1.21.1)");
            println!("Provider: {}", config.provider);
            println!("Model: {}", config.model);
            println!("Minecraft server configured: {}", config.minecraft_server.is_some());
            println!("API bind: {}", config.bind);
            println!("API token configured: {}", !config.api_token.is_empty());
            println!("Database: {}", config.database.display());
            println!("Autonomy: {}", config.mode);
        }
    }
    Ok(())
}
