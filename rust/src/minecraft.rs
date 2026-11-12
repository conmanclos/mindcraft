use crate::{api::RuntimeStatus, brain::Brain};
use anyhow::Result;
use azalea::{account::microsoft::MicrosoftAccountOpts, pathfinder::{PathfinderClientExt, goals::BlockPosGoal}, prelude::*, swarm::SwarmBuilder};
use std::{sync::{atomic::{AtomicBool, Ordering}, Arc}, time::{Duration, Instant}};
use tokio::sync::{mpsc, Mutex};

#[derive(Debug)]
pub enum BotCommand {
    Say(String),
    Stop,
    Jump,
    UseItem,
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
    last_auto_eat: Option<Arc<Mutex<Instant>>>,
    follow_target: Option<Arc<Mutex<Option<String>>>>,
    is_following: Option<Arc<AtomicBool>>,
    last_follow_repath: Option<Arc<Mutex<Instant>>>,
    last_snapshot: Option<Arc<Mutex<Instant>>>,
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
            let auth_opts = MicrosoftAccountOpts {
                cache_file: std::env::var_os("MINDCRAFT_AZALEA_AUTH_CACHE").map(std::path::PathBuf::from),
                ..Default::default()
            };
            match Account::microsoft_with_opts(email, auth_opts).await {
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

        let mut builder = SwarmBuilder::new().set_handler(handle);
        for (index, account) in accounts_to_join.into_iter().enumerate() {
            let is_primary = index == 0;
            let command_receiver = if is_primary { Some(shared_receiver.clone()) } else { None };
            let state = make_state(
                brain.clone(), status.clone(), command_receiver, shared_reply_cooldown.clone(),
                shared_follow_target.clone(), shared_reflex_cooldown.clone(), is_primary,
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
    is_primary: bool,
) -> BotState {
    BotState {
        brain: Some(brain),
        status: Some(status),
        last_reply: Some(last_reply),
        commands,
        connected: Some(Arc::new(AtomicBool::new(false))),
        last_reflex: Some(last_reflex),
        last_auto_eat: Some(Arc::new(Mutex::new(Instant::now() - Duration::from_secs(30)))),
        follow_target: Some(follow_target),
        is_following: Some(Arc::new(AtomicBool::new(false))),
        last_follow_repath: Some(Arc::new(Mutex::new(Instant::now() - Duration::from_secs(5)))),
        last_snapshot: Some(Arc::new(Mutex::new(Instant::now() - Duration::from_secs(2)))),
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
        Event::ConnectionFailed(error) => {
            tracing::warn!(username = %bot.username(), error = ?error, "Minecraft connection attempt failed; Azalea will retry");
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
            tracing::warn!(username = %bot.username(), reason = ?reason, "Minecraft agent disconnected; restarting the swarm");
            bot.exit();
        }
        Event::Tick => {
            let mut critical_health = false;
            if bot.exists() {
                let health = bot.health();
                let hunger = bot.hunger();
                critical_health = health.is_finite() && health <= 6.0;
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
                // Autonomous survival may eat only recognized food already in the hotbar.
                // Never consume arbitrary held items, potions, or items from an open container.
                if state.brain.as_ref().is_some_and(|brain| brain.is_autonomous())
                    && hunger.food <= 12
                    && !critical_health
                {
                    let should_eat = if let Some(last) = &state.last_auto_eat {
                        let mut last = last.lock().await;
                        if last.elapsed() >= Duration::from_secs(8) {
                            *last = Instant::now();
                            true
                        } else { false }
                    } else { false };
                    if should_eat {
                        let menu = bot.menu();
                        if let Some((slot, _)) = best_safe_food_slot(&menu) {
                            let hotbar = menu.hotbar_slots_range();
                            if hotbar.contains(&slot) {
                                let hotbar_index = (slot - *hotbar.start()) as u8;
                                bot.set_selected_hotbar_slot(hotbar_index);
                                // Azalea applies hotbar selection on the next ECS Update.
                                // Wait for that update so the right-click cannot consume
                                // whatever item happened to be selected previously.
                                bot.wait_updates(1).await;
                                bot.start_use_item();
                                tracing::info!(username = %bot.username(), hotbar_slot = hotbar_index, "autonomous safe-food reflex started eating");
                            }
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
            let mut queued = Vec::new();
            if let Some(receiver) = &state.commands {
                let mut receiver = receiver.lock().await;
                while let Ok(command) = receiver.try_recv() { queued.push(command); }
            }
            if critical_health {
                if let Some(target) = &state.follow_target { *target.lock().await = None; }
                if let Some(following) = &state.is_following { following.store(false, Ordering::Relaxed); }
                // A low-health reflex has priority over stale movement/combat/build commands.
                queued.retain(|command| matches!(command, BotCommand::Stop | BotCommand::Say(_)));
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
                        .map(|target| {
                            // Stand one block away on the side facing our current position.
                            // A fixed +X offset makes every follower crowd the same side.
                            let own = bot.position();
                            let dx = own.x - target.x;
                            let dz = own.z - target.z;
                            let (offset_x, offset_z) = if dx.abs() >= dz.abs() {
                                (if dx >= 0.0 { 1 } else { -1 }, 0)
                            } else {
                                (0, if dz >= 0.0 { 1 } else { -1 })
                            };
                            BlockPos::new(
                                target.x.floor() as i32 + offset_x,
                                target.y.floor() as i32,
                                target.z.floor() as i32 + offset_z,
                            )
                        });
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
                            if let HitResult::Entity(hit) = bot.hit_result() {
                                let target = bot.entity_ref_for(hit.entity);
                                let target_position = target.position();
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
                            } else {
                                bot.chat("No visible entity is in my crosshair.");
                            }
                        }
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
                        match bot.hit_result() {
                            HitResult::Block(hit) if !hit.miss && hit.block_pos == position => bot.block_interact(position),
                            _ => bot.chat("I can only place against a block that is currently in my crosshair."),
                        }
                    }
                    BotCommand::Jump => bot.jump(),
                    BotCommand::UseItem => bot.start_use_item(),
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
                    if last.elapsed() >= Duration::from_secs(12) {
                        *last = Instant::now();
                        true
                    } else { false }
                } else { false };

                if allowed {
                    if let Some(brain) = &state.brain {
                        let brain = brain.clone();
                        let autonomous = brain.is_autonomous();
                        let bot = bot.clone();
                        let status = state.status.clone();
                        let world_context = if let Some(status) = &status {
                            status.world.lock().await.clone().map(|world| format!(
                                " Current game state: position {:?}, health {}/20, food {}/20, saturation {}, inventory {:?}.",
                                world.position, world.health, world.food, world.saturation, world.inventory
                            )).unwrap_or_default()
                        } else {
                            String::new()
                        };
                        let prompt = format!(
                            "A Minecraft player addressed you in server chat. Respond as a concise teammate. Do not claim to have performed any in-game action.{world_context} Message: {content}"
                        );
                        // Provider work runs outside the tick handler so it cannot stall
                        // movement, keep-alives, or the Minecraft event loop.
                        tokio::task::spawn_local(async move {
                            if autonomous {
                                let action_request = format!(
                                    "Player request: {content}\nCurrent Minecraft state:{world_context}"
                                );
                                match brain.plan(&action_request).await {
                                    Ok(action) if brain.authorize_action(&action, false).is_ok() => {
                                        match crate::api::command_from_action(action.clone()) {
                                            Ok(command) => {
                                                let sender = if let Some(status) = &status {
                                                    status.command_tx.lock().await.clone()
                                                } else {
                                                    None
                                                };
                                                if let Some(sender) = sender {
                                                    match sender.try_send(command) {
                                                        Ok(()) => {
                                                            if bot.exists() && !action.action.eq_ignore_ascii_case("say") {
                                                                bot.chat("On it.");
                                                            }
                                                            return;
                                                        }
                                                        Err(error) => tracing::warn!(%error, "autonomous chat action could not enter the Minecraft command queue"),
                                                    }
                                                }
                                            }
                                            Err((_, error)) => tracing::debug!(%error, "autonomous chat action has no safe executor"),
                                        }
                                    }
                                    Ok(action) => tracing::info!(action = %action.action, "autonomous chat action requires approval or failed policy checks"),
                                    Err(error) => tracing::debug!(%error, "autonomous chat action planning failed; falling back to a conversational reply"),
                                }
                            }

                            match brain.reply(&prompt).await {
                                Ok(reply) => {
                                    let safe_reply: String = reply.chars().take(240).collect();
                                    if bot.exists()
                                        && !safe_reply.trim().is_empty()
                                        && !safe_reply.trim_start().starts_with('/')
                                        && !safe_reply.chars().any(|c| c.is_control()) {
                                        bot.chat(safe_reply);
                                    }
                                }
                                Err(error) => {
                                    tracing::error!(%error, "AI chat response failed");
                                    if bot.exists() {
                                        bot.chat("I couldn't reach my AI provider just now.");
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

/// Return the best recognized, ordinary food item in the player's hotbar.
/// Debug formatting is used only to identify the registry item name; unknown items fail closed.
fn best_safe_food_slot(menu: &azalea::inventory::Menu) -> Option<(usize, u8)> {
    // Do not attempt to eat while a chest, furnace, crafting table, or other
    // server container is open; right-click would interact with that screen.
    menu.try_as_player()?;
    menu.hotbar_slots_range().filter_map(|slot| {
        let item = menu.slots().get(slot)?;
        if item.is_empty() { return None; }
        let name = format!("{item:?}").to_ascii_lowercase();
        Some((slot, safe_food_priority(&name)?))
    }).max_by_key(|(_, priority)| *priority)
}

fn safe_food_priority(name: &str) -> Option<u8> {
    // Debug output may render registry names as either snake_case or CamelCase.
    // Compare an alphanumeric-only lowercase form so both representations work.
    let compact: String = name.chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect();
    // Some edible-looking items are harmful or intentionally excluded from
    // autonomous consumption. Check denials before the positive substring list.
    if ["poisonouspotato", "rottenflesh", "spidereye", "pufferfish"]
        .iter()
        .any(|needle| compact.contains(needle))
    {
        return None;
    }
    [
        ("goldencarrot", 10), ("cookedbeef", 10), ("cookedporkchop", 10),
        ("goldenapple", 10), ("cookedmutton", 8), ("cookedrabbit", 8),
        ("cookedchicken", 8), ("cookedcod", 8), ("cookedsalmon", 8),
        ("bread", 7), ("pumpkinpie", 7), ("bakedpotato", 7),
        ("carrot", 4), ("potato", 4), ("apple", 3),
        ("melonslice", 3), ("sweetberries", 3), ("glowberries", 3),
    ].iter().find_map(|(needle, score)| compact.contains(needle).then_some(*score))
}

#[cfg(test)]
mod food_tests {
    use super::safe_food_priority;

    #[test]
    fn safe_food_classifier_prefers_nutritious_food_and_fails_closed() {
        assert!(safe_food_priority("itemstack { kind: cooked_beef }").unwrap()
            > safe_food_priority("itemstack { kind: bread }").unwrap());
        assert_eq!(safe_food_priority("itemstack { kind: potion }"), None);
        assert_eq!(safe_food_priority("itemstack { kind: rotten_flesh }"), None);
        assert_eq!(safe_food_priority("itemstack { kind: poisonous_potato }"), None);
        assert_eq!(safe_food_priority("itemstack { kind: spider_eye }"), None);
        assert_eq!(safe_food_priority("itemstack { kind: pufferfish }"), None);
        assert_eq!(safe_food_priority("itemstack { kind: golden_apple }"), Some(10));
        assert_eq!(safe_food_priority("ItemStack { kind: CookedBeef }"), Some(10));
        assert_eq!(safe_food_priority("ItemStack { kind: BakedPotato }"), Some(7));
    }
}
