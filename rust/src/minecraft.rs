use crate::{api::RuntimeStatus, brain::Brain};
use anyhow::Result;
use azalea::{
    BlockPos,
    container::ContainerHandle,
    pathfinder::goals::{BlockPosGoal, RadiusGoal},
    prelude::*,
    swarm::{Swarm, SwarmBuilder, SwarmEvent},
};
use azalea::{
    entity::{indexing::EntityUuidIndex, LookDirection, Position},
    interact::{CurrentSequenceNumber, HitResultComponent},
    inventory::SetSelectedHotbarSlotEvent,
    movement::{StartWalkEvent, WalkDirection},
    world::MinecraftEntityId,
    protocol::packets::game::{
        serverbound_interact_packet::InteractionHand,
        serverbound_player_action_packet::{Action as PlayerAction, ServerboundPlayerActionPacket},
        serverbound_player_command_packet::{Action as PlayerCommandAction, ServerboundPlayerCommandPacket},
        serverbound_use_item_packet::ServerboundUseItemPacket,
    },
    direction::Direction,
};
use azalea_inventory::{operations::{PickupClick, QuickMoveClick}, Menu};
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
    Sneak(bool),
    UseItem,
    Craft(String),
    Deposit(i32, i32, i32),
    Withdraw(i32, i32, i32, String),
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
    mode: String,
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
    mode: String,
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
            mode: self.mode.clone(),
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
    mode: String,
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
            mode: mode.clone(),
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
        let _reconnected = swarm
            .add_and_retry_forever_with_opts(&account, state.bot_state(is_primary), &join_opts)
            .await;
        tracing::info!(username = %account.username, "Minecraft agent reconnected");
    }
    Ok(())
}

async fn handle(mut bot: Client, event: Event, state: BotState) -> anyhow::Result<()> {
    match event {
        Event::Login => {
            if let Some(connected) = &state.connected {
                if !connected.swap(true, Ordering::Relaxed) {
                    if let Some(status) = &state.status {
                        status.connected_agents.fetch_add(1, Ordering::Relaxed);
                        if state.is_primary {
                            status.minecraft_connected.store(true, Ordering::Relaxed);
                        }
                    }
                }
            }
            tracing::info!(username = %bot.username(), "Minecraft agent logged in");
        }
        Event::Disconnect(reason) => {
            if let Some(connected) = &state.connected {
                if connected.swap(false, Ordering::Relaxed) {
                    if let Some(status) = &state.status {
                        status.connected_agents.fetch_sub(1, Ordering::Relaxed);
                        if state.is_primary {
                            status.minecraft_connected.store(false, Ordering::Relaxed);
                            *status.world.lock().await = None;
                        }
                    }
                }
            }
            if state.is_primary {
                if let Some(pending) = &state.pending_block_action {
                    *pending.lock().await = None;
                }
                if let Some(receiver) = &state.commands {
                    let mut receiver = receiver.lock().await;
                    while receiver.try_recv().is_ok() {}
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
                            stop_walking(&bot);
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
                                    tokio::spawn(async move {
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
                stop_walking(&bot);
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
                        stop_walking(&bot);
                    }
                }
            } else if let Some(following) = &state.is_following {
                if following.swap(false, Ordering::Relaxed) {
                    bot.stop_pathfinding();
                    stop_walking(&bot);
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
                        stop_walking(&bot);
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
                        stop_walking(&bot);
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
                        stop_walking(&bot);
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
                    BotCommand::Sneak(enabled) => {
                        let action = if enabled {
                            PlayerCommandAction::PressShiftKey
                        } else {
                            PlayerCommandAction::ReleaseShiftKey
                        };
                        let entity_id = *bot.component::<MinecraftEntityId>();
                        bot.write_packet(ServerboundPlayerCommandPacket {
                            id: entity_id,
                            action,
                            data: 0,
                        }.get())?;
                    }
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
                    BotCommand::Craft(recipe) => {
                        let mut crafting_bot = bot.clone();
                        tokio::spawn(async move {
                            match craft_basic(&mut crafting_bot, &recipe).await {
                                Ok(()) => crafting_bot.chat("Crafting completed."),
                                Err(error) => {
                                    tracing::warn!(%error, %recipe, "basic crafting failed");
                                    crafting_bot.chat("I couldn't complete that recipe. Check that the required materials and inventory space are available.");
                                }
                            }
                        });
                    }
                    BotCommand::Deposit(x, y, z) => {
                        let mut storage_bot = bot.clone();
                        tokio::spawn(async move {
                            let position = BlockPos::new(x, y, z);
                            match deposit_inventory(&mut storage_bot, position).await {
                                Ok(count) => storage_bot.chat(&format!("Stored {count} inventory stacks in the chest.")),
                                Err(error) => {
                                    tracing::warn!(%error, ?position, "inventory deposit failed");
                                    storage_bot.chat("I couldn't safely deposit items into that container.");
                                }
                            }
                        });
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
                        tokio::spawn(async move {
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



async fn open_target_container(bot: &mut Client, position: BlockPos) -> Result<ContainerHandle> {
    let mut ticks = bot.get_tick_broadcaster();
    bot.look_at(position.center());
    wait_ticks(&mut ticks, 4).await?;

    let hit = bot.get_component::<HitResultComponent>()
        .ok_or_else(|| anyhow::anyhow!("crosshair target is unavailable"))?;
    if hit.miss || hit.block_pos != position {
        anyhow::bail!("the requested container is not under the crosshair");
    }

    match tokio::time::timeout(Duration::from_secs(8), bot.open_container_at(position)).await {
        Ok(Some(handle)) => Ok(handle),
        Ok(None) => anyhow::bail!("no openable container exists at the requested position"),
        Err(_) => {
            bot.ecs.lock().entity_mut(bot.entity)
                .remove::<azalea::container::WaitingForInventoryOpen>();
            anyhow::bail!("container did not open before the timeout");
        }
    }
}

async fn deposit_inventory(bot: &mut Client, position: BlockPos) -> Result<u32> {
    let handle = open_target_container(bot, position).await?;
    let mut moved_items = 0u32;

    for _ in 0..36 {
        let menu = handle.menu()
            .ok_or_else(|| anyhow::anyhow!("container closed during inventory transfer"))?;
        let player_slots = menu.player_slots_range();
        let hotbar_slots = menu.hotbar_slots_range();
        let source = player_slots.find(|slot| {
            !hotbar_slots.contains(slot)
                && menu.slots().get(*slot).is_some_and(|item| !item.is_empty())
        });
        let Some(source) = source else {
            break;
        };
        let before = inventory_slot_count(&menu, source as u16);
        drop(menu);

        click_quick_move(bot, &handle, source as u16).await?;

        let updated = handle.menu()
            .ok_or_else(|| anyhow::anyhow!("container closed during inventory transfer"))?;
        let after = inventory_slot_count(&updated, source as u16);
        if after < before {
            moved_items = moved_items.saturating_add(before - after);
        } else {
            break;
        }
    }

    if moved_items == 0 {
        anyhow::bail!("no main-inventory items could be moved; check container space and inventory contents");
    }
    Ok(moved_items)
}

fn normalize_item_name(debug_name: &str) -> String {
    let mut normalized = String::with_capacity(debug_name.len() + 8);
    for (index, character) in debug_name.chars().enumerate() {
        if character.is_ascii_uppercase() && index > 0 {
            normalized.push('_');
        }
        normalized.push(character.to_ascii_lowercase());
    }
    normalized
}

fn count_container_item(slots: &[azalea_inventory::ItemSlot], item_name: &str) -> u32 {
    slots.iter()
        .filter_map(|slot| slot.as_present())
        .filter(|item| normalize_item_name(&format!("{:?}", item.kind)) == item_name)
        .map(|item| item.count.max(0) as u32)
        .sum()
}

async fn withdraw_from_container(
    bot: &mut Client,
    position: BlockPos,
    item_name: &str,
) -> Result<u32> {
    let item_name = item_name.strip_prefix("minecraft:").unwrap_or(item_name);
    let handle = open_target_container(bot, position).await?;
    let mut moved_items = 0u32;

    for _ in 0..54 {
        let contents = handle.contents()
            .ok_or_else(|| anyhow::anyhow!("container closed during withdrawal"))?;
        let before_total = count_container_item(&contents, item_name);
        if before_total == 0 {
            break;
        }
        let source_slot = contents.iter().position(|slot| {
            slot.as_present().is_some_and(|item| {
                normalize_item_name(&format!("{:?}", item.kind)) == item_name
            })
        });
        let Some(source_slot) = source_slot else {
            break;
        };

        let mut ticks = bot.get_tick_broadcaster();
        handle.click(QuickMoveClick::Left { slot: source_slot as u16 });
        wait_ticks(&mut ticks, 4).await?;

        let updated = handle.contents()
            .ok_or_else(|| anyhow::anyhow!("container closed during withdrawal"))?;
        let after_total = count_container_item(&updated, item_name);
        if after_total >= before_total {
            break;
        }
        moved_items = moved_items.saturating_add(before_total - after_total);
    }

    if moved_items == 0 {
        anyhow::bail!("no matching items could be moved; check the item name and available inventory space");
    }
    Ok(moved_items)
}

async fn wait_ticks(
    ticks: &mut tokio::sync::broadcast::Receiver<()>,
    count: usize,
) -> Result<()> {
    for _ in 0..count {
        tokio::time::timeout(Duration::from_secs(2), ticks.recv()).await??;
    }
    Ok(())
}

async fn click_pickup(
    bot: &Client,
    handle: &ContainerHandle,
    slot: u16,
    right_click: bool,
) -> Result<()> {
    let mut ticks = bot.get_tick_broadcaster();
    let click = if right_click {
        PickupClick::Right { slot: Some(slot) }
    } else {
        PickupClick::Left { slot: Some(slot) }
    };
    handle.click(click);
    wait_ticks(&mut ticks, 3).await
}

async fn click_quick_move(
    bot: &Client,
    handle: &ContainerHandle,
    slot: u16,
) -> Result<()> {
    let mut ticks = bot.get_tick_broadcaster();
    handle.click(QuickMoveClick::Left { slot });
    wait_ticks(&mut ticks, 4).await
}

fn find_inventory_slot(menu: &Menu, predicate: impl Fn(&str) -> bool) -> Option<u16> {
    menu.player_slots_range().find_map(|index| {
        let item = menu.slots().get(index)?.as_present()?;
        let name = format!("{:?}", item.kind);
        predicate(&name).then_some(index as u16)
    })
}

fn inventory_slot_count(menu: &Menu, slot: u16) -> u32 {
    menu.slots()
        .get(slot as usize)
        .and_then(|item| item.as_present())
        .map(|item| item.count.max(0) as u32)
        .unwrap_or(0)
}

async fn place_one_in_crafting_grid(
    bot: &Client,
    handle: &ContainerHandle,
    source_slot: u16,
    grid_slot: u16,
) -> Result<()> {
    click_pickup(bot, handle, source_slot, false).await?;
    click_pickup(bot, handle, grid_slot, true).await?;
    click_pickup(bot, handle, source_slot, false).await?;
    Ok(())
}

async fn craft_basic(bot: &mut Client, recipe: &str) -> Result<()> {
    let (source_predicate, grid_slots): (Box<dyn Fn(&str) -> bool + Send + Sync>, Vec<u16>) =
        match recipe {
            "planks" => (
                Box::new(|name| name.ends_with("Log") || name.ends_with("Stem") || name == "BambooBlock"),
                vec![1],
            ),
            "sticks" => (Box::new(|name| name.ends_with("Planks")), vec![1, 3]),
            "crafting_table" => (Box::new(|name| name.ends_with("Planks")), vec![1, 2, 3, 4]),
            _ => anyhow::bail!("unsupported basic recipe '{recipe}'"),
        };

    let handle = bot.open_inventory()
        .ok_or_else(|| anyhow::anyhow!("close the currently open container before crafting"))?;
    let initial_menu = handle.menu()
        .ok_or_else(|| anyhow::anyhow!("player inventory is not available"))?;
    if (0..=4).any(|slot| initial_menu.slots().get(slot).is_some_and(|item| !item.is_empty())) {
        anyhow::bail!("clear the player crafting grid before starting another recipe");
    }
    drop(initial_menu);

    for &grid_slot in &grid_slots {
        let menu = handle.menu()
            .ok_or_else(|| anyhow::anyhow!("player inventory closed during crafting"))?;
        let source_slot = find_inventory_slot(&menu, |name| source_predicate(name))
            .ok_or_else(|| anyhow::anyhow!("not enough matching ingredients for '{recipe}'"))?;
        drop(menu);
        place_one_in_crafting_grid(bot, &handle, source_slot, grid_slot).await?;
    }

    let mut ticks = bot.get_tick_broadcaster();
    wait_ticks(&mut ticks, 4).await?;
    let menu = handle.menu()
        .ok_or_else(|| anyhow::anyhow!("player inventory closed during crafting"))?;
    if menu.slots().first().is_none_or(|item| item.is_empty()) {
        drop(menu);
        for grid_slot in grid_slots {
            click_quick_move(bot, &handle, grid_slot).await?;
        }
        anyhow::bail!("the 2x2 crafting grid did not produce the requested recipe");
    }
    drop(menu);

    click_quick_move(bot, &handle, 0).await?;
    let menu = handle.menu()
        .ok_or_else(|| anyhow::anyhow!("player inventory closed before collecting the result"))?;
    if menu.slots().first().is_some_and(|item| !item.is_empty()) {
        anyhow::bail!("inventory may be full; crafting result remains in the output slot");
    }
    Ok(())
}

fn stop_walking(bot: &Client) {
    bot.ecs.lock().send_event(StartWalkEvent {
        entity: bot.entity,
        direction: WalkDirection::None,
    });
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


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn item_debug_names_normalize_to_minecraft_ids() {
        assert_eq!(normalize_item_name("OakLog"), "oak_log");
        assert_eq!(normalize_item_name("CrimsonStem"), "crimson_stem");
        assert_eq!(normalize_item_name("CraftingTable"), "crafting_table");
    }
}
