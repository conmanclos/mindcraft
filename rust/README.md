# Mindcraft Native (Rust)

## macOS desktop application

A Tauri-based native macOS app now provides a first-run setup screen, persists non-secret settings in the app data directory, stores the AI provider key in the operating system credential store, starts/stops the bundled Rust runtime, and displays live connection telemetry plus teammate chat. The app is designed so users do not need Node.js or a Rust toolchain installed.

Build on the target operating system with `bash build-desktop.sh` on macOS/Linux or `./build-desktop.ps1` in PowerShell on Windows. These scripts build and stage the matching Rust runtime binary before Tauri packaging; they do not run tests. The macOS-specific `build-macos.sh` remains available for `.app`/`.dmg` packaging. See `desktop/README.md` for platform prerequisites and current limitations.


This is a Rust-native runtime being developed alongside the original JavaScript/Mineflayer project. Its target is **Minecraft Java Edition 26.1**, pinned to **Azalea 0.16.0+mc26.1**. The native runtime does not use Mineflayer or a JavaScript gameplay bridge.

## Current implementation

- Shared Tokio process for the CLI, HTTP API, dashboard, persistence, AI provider client, and optional Minecraft connection.
- Axum dashboard/API bound to loopback by default. API routes require a bearer token; a one-time token is generated and printed when no token is configured. A per-credential fixed-window limiter caps requests at 180 per 60 seconds and bounds its in-memory credential buckets.
- Provider adapters for OpenAI-compatible endpoints (including Ollama), Anthropic Messages, and Google Gemini.
- SQLite conversation and strategic-goal persistence using WAL mode; dashboard chat and addressed in-game teammate conversations share the same bounded history.
- Layered persistent memory in SQLite for semantic knowledge, episodic events, player preferences, and world facts. Records are keyed by memory type and subject, carry confidence/importance/source metadata, update in place instead of duplicating, and are available to both chat and specialist planning. The browser dashboard and macOS desktop app can create, review, and delete memories through authenticated API routes.
- Specialist-role task decomposition (planner, scout, gatherer, builder, crafter, guardian), durable priority queue, atomic goal-and-task plan creation, atomic task claims with two-minute renewable leases, abandoned-task recovery, and completion tracking shared by workers using the same database. Workers renew with `POST /api/tasks/{id}/heartbeat`. In Autonomous mode, each connected bot prioritizes tasks matching its assigned role, then falls back to other specialist queues when its own queue is empty so solo and smaller teams can still progress. It plans one supported action, dispatches it to that exact bot, and moves the task to human result verification; tasks whose action requires explicit approval are released back to the queue. Assisted and Balanced modes retain manual task claiming and approval. In the browser dashboard and desktop app, a claimed task can be sent to the action planner; the UI renews the task lease while it remains selected, remembers the claiming agent ID, and requires confirmation before marking the task complete. Review and approve the proposed game action as required by the autonomy policy, then mark the task complete only after verifying the in-game result. Azalea can run a multi-client offline-mode swarm with a staggered join delay; Microsoft auth currently supports one account.
- Deterministic reflex safety layer cancels active mining, stops pathfinding/movement, and warns when health reaches 6/20. In Autonomous mode, bounded survival reflexes may equip recognized armor into empty armor slots (never replacing worn gear), eat recognized hotbar food at hunger 12/20 or lower, prioritize a hotbar golden apple for regeneration when health is 10/20 or lower, and defend against visible whitelisted hostile mobs within 4 blocks. Defensive combat never targets players, passive mobs, or neutral mobs and is disabled at critical health. The reflexes never consume arbitrary held items, potions, harmful/unknown food, or items from the main inventory. Low-food and critical-health warnings remain available in other modes.
- Tactical single-action planner and strategic goal decomposition are separate provider-backed paths.
- Explicit Assisted / Balanced / Autonomous policy gate. The dashboard's Approve & Execute action is the explicit approval path; Autonomous automatically dispatches supported plans without a click. Risk floors are derived from the executor (the model cannot downgrade mining, placement, attack, or generic item use); Assisted requires explicit approval for every action; Balanced requires approval for high-risk actions, and attack/generic item use always require it. Drop remains blocked; attack is restricted to a visible crosshair target within 3.5 blocks and always requires explicit approval.
- Azalea multi-client swarm with offline or single-account Microsoft device-code authentication, per-agent spawn/disconnect accounting, live per-agent position/health/hunger/inventory telemetry, and AI chat replies when players mention `Mindcraft`. In Autonomous mode, server-chat actions are disabled unless the sender is explicitly listed in `MINDCRAFT_TRUSTED_PLAYERS`; chat from other players remains conversational and cannot trigger gameplay actions. The allowlist is limited to 32 valid Minecraft usernames and defaults to empty. The desktop setup exposes the same allowlist. The app supplies an app-scoped Azalea auth-cache path so Microsoft sign-in data stays with Mindcraft's application data.
- Recipe crafting uses Azalea's inventory click protocol for planks, sticks, torches, crafting tables, wooden/stone/iron/golden/diamond tools, leather/iron/golden/diamond armor, bows, fishing rods, arrows, paper, glass panes, shears, buckets, doors, trapdoors, slabs, stairs, fences, signs, boats, bowls, furnaces, chests, bread, and ladders. Two-by-two recipes use the player inventory; three-by-three recipes require a crafting-table coordinate. The bot pathfinds to a table without breaking blocks, requires line of sight, returns only ingredients it placed when setup fails, and verifies the expected output before collecting it. Crafting remains policy-gated.
- Allowlisted batch smelting uses real furnace, blast-furnace, or smoker menus. `smelt input[*count]@x,y,z` handles 1-64 recognized inputs, uses existing fuel or calculates the required amount from one inventory stack (coal/charcoal: 8 items each, blaze rods: 12, lava buckets: 100), waits for the server-produced full output batch, and collects only after the expected count is confirmed. Mixing into an occupied fuel slot and unsupported recipes are refused; combining multiple fuel stacks and automatic follow-up collection remain future work.
- Exploration uses a bounded golden-angle spiral around the bot's current position (8-128 block radius), continues rotating around the radius boundary instead of repeating a fixed compass loop, skips unreachable waypoints after 60 seconds, and does not mine while navigating; container transfers pathfind to a loaded target without mining, require direct line of sight, and verify that the selected slot changed after a shift-click. Both are policy-gated, and container transfers always require explicit approval.
- Targeted multi-client command routing: each offline-mode bot has its own broadcast subscription, receives a stable specialist role (planner, scout, gatherer, builder, crafter, guardian, then repeating for larger teams), and exposes that role in live telemetry and the dashboard/desktop roster. Commands can be routed to one configured username; untargeted commands remain primary-only. Microsoft single-account mode remains primary-only.
- A bounded in-process broadcast command channel. `say`, `stop`, `jump`, `follow <player>` (recalculates routes that remain stuck for 60 seconds), `look x,y,z`, `goto x,y,z`, `explore [radius]`, `mine x,y,z` (pathfinds without mining en route, requires a loaded non-air target and direct raycast, selects the fastest suitable hotbar tool, confirms the block became air, then safely approaches the drop location; cancellation and timeout abort active mining), `place x,y,z` (pathfinds to a reachable adjacent full-collision support block, verifies the exact top/bottom/side face raycast, then confirms placement; partial collision shapes are not supported), `build [item@]x,y,z;x,y,z;...` (places 1-64 positions sequentially, sorted bottom-up; optionally selects an exact item from the hotbar or moves it from main inventory into an empty hotbar slot without overwriting items; supports reusable `blueprint:platform_5x5@x,y,z`, `blueprint:wall_7x3@x,y,z`, and `blueprint:starter_hut@x,y,z` templates, optionally prefixed with `item@`; every target must be air with a reachable adjacent full-collision support face; stops on the first unconfirmed placement), `container_transfer x,y,z,slot` (pathfinds without mining, verifies line of sight, and checks the slot after shift-click), `craft recipe[@x,y,z]` (allowlisted recipe crafting with output verification), `smelt input[*count]@x,y,z` (1-64 allowlisted inputs in a nearby furnace, blast furnace, or smoker; count defaults to one; fuel is selected by capacity from one inventory stack—coal/charcoal 8 items, blaze rods 12, lava buckets 100—and the executor refuses occupied fuel slots and verifies the full output count), `attack crosshair` (manual visible-entity attack within 3.5 blocks; always requires explicit approval; Autonomous mode also has a separate narrow defensive reflex against whitelisted hostile mobs within 4 blocks), `use_item` (always requires explicit approval), and `equip 0-8` are dispatched on Minecraft ticks. Assisted/Balanced dispatch requires user approval; Autonomous mode can queue supported policy-approved plans from the dashboard or from allowlisted Minecraft chat senders without a click; attack, generic item use, and container transfers still require explicit approval. When Autonomous mode starts, a background planner creates one durable baseline survival objective (safe survival, sustainable food/basic resources, and a compact shelter near spawn). If provider planning is unavailable, it retries every 60 seconds; the exact goal is persisted once and is not duplicated on reconnect. Coordinates are range-checked; unimplemented actions fail closed.
- No CI or GitHub Actions workflows are added.

## Minecraft 26.1 migration

Azalea 0.16.0 is the version-specific dependency for Minecraft 26.1. This is a major protocol and API jump from the former 1.21.1 implementation: Azalea's changelog records breaking API changes including `Instance` to `World`, client/entity reference changes, and an ECS lock change. The gameplay integration has been updated to the 0.16 API, including the new Microsoft auth options and connection-failure event. However, the code has not yet passed a native compile, and it must not be considered buildable until `cargo test` and `cargo build --release` succeed on the target toolchain. Do not downgrade the target or silently substitute another protocol version to avoid migration work.

## Build

Install Rust nightly (required by the Azalea crate's toolchain) and a C toolchain required by bundled SQLite. The checked-in `rust-toolchain.toml` selects nightly automatically:

```sh
cd rust
cp .env.example .env
cargo build --release
```

A successful build has not yet been verified in this environment. Treat the runtime as in development until the Rust unit tests and release build pass, then the bot joins a live 26.1 server. For the complete Mac app bundle, use `bash build-macos.sh` as documented in `desktop/README.md`.

## Configure

Edit `.env`:

- `MINDCRAFT_MC_SERVER=127.0.0.1:25565`
- `MINDCRAFT_MC_USERNAME=Mindcraft` (fallback offline username)
- `MINDCRAFT_MC_ACCOUNTS=Mindcraft,Scout,Builder` for multiple offline-mode clients
- `MINDCRAFT_MC_AUTH=offline|microsoft`
- `MINDCRAFT_MICROSOFT_EMAIL=you@example.com` when using Microsoft device-code authentication
- `MINDCRAFT_TRUSTED_PLAYERS=YourName,FriendName` to allow only those server-chat users to trigger autonomous gameplay actions (empty by default)
- `MINDCRAFT_PROVIDER=ollama` (or `openai`, `openai-compatible`, `anthropic`, `google`)
- `MINDCRAFT_MODEL=llama3.1`
- `MINDCRAFT_BASE_URL=http://127.0.0.1:11434/v1` for Ollama
- `MINDCRAFT_API_KEY=...` for hosted providers
- `MINDCRAFT_MODE=assisted|balanced|autonomous`
- `MINDCRAFT_BIND=127.0.0.1:8787`

Secrets are read from environment variables and are not written to SQLite. Keep the bind address on loopback unless you set a strong `MINDCRAFT_API_TOKEN` and put the service behind a trusted TLS reverse proxy. Azalea supports offline account mode for local/offline-mode servers and Microsoft device-code authentication for online-mode servers. No Microsoft password is accepted or stored by this application.

## Run

```sh
cargo run --release -- serve
```

Open http://127.0.0.1:8787. Copy the one-time API token printed in the terminal into the dashboard. Or set `MINDCRAFT_API_TOKEN` yourself.

One-off provider check:

```sh
cargo run --release -- chat "Plan a safe first night in survival."
```

Safe config diagnostic:

```sh
cargo run --release -- doctor
```

## HTTP API

The dashboard is served from `/`. `GET /api/health` is public; all other API routes require `Authorization: Bearer <MINDCRAFT_API_TOKEN>`.

| Route | Purpose |
|---|---|
| `GET /api/status` | Connected swarm count, autonomy/provider state, position, health, inventory |
| `POST /api/chat` | Provider-backed chat with recent conversation and live bot context |
| `GET/POST /api/memory` | List or upsert typed persistent memories; memory records are included in AI context |
| `DELETE /api/memory/{id}` | Delete a persistent memory record |
| `POST /api/plan` | Generate and policy-check a structured action proposal |
| `POST /api/execute` | Explicitly approve and queue a supported game action |
| `GET/POST /api/goals` | List goals or decompose a new strategic goal into specialist tasks |
| `GET /api/goals/{id}/tasks` | List a goal's tasks |
| `POST /api/agents/{agent_id}/credential` | Issue/rotate a one-hour, in-memory agent token (master token required) |
| `DELETE /api/agents/{agent_id}/credential` | Revoke an agent token (master token required) |
| `POST /api/tasks/claim` | Atomically claim the highest-priority pending task for a role; accepts the master token or that agent's token |
| `POST /api/tasks/{id}/heartbeat` | Renew a two-minute task lease |
| `POST /api/tasks/{id}/release` | Release a task back to the pending queue when a worker cannot complete it |
| `POST /api/tasks/{id}/complete` | Complete a task owned by the requesting agent |

Worker roles are `planner`, `scout`, `gatherer`, `builder`, `crafter`, and `guardian`. Claims include an agent ID so multiple workers can share the same SQLite database without taking the same live task.

## Important scope boundary

The current branch contains a first-pass command executor implemented against the Azalea 0.16.0 API for chat, stop, follow, look, pathfinding, mining, block interaction, attacking, and hotbar selection. These code paths are present, but none should be represented as verified working until the native project compiles and each action is exercised against a live Minecraft 26.1 server. Connection and spawn telemetry are implemented separately from gameplay verification.

The durable specialist task queue supports role-based claims, two-minute leases, heartbeats, and completion. In Autonomous mode, connected bots claim a specialist task, plan one supported action, dispatch it to the assigned bot, and move the task into a human review state; the player must still verify the result before completion. The browser dashboard and desktop app renew a manually claimed task lease while it remains selected; the desktop app sends a heartbeat every 45 seconds and binds completion to the original claiming agent ID. This prevents an otherwise active worker from losing its lease merely because it is working longer than two minutes. The autonomous food reflex is deliberately limited to recognized hotbar food; it is not a general inventory manager or healing system. Remaining work includes broader recipe coverage, multi-stack smelting queues and fuel-time-aware sufficiency checks; full inventory/container management (the current executor transfers one selected slot per interaction); custom schematic parsing, mixed-material blueprints, and block-by-block material optimization (the current build executor supports explicit coordinates and three bounded single-material templates, sources an exact material stack from the main inventory into an empty hotbar slot, and places bottom-up using any adjacent full-collision support face); point-of-interest, biome, and structure-aware exploration (the current executor is a bounded golden-angle spiral patrol); multi-action task execution, automatic world-state outcome verification, and recovery/replanning after partial task completion; and TLS termination for remote deployments. Task claim, heartbeat, and completion already accept per-agent one-hour credentials or the master dashboard token, and the runtime enforces a per-credential 180-request/60-second limit. Do not treat this branch as gameplay-ready until a local build and live 26.1 server verification succeed.
