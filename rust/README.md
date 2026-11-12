# Mindcraft Native (Rust)

## macOS desktop application

A Tauri-based native macOS app now provides a first-run setup screen, persists non-secret settings in the app data directory, stores the AI provider key in macOS Keychain, starts/stops the bundled Rust runtime, and displays live connection telemetry plus teammate chat. The app is designed so users do not need Node.js or a Rust toolchain installed.

Build locally on a Mac with `bash build-macos.sh` from the `rust/` directory. This builds the Rust runtime, stages it inside the app bundle, and produces an `.app` and `.dmg`. The packaging flow is not yet verified; see `desktop/README.md` for prerequisites and current limitations.


This is a Rust-native runtime being developed alongside the original JavaScript/Mineflayer project. Its target is **Minecraft Java Edition 26.1**, pinned to **Azalea 0.16.0+mc26.1**. The native runtime does not use Mineflayer or a JavaScript gameplay bridge.

## Current implementation

- Shared Tokio process for the CLI, HTTP API, dashboard, persistence, AI provider client, and optional Minecraft connection.
- Axum dashboard/API bound to loopback by default. API routes require a bearer token; a one-time token is generated and printed when no token is configured.
- Provider adapters for OpenAI-compatible endpoints (including Ollama), Anthropic Messages, and Google Gemini.
- SQLite conversation and strategic-goal persistence using WAL mode; dashboard chat and addressed in-game teammate conversations share the same bounded history.
- Specialist-role task decomposition (planner, scout, gatherer, builder, crafter, guardian), durable priority queue, atomic task claims with two-minute renewable leases, abandoned-task recovery, and completion tracking shared by workers using the same database. Workers renew with `POST /api/tasks/{id}/heartbeat`. In the dashboard and macOS desktop app, a claimed task can be sent to the action planner; the UI renews the task lease while it remains selected, remembers the claiming agent ID, and requires confirmation before marking the task complete. Review and approve the proposed game action as required by the autonomy policy, then mark the task complete only after verifying the in-game result. Azalea can run a multi-client offline-mode swarm with a staggered join delay; Microsoft auth currently supports one account.
- Deterministic reflex safety layer stops movement and warns when health reaches 6/20. In Autonomous mode, a separate bounded survival reflex may eat a recognized ordinary food item already in the hotbar when hunger is 12/20 or lower; it never consumes arbitrary held items, potions, harmful/unknown food, or items from the main inventory. Low-food warnings remain available in other modes.
- Tactical single-action planner and strategic goal decomposition are separate provider-backed paths.
- Explicit Assisted / Balanced / Autonomous policy gate. The dashboard's Approve & Execute action is the explicit approval path; Autonomous automatically dispatches supported plans without a click. Risk floors are derived from the executor (the model cannot downgrade mining, placement, attack, or generic item use); Assisted requires explicit approval for every action; Balanced requires approval for high-risk actions, and attack/generic item use always require it. Drop remains blocked; attack is restricted to a visible crosshair target within 3.5 blocks and always requires explicit approval.
- Azalea multi-client swarm with offline or single-account Microsoft device-code authentication, per-agent spawn/disconnect accounting, live per-agent position/health/hunger/inventory telemetry, and AI chat replies when players mention `Mindcraft`. The desktop app supplies an app-scoped Azalea auth-cache path so Microsoft sign-in data stays with Mindcraft's application data.
- Recipe crafting uses Azalea's inventory click protocol for planks, sticks, torches, crafting tables, wooden/stone/iron/diamond tools, doors, trapdoors, slabs, stairs, fences, signs, boats, bowls, furnaces, chests, bread, and ladders. Two-by-two recipes use the player inventory; three-by-three recipes require a crafting-table coordinate. The bot pathfinds to a table without breaking blocks, requires line of sight, returns only ingredients it placed when setup fails, and verifies the expected output before collecting it. Crafting remains policy-gated.
- Exploration uses bounded 8-direction waypoints around the bot's current position (8-128 block radius); container transfers pathfind to a loaded target without mining, require direct line of sight, and verify that the selected slot changed after a shift-click. Both are policy-gated, and container transfers always require explicit approval.
- Targeted multi-client command routing: each offline-mode bot has its own broadcast subscription, and commands can be routed to one configured username from the dashboard or desktop app; untargeted commands remain primary-only. Microsoft single-account mode remains primary-only.
- A bounded in-process broadcast command channel. `say`, `stop`, `jump`, `follow <player>`, `look x,y,z`, `goto x,y,z`, `explore [radius]`, `mine x,y,z` (pathfinds without mining en route, requires a loaded non-air target and direct raycast, selects the fastest suitable hotbar tool, then confirms the block became air; cancellation and timeout abort active mining), `place x,y,z` (pathfinds to a solid support block below the target, verifies the top-face hit, then confirms placement), `container_transfer x,y,z,slot` (pathfinds without mining, verifies line of sight, and checks the slot after shift-click), `craft recipe[@x,y,z]` (allowlisted recipe crafting with output verification), `attack crosshair` (visible entity within 3.5 blocks; always requires explicit approval), `use_item` (always requires explicit approval), and `equip 0-8` are dispatched on Minecraft ticks. Assisted/Balanced dispatch requires user approval; Autonomous mode can queue supported policy-approved plans from the dashboard or addressed Minecraft chat without a click; attack, generic item use, and container transfers still require explicit approval. Autonomous mode does not yet invent or pursue long-term goals on its own. Coordinates are range-checked; unimplemented actions fail closed.
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
| `POST /api/plan` | Generate and policy-check a structured action proposal |
| `POST /api/execute` | Explicitly approve and queue a supported game action |
| `GET/POST /api/goals` | List goals or decompose a new strategic goal into specialist tasks |
| `GET /api/goals/{id}/tasks` | List a goal's tasks |
| `POST /api/tasks/claim` | Atomically claim the highest-priority pending task for a role |
| `POST /api/tasks/{id}/heartbeat` | Renew a two-minute task lease |
| `POST /api/tasks/{id}/complete` | Complete a task owned by the requesting agent |

Worker roles are `planner`, `scout`, `gatherer`, `builder`, `crafter`, and `guardian`. Claims include an agent ID so multiple workers can share the same SQLite database without taking the same live task.

## Important scope boundary

The current branch contains a first-pass command executor implemented against the Azalea 0.16.0 API for chat, stop, follow, look, pathfinding, mining, block interaction, attacking, and hotbar selection. These code paths are present, but none should be represented as verified working until the native project compiles and each action is exercised against a live Minecraft 26.1 server. Connection and spawn telemetry are implemented separately from gameplay verification.

The durable specialist task queue supports role-based claims, two-minute leases, heartbeats, and completion, but task claims are not yet automatically tied to in-game execution. The dashboard and macOS desktop app renew a claimed task lease while it remains selected; the desktop app sends a heartbeat every 45 seconds and binds completion to the original claiming agent ID. This prevents an otherwise active worker from losing its lease merely because it is working longer than two minutes. The autonomous food reflex is deliberately limited to recognized hotbar food; it is not a general inventory manager or healing system. Remaining work includes broader recipe coverage and smelting/furnace automation, full inventory/container management (the current executor only shift-clicks a selected slot in a nearby container), healing beyond the current food-only reflex, advanced multi-step construction, goal-driven exploration (the current executor is a waypoint patrol), automated execution and outcome verification for strategic tasks, automatic per-agent role assignment, and full remote API hardening (TLS termination, rate limits, and per-agent authorization). Do not treat this branch as gameplay-ready until a local build and live 26.1 server verification succeed.
