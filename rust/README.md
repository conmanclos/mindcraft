# Mindcraft Native (Rust)

This is a Rust-native runtime being developed alongside the original JavaScript/Mineflayer project. Its target is **Minecraft Java Edition 1.21.1**, pinned to **Azalea 0.10.3+mc1.21.1**. The native runtime does not use Mineflayer or a JavaScript gameplay bridge.

## Current implementation

- Shared Tokio process for the CLI, HTTP API, dashboard, persistence, AI provider client, and optional Minecraft connection.
- Axum dashboard/API bound to loopback by default. API routes require a bearer token; a one-time token is generated and printed when no token is configured.
- Provider adapters for OpenAI-compatible endpoints (including Ollama), Anthropic Messages, and Google Gemini.
- SQLite conversation and strategic-goal persistence using WAL mode.
- Specialist-role task decomposition (planner, scout, gatherer, builder, crafter, guardian), durable priority queue, atomic task claims with two-minute renewable leases, abandoned-task recovery, and completion tracking shared by workers using the same database. Workers renew with `POST /api/tasks/{id}/heartbeat`. Azalea can run a multi-client offline-mode swarm with a staggered join delay; Microsoft auth currently supports one account.
- Deterministic reflex safety layer stops movement and blocks new navigation, mining, placement, and combat at critical health. At food 6/20 or lower, Autonomous mode selects safe food (never golden apples or harmful foods), moves it to an empty hotbar slot if needed, and eats it; Assisted/Balanced modes warn and wait for user approval. These reflexes do not depend on an LLM.
- Tactical single-action planner and strategic goal decomposition are separate provider-backed paths.
- Explicit Assisted / Balanced / Autonomous policy gate. The dashboard's Approve & Execute action is the explicit approval path; Autonomous automatically dispatches supported plans without a click. Drop remains blocked; attack targets a named online player in the loaded world within 3.5 blocks and always requires explicit approval.
- Azalea multi-client swarm with offline or single-account Microsoft device-code authentication, per-agent spawn/disconnect accounting, live position/health/hunger/inventory telemetry, and AI chat replies when players mention `Mindcraft`.
- A bounded in-process command queue with `say`, `stop`, `follow <player>`, `look x,y,z`, `goto x,y,z`, `explore <radius>` (8–128 blocks, non-destructive randomized compass step), `mine x,y,z`, `place x,y,z` (right-clicks that existing support block; placement occurs on the face hit), `attack <player>` (loaded player within 3.5 blocks; explicit approval), `equip 0-8`, `jump`, `sneak true|false`, `use_item` (block interaction or bounded 32-tick air use; manual use requires explicit approval), `craft planks|sticks|crafting_table` (2x2 recipes), `deposit x,y,z` (main inventory to container, hotbar preserved), and `withdraw x,y,z,item_id`. Mining and placement require exact crosshair confirmation. Assisted/Balanced modes require approval; Autonomous can dispatch supported plans and addressed server-chat requests. Navigation never auto-mines blocks; unsupported actions fail closed.
- No CI or GitHub Actions workflows are added.

## Minecraft 1.21.1 compatibility

The dependency is pinned to the published `0.10.3+mc1.21.1` Azalea release. The Minecraft adapter has been migrated away from the 0.16 API and now uses the 0.10.3 client, event, pathfinder, inventory, and swarm interfaces. This source-level migration still needs `cargo check` and a live 1.21.1 server run before it can be considered verified.

## Build

Install Rustup and a C toolchain required by bundled SQLite. The checked-in `rust-toolchain.toml` pins `nightly-2025-06-01` automatically; the pinned toolchain still needs a local build verification:

```sh
cd rust
cp .env.example .env
cargo fmt --all
cargo check --all-targets
cargo test
cargo build --release
```

These are local commands only; no GitHub Actions or CI workflow is used. A successful build has not yet been verified after the 1.21.1 API migration. Treat the runtime as in development until formatting, compile, unit tests, and a live 1.21.1 server connection have all been verified.

## Configure

Edit `.env`:

- `MINDCRAFT_MC_SERVER=127.0.0.1:25565`
- `MINDCRAFT_MC_USERNAME=Mindcraft` (fallback offline username)
- `MINDCRAFT_MC_ACCOUNTS=Mindcraft,Scout,Builder` for multiple offline-mode clients
- `MINDCRAFT_MC_AUTH=offline|microsoft`
- `MINDCRAFT_MICROSOFT_EMAIL=you@example.com` when using Microsoft device-code authentication
- `MINDCRAFT_PROVIDER=ollama` (or `openai`, `openai-compatible`, `anthropic`, `google`)
- `MINDCRAFT_MODEL=llama3.1` (required for hosted and OpenAI-compatible providers; Ollama defaults to `llama3.1`)
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

The core action layer now uses Azalea 0.10.3 APIs for chat, stop, follow, look, non-destructive pathfinding, mining, block interaction, named-player melee, hotbar selection, jumping, and inventory telemetry. Sneak toggling is sent with the Minecraft 1.21.1 player-command packet. Manual held-item use supports block interaction or an air-use packet with a bounded 32-tick hold. Autonomous low-food recovery uses only the safe-food allowlist and preserves the hotbar by moving food only into an empty slot. Mining and placement wait for exact crosshair confirmation. These paths are implemented but not yet compile-verified or live-server tested on 1.21.1.

The durable specialist task queue supports role-based claims, two-minute leases, heartbeats, and completion, but task claims are not yet automatically tied to in-game execution. Remaining work includes additional recipe-based crafting and finer-grained inventory transfers, automatic health restoration/healing, advanced multi-step construction, autonomous exploration strategy, automated execution/completion of strategic tasks, per-agent role assignment, and full remote API hardening (TLS termination, rate limits, and per-agent authorization). Do not treat this branch as gameplay-ready until a local build and live 1.21.1 server verification succeed.
