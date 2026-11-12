# Mindcraft Native (Rust)

This is a Rust-native runtime added alongside the original JavaScript/Mineflayer project. It targets **Minecraft Java 1.21.1** and pins **Azalea 0.10.3+mc1.21.1** rather than silently tracking the current Azalea release.

## Current implementation

- Shared Tokio process for the CLI, HTTP API, dashboard, persistence, AI provider client, and optional Minecraft connection.
- Axum dashboard/API bound to loopback by default. API routes require a bearer token; a one-time token is generated and printed when no token is configured.
- Provider adapters for OpenAI-compatible endpoints (including Ollama), Anthropic Messages, and Google Gemini.
- SQLite conversation and strategic-goal persistence using WAL mode.
- Specialist-role task decomposition (planner, scout, gatherer, builder, crafter, guardian), durable priority queue, atomic task claims, and completion tracking shared by workers using the same database.
- Explicit Assisted / Balanced / Autonomous policy gate. The dashboard's Approve & Execute action is the explicit approval path; Autonomous automatically dispatches supported plans without a click. Attack/drop remain blocked until target-aware execution is implemented.
- Azalea offline-account connection for local/offline-mode servers, spawn/disconnect state, and AI chat replies when players mention `Mindcraft` or whisper to it.
- A bounded in-process command queue. `say`, `stop`, `look x,y,z`, `goto x,y,z`, `mine x,y,z`, and `equip 0-8` are dispatched on Minecraft ticks. Assisted/Balanced dispatch requires user approval; Autonomous dispatches supported plans automatically. Coordinates are range-checked; unimplemented actions fail closed.
- No CI or GitHub Actions workflows are added.

## Build

Install Rust nightly (required by the Azalea crate's toolchain) and a C toolchain required by bundled SQLite. The checked-in `rust-toolchain.toml` selects nightly automatically, then:

```sh
cd rust
cp .env.example .env
cargo build --release
```

Do not update Azalea to a different Minecraft protocol version as a workaround.

## Configure

Edit `.env`:

- `MINDCRAFT_MC_SERVER=127.0.0.1:25565`
- `MINDCRAFT_MC_USERNAME=Mindcraft` (offline account mode)
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

## Important scope boundary

This is an early native runtime slice, not feature parity with the original Mineflayer agent. Implemented live actions are plain chat, stop, look-at explicit block coordinates, pathfinding to explicit block coordinates, starting a mining operation at explicit coordinates, and selecting a hotbar slot (0-8). The AI also answers Minecraft chat when addressed by name or via whisper. The task coordinator is durable and supports multiple named workers claiming tasks by role, but those worker agents are not yet separate Azalea clients and task completion is not automatically tied to in-game execution. Crafting, inventory management, combat, building/placement, exploration goals, additional account/session lifecycle hardening, and comprehensive remote API hardening remain unimplemented. The dashboard and provider integration have not been built or run in this environment, so a successful compile and live-server session are still required before calling this ready for gameplay.
