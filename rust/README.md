# Mindcraft Native (Rust)

This is a Rust-native runtime added alongside the original JavaScript/Mineflayer project. It targets **Minecraft Java 1.21.1** and pins **Azalea 0.10.3+mc1.21.1** rather than silently tracking the current Azalea release.

## Current implementation

- Shared Tokio process for the CLI, HTTP API, dashboard, persistence, AI provider client, and optional Minecraft connection.
- Axum dashboard/API bound to loopback by default. API routes require a bearer token; a one-time token is generated and printed when no token is configured.
- Provider adapters for OpenAI-compatible endpoints (including Ollama), Anthropic Messages, and Google Gemini.
- SQLite conversation persistence using WAL mode.
- Explicit Assisted / Balanced / Autonomous policy gate for planned actions. Assisted blocks destructive actions; high-risk actions are blocked outside Autonomous. Attack/drop remain blocked until target-aware execution is implemented.
- Azalea offline-account connection for local/offline-mode servers, spawn/disconnect state, and AI chat replies when players mention `Mindcraft` or whisper to it.
- A bounded in-process command queue. Explicitly approved `say`, `stop`, `goto x,y,z`, and `mine x,y,z` commands are dispatched on Minecraft ticks. Coordinates are range-checked; unimplemented actions fail closed.
- No CI or GitHub Actions workflows are added.

## Build

Install Rust (stable toolchain) and a C toolchain required by bundled SQLite, then:

```sh
cd rust
cp .env.example .env
cargo build --release
```

If your environment requires nightly for the pinned Azalea release, install the toolchain requested by Cargo's error output; do not update Azalea to a different Minecraft protocol version as a workaround.

## Configure

Edit `.env`:

- `MINDCRAFT_MC_SERVER=127.0.0.1:25565`
- `MINDCRAFT_MC_USERNAME=Mindcraft`
- `MINDCRAFT_PROVIDER=ollama` (or `openai`, `openai-compatible`, `anthropic`, `google`)
- `MINDCRAFT_MODEL=llama3.1`
- `MINDCRAFT_BASE_URL=http://127.0.0.1:11434/v1` for Ollama
- `MINDCRAFT_API_KEY=...` for hosted providers
- `MINDCRAFT_MODE=assisted|balanced|autonomous`
- `MINDCRAFT_BIND=127.0.0.1:8787`

Secrets are read from environment variables and are not written to SQLite. Keep the bind address on loopback unless you set a strong `MINDCRAFT_API_TOKEN` and put the service behind a trusted TLS reverse proxy. The initial Azalea connector uses offline account mode; it is intended for local/offline-mode servers until Microsoft account authentication is integrated.

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

This is the first native runtime slice, not a claim that every Mineflayer skill has already been ported. The dashboard chat and provider adapters work independently of Minecraft; the Azalea client connects and observes chat. Full action execution (navigation, mining, crafting, inventory, combat, building), Microsoft auth, robust reconnect supervision, multi-agent coordination, and remote API hardening still need implementation and live-server verification. The action policy is deliberately fail-closed while those execution adapters are absent.
