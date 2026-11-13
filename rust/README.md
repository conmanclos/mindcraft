# Mindcraft Native (Rust)

## Cross-platform desktop application

A Tauri-based cross-platform native app now provides a first-run setup screen, persists non-secret settings in the app data directory, stores the AI provider key in the operating system credential store, starts/stops the bundled Rust runtime, and displays live connection telemetry plus teammate chat. The app is designed so users do not need Node.js or a Rust toolchain installed.

Build on the target operating system with `bash build-desktop.sh` on macOS/Linux or `./build-desktop.ps1` in PowerShell on Windows. These scripts build and stage the matching Rust runtime binary before Tauri packaging; they do not run tests. The macOS-specific `build-macos.sh` remains available for `.app`/`.dmg` packaging. See `desktop/README.md` for platform prerequisites and current limitations.


This is a Rust-native runtime being developed alongside the original JavaScript/Mineflayer project. Its target is **Minecraft Java Edition 26.1**, pinned to **Azalea 0.16.0+mc26.1**. The native runtime does not use Mineflayer or a JavaScript gameplay bridge.

## CLI operations

The native CLI can operate the same local runtime used by the browser dashboard and desktop app. Start the runtime with `cargo run -- serve`, then set `MINDCRAFT_API_TOKEN` in the CLI environment to the same bearer token printed/configured by that running process. API-backed CLI operations intentionally connect only to a loopback runtime; they do not send the bearer token to a LAN host.

```sh
cargo run -- status
cargo run -- chat "What should we do next?"
cargo run -- goals
cargo run -- memories
cargo run -- remember preference building "Prefers compact stone bases"
cargo run -- forget-memory 1
cargo run -- goal-tasks 1
cargo run -- review-tasks
cargo run -- claim-task Mindcraft scout
cargo run -- heartbeat-task 1 Mindcraft
cargo run -- complete-task 1 Mindcraft
cargo run -- release-task 1 Mindcraft
cargo run -- fail-task 1 Mindcraft "Path was blocked"
cargo run -- issue-agent-credential scout-worker-1
cargo run -- revoke-agent-credential scout-worker-1
cargo run -- plan "Gather food and build a shelter" --agent Mindcraft
cargo run -- create-goal "Prepare a safe base near spawn"
cargo run -- cancel-goal 1
cargo run -- retry-goal 1
cargo run -- build '{"origin":[0,64,0],"blocks":[{"x":0,"y":0,"z":0,"material":"oak_planks"}]}'
cargo run -- build-sponge starter.schem --origin 100,64,100 --rotation 90 --agent Builder
cargo run -- execute '{"action":"stop","risk":"low","reason":"User approved stop"}'
cargo run -- execute '{"action":"trade","target":"12,64,20","risk":"high","reason":"User approved merchant inspection"}'
cargo run -- execute '{"action":"trade","target":"12,64,20,0","risk":"high","reason":"User approved this exact offer"}'
cargo run -- execute '{"action":"enter_portal","target":"100,64,100","risk":"high","reason":"User approved this portal transition"}'
```

`chat` uses the running API so its history and persistent memory are shared with the dashboard; `direct-chat` calls the provider without runtime history. `remember`, `forget-memory`, `goal-tasks`, `review-tasks`, and the task lease/claim/complete/release/fail commands and short-lived agent credential issue/revoke commands expose the same persistent-memory and specialist-task workflow; `build` accepts the same bounded mixed-material JSON schematic payload as the dashboard; `build-sponge` imports a local Sponge v2/v3 .schem file through the same authenticated runtime. `execute` is an explicit approval action: it sends the supplied structured action to the same policy gate and executor used by the dashboard. `plan` may dispatch automatically only when the configured autonomy mode permits it.

## Current implementation

- Shared Tokio process for the CLI, HTTP API, dashboard, persistence, AI provider client, and optional Minecraft connection.
- Axum dashboard/API bound to loopback by default, with optional native Rustls HTTPS for direct remote binds. Non-loopback binds require both MINDCRAFT_TLS_CERT and MINDCRAFT_TLS_KEY; otherwise use a loopback bind behind a trusted TLS reverse proxy. API routes require a bearer token; a one-time token is generated and printed when no token is configured. A per-credential fixed-window limiter caps requests at 180 per 60 seconds and bounds its in-memory credential buckets.
- Provider adapters for OpenAI-compatible endpoints (including Ollama), Anthropic Messages, and Google Gemini.
- SQLite conversation and strategic-goal persistence using WAL mode, a bounded five-second busy wait, automatic WAL checkpoints, secure deletion, owner-only database-file permissions on Unix, and a 10,000-record memory ceiling that evicts the least-important/least-recently-accessed entry when a new unique record is added; persistent writes validate conversation roles and payload sizes, enforce bounded task roles/descriptions and 1-12 task goal plans, and only allow tasks to be appended to active goals; dashboard chat and addressed in-game teammate conversations share the same bounded history.
- Layered persistent memory in SQLite for semantic knowledge, episodic events, player preferences, and world facts. Records are keyed by memory type and subject, carry confidence/importance/source metadata, update in place instead of duplicating, and are available to both chat and specialist planning. The browser dashboard and desktop app can create, review, and delete memories through authenticated API routes.
- Specialist-role task decomposition (planner, scout, gatherer, builder, crafter, guardian), durable priority queue, atomic goal-and-task plan creation, atomic task claims with two-minute renewable leases, strict per-goal task ordering (a later specialist cannot claim its task until every earlier task in that goal is verified complete), abandoned-task recovery, persisted attempt/error diagnostics, bounded exponential-style retry delays, terminal failure after five claims, and completion tracking shared by workers using the same database. Workers renew with `POST /api/tasks/{id}/heartbeat` and report execution failures with `POST /api/tasks/{id}/fail`; retryable failures return to the queue after a delay, while exhausted tasks fail their goal and cancel its remaining work. In Autonomous mode, each connected bot prioritizes tasks matching its assigned role, then falls back to other specialist queues when its own queue is empty so solo and smaller teams can still progress. It plans one supported action, dispatches it to that exact bot, and moves the task to human result verification; tasks whose action requires explicit approval are released back to the queue. Assisted and Balanced modes retain manual task claiming and approval. In the browser dashboard and desktop app, a claimed task can be sent to the action planner; the UI renews the task lease while it remains selected, remembers the claiming agent ID, and requires confirmation before marking the task complete. Review and approve the proposed game action as required by the autonomy policy, then mark the task complete only after verifying the in-game result. Azalea can run a multi-client offline-mode swarm with a staggered join delay and bounded exponential reconnect backoff (5-60 seconds, reset after a stable minute); Microsoft auth retry also backs off up to five minutes; Microsoft auth supports one email per configured bot username, up to eight accounts.
- Deterministic reflex safety layer cancels active mining, stops pathfinding/movement, and warns when health reaches 6/20. In Autonomous mode, bounded survival reflexes may equip recognized armor into empty armor slots only (never replacing worn gear), eat recognized hotbar food at hunger 12/20 or lower, prioritize a hotbar golden apple for regeneration when health is 10/20 or lower, and defend against visible whitelisted hostile mobs within 4 blocks. Defensive combat never targets players, passive mobs, or neutral mobs and is disabled at critical health. The reflexes never consume arbitrary held items, potions, harmful/unknown food, or items from the main inventory. Low-food and critical-health warnings remain available in other modes.
- Tactical single-action planner and strategic goal decomposition are separate provider-backed paths.
- Explicit Assisted / Balanced / Autonomous policy gate. The dashboard's Approve & Execute action is the explicit approval path; Autonomous automatically dispatches supported plans without a click. Risk floors are derived from the executor (the model cannot downgrade mining, placement, attack, or generic item use); Assisted requires explicit approval for every action; Balanced requires approval for high-risk actions, and attack/generic item use always require it. Drop remains blocked; attack is restricted to a visible crosshair target within 3.5 blocks and always requires explicit approval.
- Azalea multi-client swarm with offline or multi-account Microsoft device-code authentication (up to eight accounts, each with an isolated token cache), per-agent spawn/disconnect accounting, live per-agent dimension/position/health/hunger/inventory telemetry plus a server player roster maintained from tab-list join/update/leave events (dimension changes are tracked from server login and respawn packets, including Nether/End transitions), and AI chat replies when players mention `Mindcraft`. In Autonomous mode, server-chat actions are disabled unless the sender is explicitly listed in `MINDCRAFT_TRUSTED_PLAYERS`; chat from other players remains conversational and cannot trigger gameplay actions. The allowlist is limited to 32 valid Minecraft usernames and defaults to empty. The desktop setup exposes the same allowlist. The app supplies an app-scoped Azalea auth-cache path so Microsoft sign-in data stays with Mindcraft's application data.
- Recipe crafting uses Azalea's inventory click protocol for planks, sticks, torches, crafting tables, wooden/stone/iron/golden/copper/diamond tools, leather/iron/golden/copper/diamond armor, bows, fishing rods, arrows, paper, glass panes, shears, buckets, doors, trapdoors, slabs, stairs, fences, signs, boats, bowls, furnaces, chests, bread, ladders, composters, shields, beds, bookshelves, smokers, blast furnaces, buttons, pressure plates, levers, fence gates, flower pots, glass bottles, item frames, stone bricks, polished andesite/diorite/granite/deepslate, looms, campfires, brewing stands, stonecutters, grindstones, fletching tables, cartography tables, hoppers, droppers, dispensers, barrels, compasses, clocks, flint-and-steel, crossbows, maps, golden apples, golden carrots, pumpkin pies, cookies, iron/gold/diamond/emerald/redstone/lapis/coal/copper/quartz storage blocks, hay bales, slime blocks, honey blocks, iron/gold nugget conversions, hay-to-wheat, bone meal, bone blocks, dried-kelp blocks, netherite ingots/blocks, amethyst blocks, glowstone, redstone torches, jukeboxes, enchanting tables, anvils, sugar, melon blocks, glistering melon slices, magma cream, fermented spider eyes, fire charges, ender eyes, ender chests, TNT, pistons, sticky pistons, repeaters, comparators, redstone lamps, mushroom stew, rabbit stew, beetroot soup, spyglasses, archaeology brushes, recovery compasses, armor stands, dispensers, observers, daylight detectors, target blocks, note blocks, rail variants, minecarts and chest/furnace minecarts, tripwire hooks, books, writable books, iron trapdoors, iron bars, lanterns, soul lanterns, and chiseled bookshelves. Two-by-two recipes use the player inventory; three-by-three recipes require a crafting-table coordinate. The bot pathfinds to a table without breaking blocks, requires line of sight, returns only ingredients it placed when setup fails, and verifies the expected output before collecting it. Crafting remains policy-gated.
- Allowlisted batch smelting uses real furnace, blast-furnace, or smoker menus. `smelt input[*count]@x,y,z` handles 1-64 recognized inputs, assembles input items and required fuel across multiple inventory stacks of the same exact item/type (coal/charcoal: 8 items each, blaze rods: 12, lava buckets: 100), waits for the server-produced full output batch, and collects only after the expected count is confirmed. Mixing into an occupied fuel slot and unsupported recipes are refused; mixing different fuel types is refused; automatic follow-up collection remains future work. The `smelt_queue` action accepts 2-4 distinct furnace coordinates and processes their allowlisted batches in order on one bot, stopping after the first job whose full output batch is not confirmed and collected. The high-risk `smelt_parallel` action uses the same syntax but requires one connected bot per job, dispatches each furnace to a separate agent, and rejects a single-agent target; each bot reports its own result independently.
- Exploration combines a bounded golden-angle spiral around the bot's current position (8-128 block radius) with a low-cost scan of nearby loaded blocks for storage, general/utility/farming/village workstations, suspicious-sand/gravel archaeology sites, visible overworld/nether ores and debris, and loaded portal/structure landmarks. It approaches a nearby POI from an adjacent air block before falling back to the spiral, records discovered POIs as world memory, continues rotating around the radius boundary instead of repeating a fixed compass loop, skips unreachable waypoints after 60 seconds, and does not mine while navigating; container transfers pathfind to a loaded target without mining, require direct line of sight, and verify that the selected slot changed after a shift-click. Both are policy-gated, and container transfers always require explicit approval.
- Targeted multi-client command routing: each offline-mode bot has its own broadcast subscription, receives a stable specialist role (planner, scout, gatherer, builder, crafter, guardian, then repeating for larger teams), and exposes that role in live telemetry and the dashboard/desktop roster. Commands can be routed to one configured username; untargeted commands remain primary-only. Microsoft-authenticated teams can also be targeted by their configured Minecraft usernames.
- A bounded in-process broadcast command channel. `say`, `stop`, `jump`, `follow <player>` (recalculates routes that remain stuck for 60 seconds), `look x,y,z`, `goto x,y,z`, `explore [radius]`, `till x,y,z` (uses a hoe from the hotbar to convert ordinary dirt/grass blocks into farmland after checking the top face and confirming the server-side block change), `plant x,y,z` (uses recognized seeds/crops and mushrooms in the hotbar; farmland is required for wheat/beetroot/pumpkin/melon seeds and carrots/potatoes; nether wart uses soul sand, sugar cane requires adjacent water and a supported dirt/grass/sand/mud/podzol/mycelium/moss substrate, cactus requires sand and clear horizontal space, bamboo accepts supported dirt/grass/sand/gravel/podzol/mycelium/moss/mud substrates, and red/brown mushrooms are restricted to mycelium, podzol, crimson nylium, or warped nylium until local light can be verified; all planting uses a top-face raycast and server confirmation), `harvest x,y,z` (checks crop age for wheat, carrot, potato, beetroot, and nether wart; red/brown mushroom blocks are valid direct harvest targets; mature pumpkin/melon stems resolve to adjacent fruit so the stem is preserved, and grown sugar-cane/cactus/bamboo columns can be harvested from any segment by removing only their top segment), `mine x,y,z` (pathfinds without mining en route, requires a loaded non-air target and direct raycast, selects the fastest suitable hotbar tool, confirms the block became air, then safely approaches the drop location; cancellation and timeout abort active mining), `place x,y,z` (pathfinds to a reachable adjacent full-collision support block, verifies the exact top/bottom/side face raycast, then confirms placement; partial collision shapes are not supported), `build [item@]x,y,z;x,y,z;...` (places 1-64 positions sequentially, sorted bottom-up; optionally selects an exact item from the hotbar or moves it from main inventory into an empty hotbar slot without overwriting items; supports reusable `blueprint:platform_5x5@x,y,z`, `blueprint:wall_7x3@x,y,z`, `blueprint:starter_hut@x,y,z`, `blueprint:starter_house_4x4@x,y,z`, `blueprint:bridge_7x3@x,y,z`, `blueprint:watchtower_3x3@x,y,z`, and `blueprint:perimeter_wall_7x7x2@x,y,z` templates, optionally prefixed with `item@`; also accepts bounded mixed-material JSON schematics as `schematic:{"blocks":[{"x":0,"y":0,"z":0,"material":"oak_planks"}]}|x,y,z`, with 1-64 unique relative block positions within 32 blocks of the origin; optional `rotation` (0/90/180/270), `mirror_x`, and `mirror_z` transform the footprint; each material may be assembled across multiple stacks, with main-inventory stacks moved only into empty hotbar slots; every target must be air with a reachable adjacent full-collision support face; stops on the first unconfirmed placement), `container_transfer x,y,z,slot|start-end|all|item:exact_item_name` (pathfinds without mining, verifies line of sight, transfers only container-side slots, and checks each slot after shift-click), `container_deposit x,y,z[,exact_item_name]` (deposits only main-inventory slots; an optional exact item-name filter preserves nonmatching items, and the hotbar, armor, and offhand are always preserved), `container_sort x,y,z` (sorts supported 9-, 27-, or 54-slot storage containers by full stack identity, verifies every swap, and never moves player inventory slots), `craft recipe[*count][@x,y,z]` (allowlisted crafting with 1-16 sequential batches; every batch must have its full expected output collected before the next begins), `smelt input[*count]@x,y,z` (1-64 allowlisted inputs in a nearby furnace, blast furnace, or smoker; count defaults to one; fuel is selected by capacity and can be assembled across multiple stacks of the same type—coal/charcoal 8 items, blaze rods 12, lava buckets 100—and the executor refuses occupied fuel slots and verifies the full output count), `attack crosshair` (manual visible-entity attack within 3.5 blocks; always requires explicit approval; Autonomous mode also has a separate narrow defensive reflex against whitelisted hostile mobs within 4 blocks), `use_item` (always requires explicit approval), and `equip 0-8` are dispatched on Minecraft ticks. `trade x,y,z` opens a villager or wandering-trader window within 4.5 blocks and lists the server-provided offers; `trade x,y,z,index` (zero-based offer index) selects and completes exactly one offer only when the live server input costs exactly match the listed offer, then verifies the output entered inventory; offers requiring item-component matching are refused rather than guessed. Merchant trades always require explicit approval. `enter_portal x,y,z` only enters an already-active Nether portal, End portal, or End gateway at the exact coordinate; it verifies the block and reports success only after a dimension change or same-dimension portal teleport is observed. It never constructs or activates portals, and every transition requires explicit approval. The explicitly approved `optimize_armor` action compares recognized armor by material tier, remaining durability, and useful enchantments before replacing worn gear; the explicitly approved `inventory_sort` action sorts only the 27 main-inventory slots by item registry name, leaves hotbar/armor/offhand untouched, swaps whole stacks without merging, and verifies each server-side slot update; the autonomous reflex itself still fills empty armor slots only. Assisted/Balanced dispatch requires user approval; Autonomous mode can queue supported policy-approved plans from the dashboard or from allowlisted Minecraft chat senders without a click; attack, generic item use, and container transfers still require explicit approval. When Autonomous mode starts, a background planner creates one durable baseline survival objective (safe survival, sustainable food/basic resources, and a compact shelter near spawn). If provider planning is unavailable, it retries every 60 seconds; the exact goal is not duplicated on reconnect, and a failed baseline objective is replanned by the background monitor. Coordinates are range-checked; unimplemented actions fail closed.
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
- `MINDCRAFT_MC_ACCOUNTS=Mindcraft,Scout,Builder` for a multi-bot team in either auth mode
- `MINDCRAFT_MC_AUTH=offline|microsoft`
- `MINDCRAFT_MICROSOFT_EMAIL=you@example.com,teammate@example.com` when using Microsoft device-code authentication (one email per bot username)
- `MINDCRAFT_TRUSTED_PLAYERS=YourName,FriendName` to allow only those server-chat users to trigger autonomous gameplay actions (empty by default)
- `MINDCRAFT_PROVIDER=ollama` (or `openai`, `openai-compatible`, `anthropic`, `google`)
- `MINDCRAFT_MODEL=llama3.1`
- `MINDCRAFT_BASE_URL=http://127.0.0.1:11434/v1` for Ollama
- `MINDCRAFT_API_KEY=...` for hosted providers
- `MINDCRAFT_MODE=assisted|balanced|autonomous`
- `MINDCRAFT_BIND=127.0.0.1:8787`
- `MINDCRAFT_TLS_CERT=/path/to/fullchain.pem` and `MINDCRAFT_TLS_KEY=/path/to/private-key.pem` (both required for direct non-loopback binds; optional for loopback)

Secrets are read from environment variables and are not written to SQLite. Keep the bind address on loopback unless you set a strong `MINDCRAFT_API_TOKEN` and configure both `MINDCRAFT_TLS_CERT` and `MINDCRAFT_TLS_KEY` for native HTTPS, or put the loopback service behind a trusted TLS reverse proxy. Azalea supports offline account mode for local/offline-mode servers and Microsoft device-code authentication for online-mode servers. No Microsoft password is accepted or stored by this application.

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


### Sponge schematic import

The dashboard and desktop app accept Sponge v2/v3 .schem files. The file is Base64-encoded in the client and submitted to POST /api/build/sponge with an absolute origin coordinate. The server accepts gzip-compressed or raw NBT, caps the compressed request and decompressed NBT size, validates dimensions and palette IDs, decodes the block-data VarInts, skips air, and refuses imports with more than 64 placeable blocks. The existing build executor still requires exact material stacks in inventory and confirms each placement. Block-state properties such as stair facing/axis, block entities, entities, fluids, fire, and portal states are not restored; use this import for material/layout transfer, not exact world-state cloning.

## Custom mixed-material schematic API

```http
POST /api/build
Authorization: Bearer <MINDCRAFT_API_TOKEN>
Content-Type: application/json

{
  "origin": [100, 64, 100],
  "rotation": 90,
  "mirror_x": false,
  "mirror_z": false,
  "agent": "Builder",
  "blocks": [
    { "x": 0, "y": 0, "z": 0, "material": "oak_planks" },
    { "x": 1, "y": 0, "z": 0, "material": "cobblestone" }
  ]
}
```
Coordinates in `blocks` are relative to `origin`. The runtime accepts 1-64 unique blocks, limits each relative axis to 32 blocks, supports 0/90/180/270-degree x/z rotation and independent x/z mirroring, and preflights one sufficient inventory stack per material before placing anything. The optional `agent` selects one configured connected bot; omit it to use the primary bot.

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
| `POST /api/build` | Explicitly queue a bounded mixed-material JSON schematic at an absolute origin |
| `POST /api/build/sponge` | Import a bounded gzip-NBT Sponge v2/v3 schematic from Base64 and queue its block palette at an absolute origin |
| `GET/POST /api/goals` | List goals or decompose a new strategic goal into specialist tasks |
| `GET /api/goals/{id}/tasks` | List a goal's tasks in enforced execution order |
| `POST /api/goals/{id}/cancel` | Cancel an active goal and revoke its pending, claimed, and review tasks |
| `POST /api/goals/{id}/retry` | Resume a failed goal without repeating tasks already verified complete |
| `POST /api/agents/{agent_id}/credential` | Issue/rotate a one-hour, in-memory agent token (master token required) |
| `DELETE /api/agents/{agent_id}/credential` | Revoke an agent token (master token required) |
| `POST /api/tasks/claim` | Atomically claim the highest-priority pending task for a role; accepts the master token or that agent's token |
| `POST /api/tasks/{id}/heartbeat` | Renew a two-minute task lease |
| `POST /api/tasks/{id}/fail` | Report a failed attempt; retries use bounded backoff and stop after five claims |
| `POST /api/tasks/{id}/release` | Release a task back to the pending queue when a worker cannot complete it |
| `POST /api/tasks/{id}/complete` | Complete a task owned by the requesting agent |

Worker roles are `planner`, `scout`, `gatherer`, `builder`, `crafter`, and `guardian`. Claims include an agent ID so multiple workers can share the same SQLite database without taking the same live task.

## Important scope boundary

The current branch contains a first-pass command executor implemented against the Azalea 0.16.0 API for chat, stop, follow, look, pathfinding, mining, block interaction, attacking, hotbar selection, crop workflows (including pumpkin/melon fruit and nether wart), recipe crafting, batch smelting, container transfers, and bounded construction. These code paths are present, but none should be represented as verified working until the native project compiles and each action is exercised against a live Minecraft 26.1 server. Connection and spawn telemetry are implemented separately from gameplay verification.

The durable specialist task queue supports role-based claims, two-minute leases, heartbeats, completion, and explicit worker failure reporting. Failed attempts retain a bounded error reason, retry after an increasing delay (30 seconds per attempt, capped at 10 minutes), and become terminal after five claims so impossible work cannot circulate forever. In Autonomous mode, connected bots claim a specialist task, plan one supported action, dispatch it to the assigned bot, and move the task into a human review state; the player must still verify the result before completion. The browser dashboard and desktop app renew a manually claimed task lease while it remains selected; the desktop app sends a heartbeat every 45 seconds and binds completion to the original claiming agent ID. This prevents an otherwise active worker from losing its lease merely because it is working longer than two minutes. The autonomous food reflex is deliberately limited to recognized hotbar food; it is not a general inventory manager or healing system. Remaining implementation work includes additional recipe coverage beyond the current explicit allowlist; parallel furnace execution within one bot and unattended follow-up collection after a wait timeout/menu close (the 2-4 job `smelt_queue` remains sequential, while `smelt_parallel` assigns jobs to separate connected bots); full inventory management beyond the implemented armor optimization, main-inventory sorting, and verified storage-container sorting (hotbar, armor, and offhand are preserved by main-inventory sorting); full-fidelity schematic state/entity import (the dashboard and desktop app now import Sponge v2/v3 gzip-NBT .schem palettes with up to 64 non-air blocks, plus the existing bounded JSON format; block-state facing/axis properties, block entities, entities, fluids, fire, and portal-state restoration are intentionally not reproduced by the current item-placement executor; coordinate rotation/mirroring, multi-stack material preflight, and empty-hotbar-only staging remain supported); biome- and structure-aware exploration beyond nearby loaded-block POIs and the bounded spiral; multi-action task execution, automatic world-state outcome verification, and recovery/replanning after partial task completion; native Rustls HTTPS is now available for direct remote binds, with loopback-only HTTP as the default. Goal tasks are now claimed in their persisted plan order, failed goals can be retried without repeating verified completed tasks, and active goals can be cancelled from the API/dashboard/desktop app. Task claim, heartbeat, and completion already accept per-agent one-hour credentials or the master dashboard token, and the runtime enforces a per-credential 180-request/60-second limit. Do not treat this branch as gameplay-ready until a local build and live 26.1 server verification succeed.
