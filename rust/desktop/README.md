# Mindcraft cross-platform desktop app

Mindcraft Desktop is a Tauri 2 cross-platform application shell around the native Rust Minecraft runtime. It is designed to make setup and launch a graphical workflow rather than requiring terminal commands.

## User experience

1. Open the Mindcraft app.
2. Enter the Minecraft server and choose Microsoft authentication or **Cracked / Offline Mode**. Offline mode lets the bot join without a second paid Minecraft account, but only on servers whose owners have explicitly enabled offline/cracked connections (`online-mode=false`). It does not bypass authentication on online-mode servers. Offline mode supports a comma-separated team of up to eight unique bot usernames; Microsoft sign-in supports up to eight accounts, with one email per configured bot username and an isolated token cache for each.
3. Select an AI provider, model, and API key (the key is stored in macOS Keychain). Optionally list trusted Minecraft chat usernames; only those players may trigger autonomous game actions through server chat, while all other players can still receive conversational replies.
4. Choose Assisted, Balanced, or Autonomous mode.
5. Save setup and click **Run Mindcraft**.
6. The app starts the bundled Rust runtime on an automatically selected local port, waits for its authenticated `/api/status` endpoint to answer before reporting success, and points to the runtime log if startup exits or times out. It then displays connection/agent/health/food status. For offline-mode teams, the action planner can target a specific configured bot username; leaving the target blank routes to the primary bot. The specialist task panel can claim work, renew its two-minute lease while the task is active, pass the task description to the action planner, and mark it complete only after you confirm the in-game result and provide a 1-500 byte verification note; the note is persisted with the task and in episodic memory. The planner also exposes bounded waypoint exploration with nearby POI discovery, guarded single-slot, range, all-container transfers, and main-inventory deposits that preserve the hotbar, verified storage-container sorting that leaves player slots untouched, and component-aware main-inventory stack compaction, allowlisted crafting and smelting (including same-type fuel assembled across inventory stacks and explicit smelt_collect recovery after a furnace wait timeout/menu close), soil tilling, farmland planting, mature-crop harvesting, explicit multi-block placement, and three reusable construction templates. Persistent memory can be managed from the desktop app while the runtime is active. Saved goals and their task statuses are reloaded into the desktop app and refreshed while the runtime is active, even if Minecraft is temporarily offline. In Autonomous mode, each connected bot prioritizes its specialist role and falls back to other task roles when its own queue is empty, allowing solo and smaller teams to progress. It plans one supported action for a claimed task and moves that task to human result verification; actions requiring explicit approval are returned to the queue.

The schematic importer accepts bounded Sponge v2/v3 .schem files as well as the native JSON format. It imports up to 64 non-air block materials; block-state orientation, block entities, entities, fluids, fire, and portal states are not restored.

The app stores its settings (including the offline bot team), local API bearer token, SQLite database, and Azalea Microsoft authentication cache under the operating system's application data directory. Unix builds apply private filesystem permissions. AI provider keys are stored through the operating system credential store: macOS Keychain, Windows Credential Manager, and Linux's persistent Secret Service/keyutils backend. Microsoft tokens remain in Mindcraft's app-scoped data rather than the global `~/.minecraft` cache. The installed app does not require Node.js or a separately installed Rust toolchain.

## Packaging

Build separately on each target operating system and architecture; the scripts package the host runtime and do not claim cross-compilation or universal binaries.

macOS / Linux (Bash):

```sh
cd rust
bash build-desktop.sh
```

Windows (PowerShell):

```powershell
cd rust
.\\build-desktop.ps1
```

The scripts install the nightly toolchain and Tauri v2 CLI if needed, build the native runtime, stage the platform-specific executable, and invoke `cargo tauri build`. They intentionally do not run tests; implementation, audit, and testing are separate project phases. macOS users may use `bash build-macos.sh` for the `.app` and `.dmg` bundles. Linux packaging requires Tauri's system WebKitGTK/build dependencies. Windows packaging produces the configured Windows installer formats. Sign/notarize macOS releases and sign Windows releases before public distribution.

## Current development status

This is the initial desktop shell and setup workflow. It still requires a successful native build, per-platform integration testing, and a live Minecraft 26.1 server test before it can be called ready. The build script and Azalea 0.16.0+mc26.1 integration are now in place, but neither has been compile-verified in this environment. The desktop task workflow now renews leases every 45 seconds, remembers the claiming worker identity even if the editable worker field changes, and requires confirmation before completion. Microsoft authentication, app-scoped auth caching, Keychain behavior, runtime startup/stop, and task workflow still require an end-to-end Mac test. Sign and notarize before public distribution. No CI or GitHub Actions are used.
