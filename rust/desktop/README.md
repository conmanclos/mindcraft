# Mindcraft macOS desktop app

Mindcraft Desktop is a Tauri 2 macOS application shell around the native Rust Minecraft runtime. It is designed to make setup and launch a graphical workflow rather than requiring terminal commands.

## User experience

1. Open the Mindcraft app.
2. Enter the Minecraft server and choose Microsoft authentication or **Cracked / Offline Mode**. Offline mode lets the bot join without a second paid Minecraft account, but only on servers whose owners have explicitly enabled offline/cracked connections (`online-mode=false`). It does not bypass authentication on online-mode servers.
3. Select an AI provider, model, and API key (the key is stored in macOS Keychain).
4. Choose Assisted, Balanced, or Autonomous mode.
5. Save setup and click **Run Mindcraft**.
6. The app starts the bundled Rust runtime and displays connection/agent/health/food status.

The app stores its non-secret configuration, SQLite database, and Azalea Microsoft authentication cache under the macOS application data directory. The provider API key is stored in Keychain. Microsoft tokens are therefore kept with the app's local data rather than the global `~/.minecraft` cache. It does not require Node.js or a separately installed Rust toolchain.

## Packaging

Build on the Mac that will run the app (the script targets the current Mac architecture). Install Rust via rustup and the Xcode Command Line Tools once. The script installs the required nightly toolchain and Tauri v2 CLI if missing:

```sh
cd rust
cargo install tauri-cli --version '^2'
bash build-macos.sh
```

The script builds the runtime from the correct `rust/` Cargo project, stages the binary into the Tauri resources directory, and builds the `.app` and `.dmg` bundles.

The resulting app bundles the native runtime as a resource. Apple Silicon and Intel distribution should be built separately unless a verified universal2 runtime is produced. Sign and notarize the app before public distribution.

## Current development status

This is the initial desktop shell and setup workflow. It still requires a successful native build, macOS integration testing, and a live Minecraft 26.1 server test before it can be called ready. The build script and Azalea 0.16.0+mc26.1 integration are now in place, but neither has been compile-verified in this environment. Microsoft authentication, app-scoped auth caching, and Keychain behavior also require an end-to-end Mac test. Sign and notarize before public distribution. No CI or GitHub Actions are used.
