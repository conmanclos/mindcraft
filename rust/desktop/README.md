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

The release packager must build the Rust runtime for the target Mac architecture and copy it to `binaries/mindcraft` before invoking Tauri bundling:

```sh
cd rust
cargo build --release --bin mindcraft
mkdir -p desktop/binaries
cp target/release/mindcraft desktop/binaries/mindcraft
cd desktop
cargo tauri build --bundles app,dmg
```

The resulting app bundles the native runtime as a resource. Apple Silicon and Intel distribution should be built separately unless a verified universal2 runtime is produced. Sign and notarize the app before public distribution.

## Current development status

This is the initial desktop shell and setup workflow. It still requires integration testing on macOS, a release-packaging script, app signing/notarization, and a verified 26.1-compatible Rust runtime. The existing Azalea gameplay code must be migrated to Azalea 0.16 before the app can truthfully promise a successful in-game launch. Microsoft authentication and Keychain behavior also require an end-to-end Mac test. No CI or GitHub Actions are used.
