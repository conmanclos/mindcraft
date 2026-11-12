# Native Rust implementation

The native implementation lives in `rust/` alongside the original JavaScript/Mineflayer application. Its target is Minecraft Java Edition **26.1**, pinned to Azalea **0.16.0+mc26.1**. It is a native Rust implementation, not a JavaScript translation layer.

The macOS desktop app is in `rust/desktop/`; it provides first-run setup, Keychain storage for the AI key, app-scoped Microsoft auth cache, and a one-click launcher for the bundled Rust runtime. See [rust/desktop/README.md](rust/desktop/README.md) for packaging and [rust/README.md](rust/README.md) for runtime setup and remaining parity work.

The runtime source currently includes a CLI, local dashboard/API, SQLite chat and goal persistence, OpenAI-compatible/Ollama, Anthropic and Gemini adapters, autonomy policy checks, an Azalea multi-client swarm, AI chat replies, and a specialist task queue. The gameplay integration has been updated to Azalea 0.16.0+mc26.1 APIs, including connection-failure diagnostics, non-blocking AI chat replies, and critical-health action gating. It still requires a successful native compile and live 26.1 server verification; do not treat it as tested until those checks are completed. No CI or GitHub Actions were added.
