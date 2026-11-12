# Native Rust implementation

The native implementation lives in `rust/` alongside the original JavaScript/Mineflayer application. Its target is Minecraft Java Edition **26.1**, pinned to Azalea **0.16.0+mc26.1**. It is a native Rust implementation, not a JavaScript translation layer.

See [rust/README.md](rust/README.md) for setup, configuration, run instructions, the 26.1 migration status, and remaining parity work.

The runtime source currently includes a CLI, local dashboard/API, SQLite chat and goal persistence, OpenAI-compatible/Ollama, Anthropic and Gemini adapters, autonomy policy checks, an Azalea multi-client swarm, AI chat replies, and a specialist task queue. Its gameplay integration was written against an older Azalea API and still requires migration and build/live-server verification against 26.1. No CI or GitHub Actions were added.
