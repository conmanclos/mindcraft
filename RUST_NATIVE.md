# Native Rust implementation

The native implementation lives in `rust/` alongside the original JavaScript/Mineflayer application. Its target is Minecraft Java Edition **26.1**, pinned to Azalea **0.16.0+mc26.1**. It is a native Rust implementation, not a JavaScript translation layer.

See [rust/README.md](rust/README.md) for setup, configuration, run instructions, the 26.1 migration status, and remaining parity work.

The runtime source includes a CLI, local dashboard/API, SQLite chat and goal persistence, OpenAI-compatible/Ollama, Anthropic and Gemini adapters, autonomy policy checks, an Azalea multi-client swarm, AI chat replies, crosshair-gated block actions, and a renewable specialist task queue. Core gameplay calls have been updated against Azalea 0.16.0; the branch still requires a real compile and live 26.1 server verification. No CI or GitHub Actions were added.
