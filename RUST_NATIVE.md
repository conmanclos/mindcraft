# Native Rust implementation

The native implementation lives in `rust/` alongside the original JavaScript/Mineflayer application. It is pinned to Minecraft Java 1.21.1 via Azalea 0.10.3+mc1.21.1.

See [rust/README.md](rust/README.md) for setup, configuration, run instructions, current executable actions, and the remaining parity work.

The current runtime includes a CLI, local dashboard/API, SQLite chat persistence, OpenAI-compatible/Ollama, Anthropic and Gemini adapters, autonomy policy checks, an Azalea Minecraft client, AI chat replies, and explicitly approved chat/stop/goto/mine commands. It is not yet feature-complete or verified in a live server. No CI or GitHub Actions were added.
