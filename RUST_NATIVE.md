# Native Rust implementation

The native implementation lives in `rust/` alongside the original JavaScript/Mineflayer application. It is pinned to Minecraft Java 1.21.1 via Azalea 0.10.3+mc1.21.1.

See [rust/README.md](rust/README.md) for setup, configuration, run instructions, implemented actions, and remaining parity work.

The current runtime includes a CLI, local dashboard/API, SQLite chat and goal persistence, OpenAI-compatible/Ollama, Anthropic and Gemini adapters, autonomy policy checks, an Azalea multi-client swarm, AI chat replies, and chat/stop/follow/look/goto/mine/place/hotbar commands with explicit approval or Autonomous-mode dispatch. It also has a durable specialist task queue with atomic role-based claims and completion tracking. Task workers are not yet independent Azalea clients, and the implementation is not feature-complete or verified in a live server. No CI or GitHub Actions were added.
