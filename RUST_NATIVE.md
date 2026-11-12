# Native Rust runtime

The Rust implementation lives in `rust/` and is developed alongside the original JavaScript/Mineflayer application. It is pinned to Minecraft Java 1.21.1 via Azalea 0.10.3+mc1.21.1.

Start here: [rust/README.md](rust/README.md).

The native runtime currently provides a shared CLI/API/dashboard process, provider adapters, SQLite chat persistence, autonomy-policy enforcement, and an Azalea offline-mode connection with chat event logging. It does **not** yet claim parity with the legacy Mineflayer skill set; missing in-game execution adapters are listed explicitly in the Rust README.
