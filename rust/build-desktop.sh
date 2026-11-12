#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

if ! command -v rustup >/dev/null 2>&1 || ! command -v cargo >/dev/null 2>&1; then
  echo "Install Rust with rustup first: https://rustup.rs" >&2
  exit 1
fi

rustup toolchain install nightly --profile minimal
if ! cargo tauri --help >/dev/null 2>&1; then
  cargo install tauri-cli --version '^2'
fi

echo "Building the native Minecraft 26.1 runtime for this host..."
cargo build --release --bin mindcraft
mkdir -p desktop/binaries
case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*)
    cp target/release/mindcraft.exe desktop/binaries/mindcraft.exe
    ;;
  *)
    cp target/release/mindcraft desktop/binaries/mindcraft
    chmod 755 desktop/binaries/mindcraft
    ;;
esac

echo "Bundling the Tauri desktop application for this host platform..."
cd desktop
cargo tauri build
echo "Desktop bundle output is under rust/desktop/target/release/bundle."
