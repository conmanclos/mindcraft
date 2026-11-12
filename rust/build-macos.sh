# macOS build helper
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "This script must run on macOS." >&2
  exit 1
fi
if ! command -v rustup >/dev/null 2>&1 || ! command -v cargo >/dev/null 2>&1; then
  echo "Install Rust with rustup first: https://rustup.rs" >&2
  exit 1
fi
if ! xcode-select -p >/dev/null 2>&1; then
  echo "Install the Xcode Command Line Tools first: xcode-select --install" >&2
  exit 1
fi
rustup toolchain install nightly --profile minimal
if ! cargo tauri --help >/dev/null 2>&1; then
  echo "Installing the Tauri v2 CLI..."
  cargo install tauri-cli --version '^2'
fi

echo "Compiling and running native Rust unit tests..."
cargo test

echo "Building the native Minecraft 26.1 Rust runtime..."
cargo build --release --bin mindcraft
mkdir -p desktop/binaries
cp target/release/mindcraft desktop/binaries/mindcraft
chmod 755 desktop/binaries/mindcraft

echo "Bundling the macOS app and DMG..."
cd desktop
cargo tauri build --bundles app,dmg

echo "Build output is under rust/desktop/target/release/bundle."
echo "Sign and notarize before distributing outside your own Mac."
