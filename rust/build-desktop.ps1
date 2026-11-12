$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $Root

if (-not (Get-Command rustup -ErrorAction SilentlyContinue) -or -not (Get-Command cargo -ErrorAction SilentlyContinue)) {
    throw "Install Rust with rustup first: https://rustup.rs"
}

rustup toolchain install nightly --profile minimal
cargo tauri --help *> $null
if ($LASTEXITCODE -ne 0) {
    cargo install tauri-cli --version "^2"
    if ($LASTEXITCODE -ne 0) { throw "Could not install the Tauri v2 CLI." }
}

Write-Host "Building the native Minecraft 26.1 runtime for Windows..."
cargo build --release --bin mindcraft
if ($LASTEXITCODE -ne 0) { throw "Native runtime build failed." }

$BinaryDir = Join-Path $Root "desktop/binaries"
New-Item -ItemType Directory -Force -Path $BinaryDir | Out-Null
Copy-Item (Join-Path $Root "target/release/mindcraft.exe") (Join-Path $BinaryDir "mindcraft.exe") -Force

Write-Host "Bundling the Tauri desktop application for Windows..."
Push-Location (Join-Path $Root "desktop")
try {
    cargo tauri build
    if ($LASTEXITCODE -ne 0) { throw "Tauri desktop bundle failed." }
}
finally {
    Pop-Location
}
Write-Host "Desktop bundle output is under rust/desktop/target/release/bundle."
