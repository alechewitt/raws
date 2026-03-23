#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=== raws development environment setup ==="
echo ""

# 1. Source .env and verify required variables
if [ -f prompt/.env ]; then
    source prompt/.env
    echo "[OK] Loaded prompt/.env"
else
    echo "[ERROR] prompt/.env not found. Copy prompt/.env.example to prompt/.env and fill in values."
    exit 1
fi

MISSING=0
if [ -z "${AWS_CLI_SRC:-}" ]; then
    echo "[ERROR] AWS_CLI_SRC is not set"
    MISSING=1
else
    echo "[OK] AWS_CLI_SRC=$AWS_CLI_SRC"
fi

if [ -z "${RAWS_TEST_ACCOUNT:-}" ]; then
    echo "[ERROR] RAWS_TEST_ACCOUNT is not set"
    MISSING=1
else
    echo "[OK] RAWS_TEST_ACCOUNT=$RAWS_TEST_ACCOUNT"
fi

if [ -z "${RAW_TEST_PROFILE:-}" ]; then
    echo "[ERROR] RAW_TEST_PROFILE is not set"
    MISSING=1
else
    echo "[OK] RAW_TEST_PROFILE=$RAW_TEST_PROFILE"
fi

if [ "$MISSING" -eq 1 ]; then
    echo ""
    echo "Fix the above errors in prompt/.env and re-run."
    exit 1
fi

echo ""

# 2. Check Rust toolchain
if ! command -v rustc &>/dev/null; then
    echo "[ERROR] rustc not found. Install Rust via: curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
    exit 1
fi

if ! command -v cargo &>/dev/null; then
    echo "[ERROR] cargo not found. Install Rust via: curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
    exit 1
fi

echo "[OK] rustc found: $(rustc --version)"
echo "[OK] cargo found: $(cargo --version)"

# 3. Check Rust version (need 1.70+)
RUST_VERSION=$(rustc --version | grep -oE '[0-9]+\.[0-9]+\.[0-9]+')
RUST_MAJOR=$(echo "$RUST_VERSION" | cut -d. -f1)
RUST_MINOR=$(echo "$RUST_VERSION" | cut -d. -f2)

if [ "$RUST_MAJOR" -lt 1 ] || ([ "$RUST_MAJOR" -eq 1 ] && [ "$RUST_MINOR" -lt 70 ]); then
    echo "[ERROR] Rust 1.70+ required, found $RUST_VERSION. Run: rustup update stable"
    exit 1
fi
echo "[OK] Rust version $RUST_VERSION (>= 1.70)"

echo ""

# 4. Verify botocore data directory
if [ -d "$AWS_CLI_SRC/botocore/data/" ]; then
    SERVICE_COUNT=$(ls "$AWS_CLI_SRC/botocore/data/" | wc -l | tr -d ' ')
    echo "[OK] Botocore data directory exists ($SERVICE_COUNT entries)"
else
    echo "[ERROR] $AWS_CLI_SRC/botocore/data/ does not exist"
    echo "       Ensure AWS_CLI_SRC points to the awscli/ subdirectory of the aws-cli repo"
    exit 1
fi

# Check if models are copied
if [ -d models/ ]; then
    MODEL_COUNT=$(ls models/ | wc -l | tr -d ' ')
    echo "[OK] models/ directory exists ($MODEL_COUNT entries)"
else
    echo "[WARN] models/ directory not found. Copy service models with:"
    echo "       source prompt/.env && cp -r \"\$AWS_CLI_SRC/botocore/data/\"* models/"
fi

echo ""

# 5. Build the project
echo "--- Building project ---"
if cargo build 2>&1; then
    echo "[OK] cargo build succeeded"
else
    echo "[ERROR] cargo build failed"
    exit 1
fi

echo ""

# 6. Run tests
echo "--- Running tests ---"
if cargo test 2>&1; then
    echo "[OK] cargo test succeeded"
else
    echo "[ERROR] cargo test failed"
    exit 1
fi

echo ""

# 7. Summary
echo "=== Environment Summary ==="
echo "  Project root:    $SCRIPT_DIR"
echo "  AWS CLI source:  $AWS_CLI_SRC"
echo "  Test account:    $RAWS_TEST_ACCOUNT"
echo "  Test profile:    $RAW_TEST_PROFILE"
echo "  Rust version:    $RUST_VERSION"
if [ -d models/ ]; then
    echo "  Models:          $MODEL_COUNT entries in models/"
fi
echo ""

# 8. Next steps
echo "=== Next Steps ==="
echo "  1. source prompt/.env"
echo "  2. cargo build            # compile"
echo "  3. cargo test             # run tests"
echo "  4. cargo clippy           # lint"
echo "  5. cargo run -- sts get-caller-identity --profile \$RAW_TEST_PROFILE"
echo ""
echo "Done!"
