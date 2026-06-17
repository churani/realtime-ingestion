#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
BIN_NAME="realtime-ingestion"
RELEASE_BIN="$PROJECT_DIR/target/release/$BIN_NAME"
DEPLOY_DIR="/usr/local/bin"
LOG_DIR="/var/www/log/realtime-ingestion"

cd "$PROJECT_DIR"

echo "[1/3] 빌드 시작..."
cargo build --release

echo "[2/3] 로그 디렉토리 확인..."
mkdir -p "$LOG_DIR"

echo "[3/3] 바이너리 배포..."
sudo cp "$RELEASE_BIN" "$DEPLOY_DIR/$BIN_NAME"

echo ""
echo "완료: $DEPLOY_DIR/$BIN_NAME"
echo "실행: $BIN_NAME"
