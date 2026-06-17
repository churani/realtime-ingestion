#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
DEPLOY_DIR="/usr/local/bin"

cd "$PROJECT_DIR"

# 실행 중인 프로세스를 종료하고 바이너리를 배포한 뒤 재시작한다.
build_and_deploy() {
    local bin="$1"      # cargo 바이너리 이름 (target/release/ 기준)
    local dest="$2"     # 배포 파일명 (/usr/local/bin/ 기준)
    local desc="$3"     # 설명

    echo "▶ 빌드: $bin"
    cargo build --release --bin "$bin"

    # 실행 중이면 종료 (없으면 무시)
    if pgrep -x "$dest" > /dev/null 2>&1; then
        echo "  ↓ 실행 중인 $dest 종료..."
        sudo pkill -x "$dest" || true
        sleep 1
    fi

    sudo cp "target/release/$bin" "$DEPLOY_DIR/$dest"
    echo "  ✓ 배포 완료: $DEPLOY_DIR/$dest ($desc)"
}

TARGET="${1:-all}"

echo "[1] 로그 디렉토리 확인..."
mkdir -p /var/www/log/realtime-ingestion
mkdir -p /var/www/log/consumer-mysql
mkdir -p /var/www/log/consumer-postgres

echo "[2] 빌드 및 배포..."
case "$TARGET" in
    receiver)
        build_and_deploy "realtime-ingestion" "realtime-ingestion" "수신기 TCP:38701 + HTTP:8081"
        ;;
    mysql)
        build_and_deploy "consumer_mysql" "consumer-mysql" "MySQL 소비자"
        ;;
    postgres)
        build_and_deploy "consumer_postgres" "consumer-postgres" "PostgreSQL 소비자"
        ;;
    all)
        build_and_deploy "realtime-ingestion" "realtime-ingestion" "수신기 TCP:38701 + HTTP:8081"
        build_and_deploy "consumer_mysql"     "consumer-mysql"     "MySQL 소비자"
        build_and_deploy "consumer_postgres"  "consumer-postgres"  "PostgreSQL 소비자"
        ;;
    *)
        echo "사용법: $0 [receiver|mysql|postgres|all]"
        exit 1
        ;;
esac

echo ""
echo "완료. 수동으로 재시작하세요:"
case "$TARGET" in
    receiver) echo "  realtime-ingestion &" ;;
    mysql)    echo "  consumer-mysql &" ;;
    postgres) echo "  consumer-postgres &" ;;
    all)
        echo "  realtime-ingestion &"
        echo "  consumer-mysql &"
        echo "  consumer-postgres &"
        ;;
esac
