#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
DEPLOY_DIR="/usr/local/bin"
SYSTEMD_DIR="/etc/systemd/system"
RUST_USER="churani"
RUST_HOME="/home/churani"

SERVICES_ALL=(realtime-ingestion consumer-mysql consumer-postgres)

cd "$PROJECT_DIR"

# ── 헬퍼 함수 ────────────────────────────────────────────────────────────────

# 서비스 목록 반환 (TARGET 기준)
services_for() {
    case "$1" in
        receiver) echo "realtime-ingestion" ;;
        mysql)    echo "consumer-mysql" ;;
        postgres) echo "consumer-postgres" ;;
        all)      echo "${SERVICES_ALL[@]}" ;;
    esac
}

# 바이너리를 /usr/local/bin 에 원자 교체 배포
deploy_bin() {
    local bin="$1" dest="$2" desc="$3"
    cp "$PROJECT_DIR/target/release/$bin" "$DEPLOY_DIR/$dest.new"
    mv "$DEPLOY_DIR/$dest.new" "$DEPLOY_DIR/$dest"
    echo "  ✓ $DEPLOY_DIR/$dest ($desc)"
}

# systemd 서비스 파일 설치 및 재시작
install_service() {
    local svc="$1"
    local src="$PROJECT_DIR/systemd/$svc.service"
    if [[ ! -f "$src" ]]; then
        echo "  [경고] 서비스 파일 없음: $src — 건너뜀"
        return
    fi
    cp "$src" "$SYSTEMD_DIR/$svc.service"
    systemctl daemon-reload
    systemctl enable "$svc" 2>/dev/null || true
    systemctl restart "$svc"
    echo "  ✓ systemd 재시작: $svc"
}

# receiver 헬스체크 (최대 10초 대기)
health_check() {
    local url="http://127.0.0.1:8081/health"
    echo ""
    echo "[헬스체크] $url"
    for i in $(seq 1 10); do
        if curl -sf "$url" > /dev/null 2>&1; then
            echo "  ✓ 정상 응답 (${i}초)"
            return 0
        fi
        sleep 1
    done
    echo "  ✗ 헬스체크 실패 — 로그 확인:"
    echo "    journalctl -u realtime-ingestion -n 30"
    return 1
}

# sudo로 실행됐을 때 빌드 단계만 churani 사용자로 재실행
# (rustup/cargo는 churani 홈에 설치되어 있어 root로는 찾을 수 없음)
run_build_as_user() {
    echo "[빌드] root 감지 → $RUST_USER 사용자로 빌드..."
    sudo -u "$RUST_USER" \
        HOME="$RUST_HOME" \
        PATH="$RUST_HOME/.cargo/bin:$RUST_HOME/.rustup/toolchains/stable-x86_64-unknown-linux-gnu/bin:/usr/local/bin:/usr/bin:/bin" \
        bash "$0" --build-only "$1"
}

# ── 내부 빌드 전용 경로 (churani 사용자로 재호출됨) ───────────────────────────
if [[ "${1:-}" == "--build-only" ]]; then
    TARGET="${2:-all}"
    case "$TARGET" in
        receiver) cargo build --release --bin "realtime-ingestion" ;;
        mysql)    cargo build --release --bin "consumer_mysql" ;;
        postgres) cargo build --release --bin "consumer_postgres" ;;
        all)
            cargo build --release \
                --bin "realtime-ingestion" \
                --bin "consumer_mysql" \
                --bin "consumer_postgres"
            ;;
    esac
    exit 0
fi

# ── 명령어 파싱 ───────────────────────────────────────────────────────────────
CMD="${1:-all}"
TARGET="${2:-all}"

# CMD가 서비스 타겟(all/receiver/mysql/postgres)인 경우 → 기존 동작 (build+deploy+start)
if [[ "$CMD" =~ ^(all|receiver|mysql|postgres)$ ]]; then
    TARGET="$CMD"
    CMD="all"  # build + deploy + start
fi

case "$CMD" in
    all)
        # 빌드
        if [[ "$(id -u)" -eq 0 ]]; then
            run_build_as_user "$TARGET"
        else
            bash "$0" --build-only "$TARGET"
            exit 0
        fi

        echo ""
        echo "[1] 로그 디렉토리 확인..."
        mkdir -p /var/www/log/realtime-ingestion /var/www/log/consumer-mysql /var/www/log/consumer-postgres
        chown -R www-data:www-data /var/www/log/realtime-ingestion /var/www/log/consumer-mysql /var/www/log/consumer-postgres

        echo "[2] 배포..."
        case "$TARGET" in
            receiver) deploy_bin "realtime-ingestion" "realtime-ingestion" "수신기 TCP:38701 + HTTP:8081" ;;
            mysql)    deploy_bin "consumer_mysql"     "consumer-mysql"     "MySQL 소비자" ;;
            postgres) deploy_bin "consumer_postgres"  "consumer-postgres"  "PostgreSQL 소비자" ;;
            all)
                deploy_bin "realtime-ingestion" "realtime-ingestion" "수신기 TCP:38701 + HTTP:8081"
                deploy_bin "consumer_mysql"     "consumer-mysql"     "MySQL 소비자"
                deploy_bin "consumer_postgres"  "consumer-postgres"  "PostgreSQL 소비자"
                ;;
        esac

        echo "[3] systemd 서비스 시작..."
        for svc in $(services_for "$TARGET"); do
            install_service "$svc"
        done

        if [[ "$TARGET" == "all" || "$TARGET" == "receiver" ]]; then
            health_check
        fi
        ;;

    deploy)
        # 빌드 없이 target/release 바이너리만 배포
        [[ "$(id -u)" -ne 0 ]] && { echo "deploy는 sudo로 실행하세요."; exit 1; }
        echo "[배포] $TARGET..."
        case "$TARGET" in
            receiver) deploy_bin "realtime-ingestion" "realtime-ingestion" "수신기 TCP:38701 + HTTP:8081" ;;
            mysql)    deploy_bin "consumer_mysql"     "consumer-mysql"     "MySQL 소비자" ;;
            postgres) deploy_bin "consumer_postgres"  "consumer-postgres"  "PostgreSQL 소비자" ;;
            all)
                deploy_bin "realtime-ingestion" "realtime-ingestion" "수신기 TCP:38701 + HTTP:8081"
                deploy_bin "consumer_mysql"     "consumer-mysql"     "MySQL 소비자"
                deploy_bin "consumer_postgres"  "consumer-postgres"  "PostgreSQL 소비자"
                ;;
        esac
        ;;

    start)
        [[ "$(id -u)" -ne 0 ]] && { echo "start는 sudo로 실행하세요."; exit 1; }
        echo "[시작] $TARGET..."
        for svc in $(services_for "$TARGET"); do
            systemctl start "$svc" && echo "  ✓ $svc 시작"
        done
        if [[ "$TARGET" == "all" || "$TARGET" == "receiver" ]]; then
            health_check
        fi
        ;;

    restart)
        [[ "$(id -u)" -ne 0 ]] && { echo "restart는 sudo로 실행하세요."; exit 1; }
        echo "[재시작] $TARGET..."
        for svc in $(services_for "$TARGET"); do
            systemctl restart "$svc" && echo "  ✓ $svc 재시작"
        done
        if [[ "$TARGET" == "all" || "$TARGET" == "receiver" ]]; then
            health_check
        fi
        ;;

    stop)
        [[ "$(id -u)" -ne 0 ]] && { echo "stop은 sudo로 실행하세요."; exit 1; }
        echo "[정지] $TARGET..."
        for svc in $(services_for "$TARGET"); do
            systemctl stop "$svc" && echo "  ✓ $svc 정지"
        done
        ;;

    *)
        cat <<EOF
사용법: $0 <명령> [대상]

명령:
  all      [대상]   빌드 + 배포 + systemd 시작  (기본값)
  deploy   [대상]   배포만 (빌드 생략, sudo 필요)
  start    [대상]   서비스 시작 (sudo 필요)
  restart  [대상]   서비스 재시작 (sudo 필요)
  stop     [대상]   서비스 정지 (sudo 필요)

대상:
  all       모든 서비스  (기본값)
  receiver  수신기만     (realtime-ingestion)
  mysql     MySQL Consumer만
  postgres  PostgreSQL Consumer만

예시:
  sudo ./build.sh all
  sudo ./build.sh all receiver
  sudo ./build.sh deploy all
  sudo ./build.sh restart receiver
  sudo ./build.sh stop mysql
EOF
        exit 1
        ;;
esac

echo ""
echo "=== 완료 ==="
