# realtime-ingestion 배포 가이드

## 아키텍처 개요

```
클라이언트 (HTTP)    ──┐
카드 단말기 (TCP)    ──┤→  Receiver  →  RabbitMQ Fanout Exchange
                        │     ↑                ├→ queue.transactions  →  MySQL Consumer
                        │   Redis              └→ queue.analytics     →  PostgreSQL Consumer
                        │  (중복 제거)
```

### 3개의 독립 프로세스

| 프로세스 | 바이너리 | 포트 | 역할 |
|---------|---------|------|------|
| Receiver | `realtime-ingestion` | HTTP 8081, TCP 38701 | 이벤트 수신 → RabbitMQ 발행 |
| MySQL Consumer | `consumer-mysql` | — | RabbitMQ → MySQL 배치 저장 |
| PostgreSQL Consumer | `consumer-postgres` | — | RabbitMQ → PostgreSQL 배치 저장 |

---

## 프로젝트 구조

```
realtime-ingestion/
├── Cargo.toml                    # 의존성 및 바이너리 타겟 정의
├── Cargo.lock
├── .env.example                  # 환경 변수 템플릿
├── build.sh                      # 빌드 & 배포 스크립트
│
├── src/
│   ├── main.rs                   # Receiver 바이너리 진입점 (HTTP + TCP 서버)
│   ├── lib.rs                    # 모듈 공개 선언
│   │
│   ├── config.rs                 # 환경 변수 로딩 및 설정 구조체
│   ├── models.rs                 # 공통 데이터 구조체 (IncomingEvent, QueueMessage)
│   ├── receiver.rs               # POST /events 핸들러
│   ├── tcp_listener.rs           # TCP 서버 (카드 단말기 연결)
│   ├── parser.rs                 # 카드 거래 프레임 파서 (LLLL 포맷, EUC-KR 디코딩)
│   ├── producer.rs               # RabbitMQ 메시지 발행
│   ├── dedup.rs                  # Redis 기반 이벤트 중복 제거
│   ├── security.rs               # API 키 인증, IP 레이트 리밋, TCP 연결 수 추적
│   ├── logger.rs                 # 시간별 파일 로테이션 로거
│   ├── telegram.rs               # Telegram 알림 (선택)
│   │
│   ├── bin/
│   │   ├── consumer_mysql.rs     # MySQL Consumer 바이너리 진입점
│   │   └── consumer_postgres.rs  # PostgreSQL Consumer 바이너리 진입점
│   │
│   └── consumer/
│       ├── mod.rs                # Consumer 모듈 선언
│       ├── mysql.rs              # RabbitMQ → MySQL 배치 INSERT
│       └── postgres.rs           # RabbitMQ → PostgreSQL 배치 INSERT
│
└── migrations/
    ├── mysql/
    │   └── 001_init.sql          # events 테이블 스키마
    └── postgres/
        └── 001_init.sql          # event_analytics 테이블 스키마
```

### 바이너리별 모듈 의존 관계

```
realtime-ingestion (main.rs)
├── config.rs        설정 로딩
├── logger.rs        로그 초기화
├── dedup.rs         Redis 연결
├── producer.rs      RabbitMQ 연결 및 발행
├── receiver.rs      HTTP /events 핸들러
│   ├── models.rs    IncomingEvent, QueueMessage
│   ├── dedup.rs     중복 확인
│   └── producer.rs  큐 발행
├── tcp_listener.rs  TCP 서버
│   ├── parser.rs    카드 프레임 파싱
│   ├── security.rs  IP 허용 목록, 연결 수 관리
│   └── producer.rs  큐 발행
└── security.rs      API 키 미들웨어, 레이트 리밋

consumer-mysql (bin/consumer_mysql.rs)
├── config.rs
├── logger.rs
└── consumer/mysql.rs
    └── models.rs    QueueMessage

consumer-postgres (bin/consumer_postgres.rs)
├── config.rs
├── logger.rs
└── consumer/postgres.rs
    └── models.rs    QueueMessage
```

---

## 사전 요구사항

### 시스템
- OS: Linux (Ubuntu 20.04+ 권장)
- Rust: 1.70+ (`rustup` 으로 설치)
- 디스크: 빌드용 1GB 이상 여유 공간

### 외부 서비스
- **Redis** — 이벤트 중복 제거 캐시
- **RabbitMQ** — 메시지 큐 (AMQP 0-9-1)
- **MySQL** — 카드 거래 저장 (선택)
- **PostgreSQL** — 이벤트 분석 저장 (선택)

---

## 환경 변수 설정

`.env.example`을 복사해 `/var/www/app/.env`에 저장합니다.

```bash
cp /opt/realtime-ingestion/.env.example /var/www/app/.env
```

### 필수 항목

```env
# HTTP 서버
SERVER_ADDR=127.0.0.1:8081

# TCP 서버 (카드 단말기)
TCP_ADDR=0.0.0.0:38701

# 인증 (반드시 설정)
EASYPOS_SECRET_KEY=your-secret-api-key-here

# Redis
REDIS_URL=redis://127.0.0.1:6379

# RabbitMQ
RABBITMQ_URL=amqp://user:pass@127.0.0.1:5672/%2F
```

### 데이터베이스

```env
# MySQL Consumer 사용 시
MYSQL_URL=mysql://user:pass@127.0.0.1:3306/events_db

# PostgreSQL Consumer 사용 시
POSTGRES_URL=postgres://user:pass@127.0.0.1:5432/analytics_db
```

### RabbitMQ 큐/익스체인지

```env
EXCHANGE_NAME=events
MYSQL_QUEUE=queue.transactions
POSTGRES_QUEUE=queue.analytics
```

### 배치 처리

```env
MYSQL_BATCH_SIZE=100
MYSQL_BATCH_INTERVAL=1

PGSQL_BATCH_SIZE=100
PGSQL_BATCH_INTERVAL=1
```

### 보안

```env
# HTTP
HTTP_RATE_PER_SECOND=100
HTTP_RATE_BURST=200
HTTP_MAX_BODY_BYTES=65536

# TCP
ALLOWED_IPS=                   # 비워두면 전체 허용, 예: 192.168.1.10,10.0.0.5
TCP_MAX_CONNECTIONS=1000
TCP_MAX_CONN_PER_IP=10
TCP_MAX_FRAME_BYTES=8192

# 중복 제거 TTL (초)
DEDUP_TTL_SECS=300
```

### 알림 (선택)

```env
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
```

### 로깅

```env
RUST_LOG=realtime_ingestion=info
```

---

## 데이터베이스 마이그레이션

바이너리 배포 전에 DB 스키마를 적용합니다.

```bash
# MySQL
mysql -u user -p events_db < /opt/realtime-ingestion/migrations/mysql/001_init.sql

# PostgreSQL
psql -U user -d analytics_db -f /opt/realtime-ingestion/migrations/postgres/001_init.sql
```

---

## 빌드 & 배포

```bash
cd /opt/realtime-ingestion

# 전체 빌드 (receiver + consumer-mysql + consumer-postgres)
./build.sh all

# 개별 빌드
./build.sh receiver    # Receiver만
./build.sh mysql       # MySQL Consumer만
./build.sh postgres    # PostgreSQL Consumer만
```

`build.sh`는 `cargo build --release` 후 바이너리를 `/usr/local/bin/`에 배치합니다.

빌드된 바이너리:
- `/usr/local/bin/realtime-ingestion`
- `/usr/local/bin/consumer-mysql`
- `/usr/local/bin/consumer-postgres`

로그 디렉토리도 자동 생성됩니다:
- `/var/www/log/realtime-ingestion/`
- `/var/www/log/consumer-mysql/`
- `/var/www/log/consumer-postgres/`

---

## 프로세스 실행

### systemd 서비스 등록

서비스 파일은 `systemd/` 디렉토리에 있습니다.

```bash
sudo cp /opt/realtime-ingestion/systemd/*.service /etc/systemd/system/
sudo systemctl daemon-reload
```

각 서비스 파일 내용:

**`realtime-ingestion.service`**

```ini
[Unit]
Description=Realtime Ingestion Receiver (HTTP :8081 + TCP :38701)
After=network.target redis.service rabbitmq-server.service

[Service]
Type=simple
User=www-data
Group=www-data
EnvironmentFile=/var/www/app/.env
ExecStartPre=+/bin/mkdir -p /var/www/log/realtime-ingestion
ExecStartPre=+/bin/chown -R www-data:www-data /var/www/log/realtime-ingestion
ExecStart=/usr/local/bin/realtime-ingestion
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal
SyslogIdentifier=realtime-ingestion

[Install]
WantedBy=multi-user.target
```

**`consumer-mysql.service`**

```ini
[Unit]
Description=Realtime Ingestion MySQL Consumer
After=network.target rabbitmq-server.service mysql.service

[Service]
Type=simple
User=www-data
Group=www-data
EnvironmentFile=/var/www/app/.env
ExecStartPre=+/bin/mkdir -p /var/www/log/consumer-mysql
ExecStartPre=+/bin/chown -R www-data:www-data /var/www/log/consumer-mysql
ExecStart=/usr/local/bin/consumer-mysql
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal
SyslogIdentifier=consumer-mysql

[Install]
WantedBy=multi-user.target
```

**`consumer-postgres.service`**

```ini
[Unit]
Description=Realtime Ingestion PostgreSQL Consumer
After=network.target rabbitmq-server.service postgresql.service

[Service]
Type=simple
User=www-data
Group=www-data
EnvironmentFile=/var/www/app/.env
ExecStartPre=+/bin/mkdir -p /var/www/log/consumer-postgres
ExecStartPre=+/bin/chown -R www-data:www-data /var/www/log/consumer-postgres
ExecStart=/usr/local/bin/consumer-postgres
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal
SyslogIdentifier=consumer-postgres

[Install]
WantedBy=multi-user.target
```

> `ExecStartPre`의 `+` 접두사: `User=www-data`여도 해당 명령은 root로 실행되어
> 로그 디렉토리 생성 및 권한 설정이 가능합니다.

---

## 서비스 관리

### 시작

```bash
sudo systemctl enable --now realtime-ingestion
sudo systemctl enable --now consumer-mysql
sudo systemctl enable --now consumer-postgres
```

### 상태 확인

```bash
sudo systemctl status realtime-ingestion
sudo systemctl status consumer-mysql
sudo systemctl status consumer-postgres
```

### 정지

```bash
sudo systemctl stop realtime-ingestion
sudo systemctl stop consumer-mysql
sudo systemctl stop consumer-postgres
```

### 재시작

```bash
sudo systemctl restart realtime-ingestion
sudo systemctl restart consumer-mysql
sudo systemctl restart consumer-postgres
```

### 자동 시작 해제

```bash
sudo systemctl disable realtime-ingestion
sudo systemctl disable consumer-mysql
sudo systemctl disable consumer-postgres
```

---

## 로그 확인

로그는 두 곳에 기록됩니다.

| 위치 | 형식 | 용도 |
|-----|------|------|
| `/var/www/log/<서비스>/연도/월/일/시.log` | 파일 (시간별 로테이션) | 장기 보관 |
| systemd journal | 바이너리 DB | 실시간 모니터링 |

### 파일 로그 (실시간)

```bash
# Receiver
tail -f /var/www/log/realtime-ingestion/$(date +%Y/%m/%d)/$(date +%H).log

# MySQL Consumer
tail -f /var/www/log/consumer-mysql/$(date +%Y/%m/%d)/$(date +%H).log

# PostgreSQL Consumer
tail -f /var/www/log/consumer-postgres/$(date +%Y/%m/%d)/$(date +%H).log
```

### 파일 로그 (날짜 지정)

```bash
# 특정 날짜/시간 로그 열기
cat /var/www/log/realtime-ingestion/2026/06/18/14.log
```

### journal 로그

```bash
# 실시간 스트리밍
journalctl -u realtime-ingestion -f
journalctl -u consumer-mysql -f
journalctl -u consumer-postgres -f

# 최근 N줄
journalctl -u realtime-ingestion -n 100

# 시간 범위 지정
journalctl -u realtime-ingestion --since "2026-06-18 14:00" --until "2026-06-18 15:00"

# 에러만
journalctl -u realtime-ingestion -p err

# 3개 서비스 동시에
journalctl -u realtime-ingestion -u consumer-mysql -u consumer-postgres -f
```

### 수동 실행 (테스트용)

```bash
/usr/local/bin/realtime-ingestion &
/usr/local/bin/consumer-mysql &
/usr/local/bin/consumer-postgres &
```

---

## 배포 후 확인

### 헬스체크

```bash
curl http://localhost:8081/health
# → 200 OK
```

### 이벤트 수신 테스트

```bash
curl -X POST http://localhost:8081/events \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-secret-api-key-here" \
  -d '{"id":"test-001","event_type":"purchase","payload":{"amount":50000}}'
# → 202 Accepted (정상)
# → 409 Conflict (중복)
# → 401 Unauthorized (API 키 오류)
```

---

## 업데이트 배포

```bash
cd /opt/realtime-ingestion
git pull

# 빌드 (실행 중인 프로세스를 자동으로 종료하고 재배포)
sudo ./build.sh all

# systemd로 재시작
sudo systemctl restart realtime-ingestion
sudo systemctl restart consumer-mysql
sudo systemctl restart consumer-postgres
```

---

## HTTP API

| 메서드 | 경로 | 인증 | 설명 |
|-------|------|------|------|
| `POST` | `/events` | X-API-Key 헤더 필수 | 이벤트 수신 |
| `GET` | `/health` | 불필요 | 헬스체크 |

### 요청 본문 (`POST /events`)

```json
{
  "id": "event-unique-id",
  "event_type": "purchase",
  "payload": { "amount": 50000, "store_id": "S001" },
  "timestamp": 1718617200000
}
```

| 필드 | 타입 | 필수 | 설명 |
|-----|------|------|------|
| `id` | string | ✅ | 이벤트 고유 ID (중복 제거 키) |
| `event_type` | string | ✅ | 이벤트 유형 |
| `payload` | object | ✅ | 임의 JSON 데이터 |
| `timestamp` | number | — | Unix 밀리초; 생략 시 서버 시각 |

### 응답 코드

| 코드 | 의미 |
|-----|------|
| 202 | 정상 수신, 큐 발행 완료 |
| 401 | API 키 없음 또는 불일치 |
| 409 | 중복 이벤트 (동일 ID, TTL 내 재수신) |
| 413 | 요청 본문 크기 초과 |
| 422 | 유효하지 않은 JSON 또는 필드 오류 |
| 429 | Rate limit 초과 |

---

## TCP 카드 단말기 프로토콜

단말기는 `LLLL{payload}` 포맷으로 데이터를 전송합니다.

- `LLLL` — 4자리 십진수 payload 길이
- `payload` — EUC-KR 인코딩된 고정 길이 카드 거래 데이터

---

## 트러블슈팅

### Receiver 기동 실패

```bash
journalctl -u realtime-ingestion -n 50
```

- `EASYPOS_SECRET_KEY` 미설정 → `.env` 확인
- Redis 연결 실패 → `REDIS_URL` 및 Redis 상태 확인
- RabbitMQ 연결 실패 → `RABBITMQ_URL` 및 RabbitMQ 상태 확인
- 포트 충돌 → `ss -tlnp | grep 8081` 또는 `ss -tlnp | grep 38701`

### Consumer 메시지 처리 안 됨

```bash
# RabbitMQ 큐 상태 확인 (rabbitmqadmin 설치 필요)
rabbitmqadmin list queues name messages consumers
```

- `MYSQL_URL` / `POSTGRES_URL` 미설정 → Consumer 비활성화됨
- Exchange/Queue 이름 불일치 → `EXCHANGE_NAME`, `MYSQL_QUEUE`, `POSTGRES_QUEUE` 확인

### 중복 이벤트가 처리됨

- `DEDUP_TTL_SECS` 증가 검토
- Redis 연결 상태 확인: `redis-cli ping`

### 로그 파일이 생성 안 됨

```bash
ls -ld /var/www/log/
chown -R www-data:www-data /var/www/log/
```
