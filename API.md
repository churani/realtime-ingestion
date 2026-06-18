# realtime-ingestion 연동 가이드

이 서버로 데이터를 전송하는 상대방 서버·단말기를 위한 참고 문서입니다.

---

## 엔드포인트 요약

| 채널 | 프로토콜 | 주소 | 용도 |
|-----|---------|------|------|
| HTTP | POST | `http://<host>:8081/events` | 일반 이벤트 전송 |
| HTTP | GET | `http://<host>:8081/health` | 헬스체크 |
| TCP | TCP | `<host>:38701` | 카드 단말기 결제 전문 |

---

## HTTP API

### 인증

모든 `POST /events` 요청에 `X-API-Key` 헤더를 포함해야 합니다.

```
X-API-Key: {발급받은 API 키}
```

키가 없거나 틀리면 `401 Unauthorized`를 반환합니다.

---

### POST /events — 이벤트 전송

#### 요청

```
POST /events HTTP/1.1
Host: <host>:8081
Content-Type: application/json
X-API-Key: {API 키}
```

**요청 바디**

```json
{
  "id": "order-20260618-001",
  "event_type": "purchase",
  "payload": {
    "amount": 50000,
    "store_id": "S001",
    "item": "coffee"
  },
  "timestamp": 1750204800000
}
```

| 필드 | 타입 | 필수 | 제약 | 설명 |
|-----|------|:----:|------|------|
| `id` | string | ✅ | 1~128자, `a-z A-Z 0-9 - _` 만 허용 | 이벤트 고유 ID. 중복 제거 기준. |
| `event_type` | string | ✅ | 1~64자 | 이벤트 종류 (예: `purchase`, `scan`, `checkin`) |
| `payload` | object | ✅ | 64KB 이하 | 자유 형식 JSON. 어떤 필드든 가능. |
| `timestamp` | number | — | Unix 밀리초 (ms) | 생략 시 서버 수신 시각으로 대체 |

#### 응답

**202 Accepted — 정상 수신**

```json
{
  "status": "accepted",
  "message": "event order-20260618-001 queued"
}
```

**409 Conflict — 중복 이벤트**

같은 `id`가 최근 5분 이내에 이미 수신된 경우입니다.  
재전송 시 이 응답이 오면 성공으로 처리해도 됩니다.

```json
{
  "status": "duplicate",
  "message": "event order-20260618-001 already processed"
}
```

**전체 응답 코드표**

| 코드 | 상황 | 권장 처리 |
|-----|------|---------|
| 202 | 정상 수신, 큐 적재 완료 | 성공 |
| 401 | API 키 없음 또는 불일치 | 키 확인 후 재시도 금지 |
| 409 | 중복 이벤트 (동일 ID, TTL 내 재수신) | 성공으로 처리 |
| 413 | 요청 바디 64KB 초과 | payload 크기 줄이기 |
| 422 | JSON 형식 오류 또는 필드 유효성 실패 | 요청 본문 확인 |
| 429 | IP당 요청 속도 초과 | 잠시 후 재시도 (backoff) |
| 503 | Redis 장애 (dedup 불가) | 잠시 후 재시도 |

---

#### 요청 예시

**curl**

```bash
curl -X POST http://<host>:8081/events \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-api-key" \
  -d '{
    "id": "order-20260618-001",
    "event_type": "purchase",
    "payload": { "amount": 50000, "store_id": "S001" },
    "timestamp": 1750204800000
  }'
```

**Python**

```python
import requests
import time

url = "http://<host>:8081/events"
headers = {
    "Content-Type": "application/json",
    "X-API-Key": "your-api-key",
}
body = {
    "id": "order-20260618-001",
    "event_type": "purchase",
    "payload": {"amount": 50000, "store_id": "S001"},
    "timestamp": int(time.time() * 1000),
}

resp = requests.post(url, json=body, headers=headers)
print(resp.status_code, resp.json())
```

**Node.js**

```js
const resp = await fetch("http://<host>:8081/events", {
  method: "POST",
  headers: {
    "Content-Type": "application/json",
    "X-API-Key": "your-api-key",
  },
  body: JSON.stringify({
    id: "order-20260618-001",
    event_type: "purchase",
    payload: { amount: 50000, store_id: "S001" },
    timestamp: Date.now(),
  }),
});
console.log(resp.status, await resp.json());
```

---

### GET /health — 헬스체크

인증 불필요. 서버가 살아있으면 `200 OK`를 반환합니다.

```bash
curl http://<host>:8081/health
# {"status":"ok"}
```

---

### 제한 사항

| 항목 | 기본값 | 비고 |
|-----|-------|------|
| 요청 바디 최대 크기 | 64 KB | 초과 시 413 |
| IP당 초당 요청 수 | 100 req/s | 초과 시 429 |
| IP당 버스트 | 200 req | 순간 최대 |
| 이벤트 중복 제거 TTL | 5분 | 같은 `id` 재전송 시 409 |

---

## TCP 프로토콜 (카드 단말기)

카드 단말기에서 결제 전문을 직접 TCP로 전송하는 경우에만 해당합니다.

### 연결 정보

```
호스트: <host>
포트:   38701
```

- 연결 후 전문을 연속으로 전송할 수 있습니다 (persistent connection).
- 30초 동안 데이터가 없으면 서버가 연결을 끊습니다.

### 연결 제한

| 항목 | 기본값 |
|-----|-------|
| 전체 최대 동시 연결 수 | 1,000 |
| IP당 최대 동시 연결 수 | 10 |
| 최대 프레임 크기 | 8 KB |

### 프레임 포맷

두 가지 포맷을 지원합니다.

#### 포맷 A — LLLL 헤더 포함 (권장)

```
┌──────────┬────────────────────────────────────────┐
│  LLLL    │  payload                               │
│ (4바이트) │ (LLLL이 지정한 길이)                    │
└──────────┴────────────────────────────────────────┘
```

- `LLLL`: ASCII 10진수 4자리, payload의 바이트 길이  
  예) payload가 170바이트이면 `"0170"`

#### 포맷 B — 고정 170바이트 (헤더 없음)

첫 4바이트가 ASCII 숫자가 아닌 경우 자동으로 고정 170바이트 포맷으로 인식합니다.

---

### payload 필드 레이아웃

인코딩: **EUC-KR**  
전체 길이: **170바이트** (+ filler 17바이트 = 187바이트)

| No. | 필드명 | Offset | 길이 | 설명 |
|----|--------|-------:|-----:|------|
| 1 | 가맹점구분 | 0 | 8 | merchant_type |
| 2 | Msg Type | 8 | 4 | 거래 유형 코드 |
| 3 | 거래고유번호 | 12 | 12 | **이벤트 ID** (중복 제거 기준) |
| 4 | 응답코드 | 24 | 2 | response_code |
| 5 | 단말기번호 | 26 | 7 | terminal_no |
| 6 | 할부개월수 | 33 | 2 | installment |
| 7 | 승인금액 | 35 | 10 | amount |
| 8 | 카드번호 | 45 | 16 | 저장 시 마스킹 처리 (앞 6 + *** + 뒤 4) |
| 9 | 승인번호 | 61 | 10 | approval_no |
| 10 | 승인일자 | 71 | 8 | YYYYMMDD |
| 11 | 승인시간 | 79 | 6 | HHMMSS |
| 12 | 원승인일자 | 85 | 8 | orig_date |
| 13 | 카드타입 | 93 | 1 | card_type |
| 14 | 발급사코드 | 94 | 3 | issuer_code |
| 15 | 발급사명 | 97 | 14 | EUC-KR 한글 |
| 16 | 매입사코드 | 111 | 3 | acquirer_code |
| 17 | 매입사명 | 114 | 14 | EUC-KR 한글 |
| 18 | 가맹점번호 | 128 | 14 | merchant_no |
| 19 | 사업자번호 | 142 | 10 | DB 테이블 라우팅 키. 저장 시 앞 6자리만 표시. |
| 20 | 취소구분 | 152 | 1 | cancel_flag |
| 21 | filler | 153 | 17 | 무시 |

> payload는 최소 153바이트 이상이어야 합니다. 미달 시 해당 전문은 폐기됩니다.

---

### 전송 예시 (Python)

```python
import socket

HOST = "<host>"
PORT = 38701

# 170바이트 EUC-KR 페이로드 준비 (예시)
payload = b"HYPOS001" + b"1100" + b"202406180001" + b" " * 146
assert len(payload) == 170

# LLLL 헤더 붙이기
header = f"{len(payload):04d}".encode("ascii")
frame  = header + payload

with socket.create_connection((HOST, PORT)) as s:
    s.sendall(frame)
```

---

## 이벤트 ID 설계 권장사항

중복 제거 TTL은 **5분**입니다. 같은 `id`를 5분 이내에 재전송하면 `409`를 받습니다.

- UUID v4 또는 `{시스템ID}-{타임스탬프ms}-{시퀀스}` 조합을 권장합니다.
- 재시도 시 동일 `id`를 그대로 사용하세요. 서버가 중복을 걸러냅니다.
- 5분을 넘긴 재시도는 새 이벤트로 처리됩니다.

**좋은 예**
```
order-S001-1750204800123-0001
scan-gate3-1750204800456
```

**나쁜 예** — 재시도마다 새 ID를 생성하면 중복 저장됩니다.
```
# 첫 시도
id: "550e8400-e29b-41d4-a716-446655440000"
# 재시도 (다른 ID 생성 — 중복 저장됨)
id: "7c9e6679-7425-40de-944b-e07fc1f90ae7"
```
