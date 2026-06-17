use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    Json,
};

use crate::{
    dedup::DedupChecker,
    models::{ApiResponse, IncomingEvent, QueueMessage},
    producer::Producer,
};

/// Axum State로 공유되는 애플리케이션 공유 상태.
///
/// Arc<AppState>로 래핑해 모든 요청 핸들러가 소유권 없이 참조할 수 있다.
/// 각 필드는 내부적으로 Arc/ConnectionManager 기반이라
/// clone 비용이 매우 작다.
pub struct AppState {
    /// Redis 중복 체크기
    pub dedup: DedupChecker,

    /// RabbitMQ 발행자 (Channel 내부 Arc → clone 가능)
    pub producer: Producer,
}

/// POST /events — 이벤트 수신 핸들러
///
/// 처리 흐름:
///   1. JSON 파싱 (axum이 자동 처리, 실패 시 400)
///   2. Redis SET NX 로 중복 확인
///   3. 중복이면 409 반환
///   4. QueueMessage 구성 후 RabbitMQ 발행
///   5. 성공 시 202 반환
///
/// 202 Accepted를 쓰는 이유:
///   DB 저장은 소비자가 비동기로 처리하므로 "받았고 처리 예정"을 나타내는
///   202가 200보다 의미상 정확하다.
pub async fn handle_event(
    State(state): State<Arc<AppState>>,
    Json(event): Json<IncomingEvent>,
) -> impl IntoResponse {
    // ── Step 1: 중복 체크 ──────────────────────────────────────────────
    let is_new = match state.dedup.is_new(&event.id).await {
        Ok(v) => v,
        Err(e) => {
            // Redis 장애 시 — 중복 체크 불가 상태이므로 503 반환
            // 주의: 이 경우 이벤트를 드롭한다. 클라이언트가 재시도해야 한다.
            tracing::error!(event_id = %event.id, error = ?e, "Redis dedup 실패");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ApiResponse {
                    status: "error",
                    message: "dedup service unavailable".into(),
                }),
            );
        }
    };

    if !is_new {
        // 이미 처리한 이벤트 — 조용히 409 반환 (클라이언트가 재전송한 경우)
        tracing::debug!(event_id = %event.id, "중복 이벤트 — 드롭");
        return (
            StatusCode::CONFLICT,
            Json(ApiResponse {
                status: "duplicate",
                message: format!("event {} already processed", event.id),
            }),
        );
    }

    // ── Step 2: QueueMessage 생성 ──────────────────────────────────────
    // 서버 수신 시각 (unix ms)
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("시스템 시계 오류")
        .as_millis() as i64;

    let msg = QueueMessage {
        id: event.id.clone(),
        event_type: event.event_type,
        payload: event.payload,
        // 클라이언트가 타임스탬프를 안 보냈으면 서버 수신 시각 사용
        timestamp: event.timestamp.unwrap_or(now_ms),
        received_at: now_ms,
    };

    // ── Step 3: RabbitMQ 발행 ─────────────────────────────────────────
    if let Err(e) = state.producer.publish(&msg).await {
        // 발행 실패 시 Redis 키는 이미 설정된 상태.
        // 클라이언트가 재시도하면 중복으로 처리될 수 있음 — 이 트레이드오프를
        // 허용하거나, 더 정교한 2PC 구현이 필요한 경우 별도 처리 필요.
        tracing::error!(event_id = %event.id, error = ?e, "RabbitMQ 발행 실패");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse {
                status: "error",
                message: "failed to enqueue event".into(),
            }),
        );
    }

    tracing::info!(
        event_id = %event.id,
        event_type = %msg.event_type,
        "이벤트 수신 및 큐 적재 완료"
    );

    (
        StatusCode::ACCEPTED, // 202: 받았고 비동기 처리 예정
        Json(ApiResponse {
            status: "accepted",
            message: format!("event {} queued", event.id),
        }),
    )
}

/// GET /health — 로드밸런서·모니터링용 헬스체크
pub async fn health_check() -> impl IntoResponse {
    (StatusCode::OK, Json(serde_json::json!({ "status": "ok" })))
}
