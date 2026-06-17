use serde::{Deserialize, Serialize};

/// HTTP POST /events 로 들어오는 요청 바디.
///
/// 클라이언트가 보내는 원본 이벤트 구조체.
/// `id` 가 중복 체크의 기준이 된다.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncomingEvent {
    /// 이벤트 고유 식별자 — Redis dedup 키로 사용됨 (필수)
    pub id: String,

    /// 이벤트 종류 (예: "purchase", "scan", "checkin")
    pub event_type: String,

    /// 이벤트 실제 데이터 — 자유 형식 JSON
    pub payload: serde_json::Value,

    /// 클라이언트 측 타임스탬프 (unix milliseconds).
    /// 없으면 서버 수신 시각으로 대체한다.
    pub timestamp: Option<i64>,
}

/// RabbitMQ 큐에 실제로 적재되는 메시지 구조체.
///
/// `IncomingEvent` 를 받아 서버 수신 시각(`received_at`)을 추가한 뒤
/// JSON 직렬화해서 큐에 넣는다.
/// 소비자(MySQL·PostgreSQL)는 이 구조체를 역직렬화해서 DB에 저장한다.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueMessage {
    /// 이벤트 고유 ID (IncomingEvent.id 그대로)
    pub id: String,

    /// 이벤트 종류
    pub event_type: String,

    /// 원본 페이로드
    pub payload: serde_json::Value,

    /// 클라이언트 타임스탬프 (unix ms). 없었으면 received_at 과 동일.
    pub timestamp: i64,

    /// 서버가 HTTP 요청을 수신한 시각 (unix ms)
    pub received_at: i64,
}

/// HTTP 응답에 사용하는 공통 JSON 바디
#[derive(Debug, Serialize)]
pub struct ApiResponse {
    /// "accepted" | "duplicate" | "error"
    pub status: &'static str,

    /// 사람이 읽을 수 있는 설명 메시지
    pub message: String,
}
