use anyhow::Result;
use redis::{aio::ConnectionManager, Client};

/// Redis 기반 중복 이벤트 필터.
///
/// 핵심 원리:
///   SET key "1" NX EX <ttl>
///   - NX : 키가 없을 때만 SET 성공 (원자적 check-and-set)
///   - EX : TTL 초과 시 자동 삭제
///   → SET 성공 = 처음 본 이벤트 (새 이벤트)
///   → SET 실패 = 이미 처리했던 이벤트 (중복)
///
/// `ConnectionManager` 는 내부적으로 Arc<Mutex<Connection>> 구조.
/// clone() 이 싸고, 연결 끊기면 자동으로 재연결한다.
pub struct DedupChecker {
    /// redis-rs 제공 자동 재연결 연결 관리자
    manager: ConnectionManager,

    /// 중복 판단 유효 기간 (초). 이 시간이 지나면 같은 ID도 새 이벤트로 처리된다.
    ttl_secs: u64,
}

impl DedupChecker {
    /// Redis URL로 연결하고 `DedupChecker` 를 생성한다.
    ///
    /// `ConnectionManager::new` 가 내부적으로 첫 연결을 수립하므로 async가 필요하다.
    pub async fn new(redis_url: &str, ttl_secs: u64) -> Result<Self> {
        let client = Client::open(redis_url)?;

        // ConnectionManager: 첫 연결 + 이후 재연결 자동 처리
        let manager = ConnectionManager::new(client).await?;

        Ok(Self { manager, ttl_secs })
    }

    /// 이벤트 ID를 받아 새 이벤트인지 확인하고, 새 이벤트라면 Redis에 마킹한다.
    ///
    /// Returns:
    ///   - `Ok(true)`  : 새 이벤트 — 큐에 넣어야 한다
    ///   - `Ok(false)` : 중복 이벤트 — 버려야 한다
    ///   - `Err(_)`    : Redis 통신 실패
    pub async fn is_new(&self, event_id: &str) -> Result<bool> {
        // 방어적 길이 검증 — 호출자가 먼저 검증하더라도 Redis 키 크기 보장
        if event_id.is_empty() || event_id.len() > 512 {
            anyhow::bail!("event_id 길이 범위 초과: {}자 (1–512 허용)", event_id.len());
        }

        // clone()은 Arc 참조 카운트 증가뿐 — 연결 복사가 아님
        let mut conn = self.manager.clone();

        // 키 형식: "dedup:<event_id>"
        // 네임스페이스 접두사로 다른 Redis 키와 충돌을 방지
        let key = format!("dedup:{}", event_id);

        // SET key value NX EX ttl 을 직접 구성
        // redis-rs의 AsyncCommands::set_options 대신 raw cmd를 쓰는 이유:
        //   set_options은 NX+EX 동시 지정 API가 버전마다 다르므로
        //   raw cmd로 직접 구성하는 것이 가장 명확하다
        let result: Option<String> = redis::cmd("SET")
            .arg(&key)       // 키
            .arg("1")        // 값 (내용 무관, 존재 여부만 중요)
            .arg("NX")       // Not eXists: 키 없을 때만 SET
            .arg("EX")       // EXpire: 다음 인수를 TTL(초)로 해석
            .arg(self.ttl_secs)
            .query_async(&mut conn)
            .await?;

        // SET 성공 → Some("OK"), 이미 존재 → None
        Ok(result.is_some())
    }
}
