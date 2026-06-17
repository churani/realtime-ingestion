/// 보안 유틸리티 모음.
///
/// 포함된 기능:
///   1. HTTP API 키 미들웨어
///   2. IP별 HTTP 요청 Rate Limiter
///   3. TCP 연결 수 추적 (전체 / IP별)
///   4. 카드번호·사업자번호 마스킹
use std::net::IpAddr;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use axum::{
    extract::{ConnectInfo, Request},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
};
use dashmap::DashMap;
use governor::{DefaultKeyedRateLimiter, Quota, RateLimiter};

// ── Rate Limiter ─────────────────────────────────────────────────────────────

pub type IpRateLimiter = DefaultKeyedRateLimiter<IpAddr>;

/// IP별 토큰버킷 Rate Limiter를 생성한다.
pub fn new_rate_limiter(per_second: u32, burst: u32) -> Arc<IpRateLimiter> {
    let quota = Quota::per_second(NonZeroU32::new(per_second).unwrap())
        .allow_burst(NonZeroU32::new(burst).unwrap());
    Arc::new(RateLimiter::keyed(quota))
}

// ── HTTP 미들웨어 ─────────────────────────────────────────────────────────────

/// 상수 시간 바이트 비교 — 타이밍 사이드채널 방지.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b.iter()).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// API 키 검증 미들웨어.
///
/// `X-API-Key` 헤더가 `expected`와 일치하지 않으면 401을 반환한다.
/// 항상 인증을 수행한다 — 비활성화 경로 없음.
pub async fn api_key_middleware(
    expected: Arc<String>,
    req: Request,
    next: Next,
) -> Response {
    let provided = req
        .headers()
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if !constant_time_eq(provided.as_bytes(), expected.as_bytes()) {
        tracing::warn!("HTTP API 키 인증 실패");
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    }
    next.run(req).await
}

/// IP별 Rate Limit 미들웨어.
///
/// `ConnectInfo<SocketAddr>`로 실제 클라이언트 IP를 추출하므로
/// `axum::serve`에 `into_make_service_with_connect_info::<SocketAddr>()`가 필요하다.
pub async fn rate_limit_middleware(
    limiter: Arc<IpRateLimiter>,
    req: Request,
    next: Next,
) -> Response {
    use std::net::{Ipv4Addr, SocketAddr};
    let ip = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ci| ci.0.ip())
        .unwrap_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED));

    if limiter.check_key(&ip).is_err() {
        tracing::warn!(ip = %ip, "HTTP rate limit 초과 — 요청 거부");
        return (StatusCode::TOO_MANY_REQUESTS, "rate limit exceeded").into_response();
    }
    next.run(req).await
}

// ── TCP 연결 추적기 ───────────────────────────────────────────────────────────

/// TCP 동시 연결 수를 추적한다.
///
/// `acquire(ip)` 호출 → 성공하면 `ConnectionGuard` 반환.
/// `ConnectionGuard`가 drop될 때 자동으로 카운터가 감소한다.
pub struct ConnectionTracker {
    total:      AtomicUsize,
    per_ip:     DashMap<IpAddr, usize>,
    max_total:  usize,
    max_per_ip: usize,
}

impl ConnectionTracker {
    pub fn new(max_total: usize, max_per_ip: usize) -> Arc<Self> {
        Arc::new(Self {
            total: AtomicUsize::new(0),
            per_ip: DashMap::new(),
            max_total,
            max_per_ip,
        })
    }

    /// 연결을 등록한다. 한도 초과 시 Err를 반환한다.
    ///
    /// per-IP 엔트리 락을 먼저 획득한 뒤 total을 증가시켜
    /// 같은 IP에서의 동시 접근 TOCTOU를 방지한다.
    pub fn acquire(self: &Arc<Self>, ip: IpAddr) -> Result<ConnectionGuard, &'static str> {
        // per-IP 락을 먼저 획득해 같은 IP에서의 TOCTOU 방지
        let mut entry = self.per_ip.entry(ip).or_insert(0);
        if *entry >= self.max_per_ip {
            return Err("IP당 최대 연결 수 초과");
        }

        // per-IP 락을 보유한 채로 전체 연결 수 확인
        let prev = self.total.fetch_add(1, Ordering::AcqRel);
        if prev >= self.max_total {
            self.total.fetch_sub(1, Ordering::AcqRel);
            return Err("전체 최대 연결 수 초과");
        }

        *entry += 1;
        drop(entry);

        Ok(ConnectionGuard { tracker: Arc::clone(self), ip })
    }

    fn release(&self, ip: IpAddr) {
        self.total.fetch_sub(1, Ordering::Relaxed);
        if let Some(mut count) = self.per_ip.get_mut(&ip) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                drop(count);
                self.per_ip.remove(&ip);
            }
        }
    }
}

/// 연결 추적 RAII 가드 — drop될 때 카운터를 자동으로 감소시킨다.
pub struct ConnectionGuard {
    tracker: Arc<ConnectionTracker>,
    ip:      IpAddr,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.tracker.release(self.ip);
    }
}

// ── 민감정보 마스킹 ───────────────────────────────────────────────────────────

/// 카드번호 마스킹: 앞 6자리 + *** + 뒤 4자리.
///
/// 이미 '*'를 포함하거나 길이가 너무 짧으면 원본을 그대로 반환한다.
/// 바이트 슬라이스 대신 문자(char) 단위로 처리해 멀티바이트 UTF-8 패닉을 방지한다.
pub fn mask_card_no(s: &str) -> String {
    let s = s.trim();
    let char_count = s.chars().count();
    if char_count < 10 || s.contains('*') {
        return s.to_string();
    }
    let masked = char_count.saturating_sub(10);
    let prefix: String = s.chars().take(6).collect();
    let suffix: String = s.chars().rev().take(4).collect::<String>().chars().rev().collect();
    format!("{}{}{}", prefix, "*".repeat(masked), suffix)
}

/// 사업자번호 마스킹: 앞 6자리만 표시.
pub fn mask_business_no(s: &str) -> String {
    let s = s.trim();
    if s.chars().count() <= 6 {
        return s.to_string();
    }
    let prefix: String = s.chars().take(6).collect();
    format!("{}****", prefix)
}
