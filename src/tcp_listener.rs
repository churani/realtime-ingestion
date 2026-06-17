/// TCP 리스너 — 카드 단말기로부터 실시간 결제 전문을 수신한다.
///
/// 동작 방식:
///   1. TcpListener::bind → 포트 38701 대기
///   2. 연결마다 IP 화이트리스트 검사 (차단 시 즉시 drop)
///   3. 전체/IP별 동시 연결 수 한도 검사 → 초과 시 거부
///   4. 연결마다 tokio task 생성 → handle_connection
///   5. 수신 바이트를 버퍼에 누적 → LLLL{payload} 프레임 단위로 추출
///   6. 프레임 크기 검사 → 초과 시 버퍼 폐기
///   7. parser::parse_frame → QueueMessage 변환
///   8. DedupChecker → 중복 확인 → Producer::publish
use std::net::IpAddr;
use std::sync::Arc;

use anyhow::Result;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;

use crate::receiver::AppState;
use crate::security::ConnectionTracker;
use crate::telegram::Notifier;

/// TCP 리스너를 시작한다.
pub async fn start(
    addr: &str,
    allowed_ips: Vec<IpAddr>,
    state: Arc<AppState>,
    notifier: Option<Arc<Notifier>>,
    max_connections: usize,
    max_conn_per_ip: usize,
    max_frame_bytes: usize,
) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    let allowed_ips = Arc::new(allowed_ips);
    let tracker = ConnectionTracker::new(max_connections, max_conn_per_ip);
    tracing::info!("TCP 리스너 시작: {addr}");

    loop {
        let (stream, peer_addr) = listener.accept().await?;
        let peer_ip = peer_addr.ip();

        // 화이트리스트 검사
        if !allowed_ips.is_empty() && !allowed_ips.contains(&peer_ip) {
            tracing::warn!(ip = %peer_ip, "허용되지 않은 IP — 연결 차단");
            continue;
        }

        // 연결 수 한도 검사 — 초과 시 거부
        let guard = match tracker.acquire(peer_ip) {
            Ok(g) => g,
            Err(reason) => {
                tracing::warn!(ip = %peer_ip, reason, "TCP 연결 거부");
                continue;
            }
        };

        tracing::info!(ip = %peer_ip, "TCP 연결 수락");

        let state    = state.clone();
        let notifier = notifier.clone();

        tokio::spawn(async move {
            let _guard = guard; // task 종료 시 자동으로 카운터 감소

            if let Err(e) = handle_connection(stream, state, max_frame_bytes).await {
                tracing::error!(ip = %peer_ip, error = ?e, "TCP 연결 처리 오류");
                if let Some(n) = &notifier {
                    n.notify(&format!(
                        "🔴 <b>[TCP 리스너]</b> 연결 오류 (IP: {})\n<code>{}</code>",
                        peer_ip, e
                    ))
                    .await;
                }
            }
        });
    }
}

/// 개별 TCP 연결 처리.
///
/// 소켓 읽기 루프와 프레임 처리를 분리한다:
///   - 소켓 읽기 → 프레임 추출 → tokio::spawn (즉시 반환)
///   - 처리(dedup + RabbitMQ)는 별도 태스크에서 병렬 실행
async fn handle_connection(
    mut stream: tokio::net::TcpStream,
    state: Arc<AppState>,
    max_frame_bytes: usize,
) -> Result<()> {
    // 동시 처리 중인 프레임 수를 제한 — 메모리 폭증 방지
    let semaphore = Arc::new(tokio::sync::Semaphore::new(500));

    let mut buf: Vec<u8> = Vec::with_capacity(4096);
    let mut tmp = [0u8; 4096];

    loop {
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            tracing::info!("TCP 연결 종료 (EOF)");
            return Ok(());
        }
        buf.extend_from_slice(&tmp[..n]);

        while let Some(frame) = extract_frame(&mut buf, max_frame_bytes) {
            let state   = state.clone();
            let permit  = semaphore.clone().acquire_owned().await?;

            tokio::spawn(async move {
                process_frame(&frame, &state).await;
                drop(permit);
            });
        }
    }
}

/// 파싱된 프레임을 dedup 체크 후 RabbitMQ에 발행한다.
async fn process_frame(frame: &[u8], state: &AppState) {
    let msg = match crate::parser::parse_frame(frame) {
        Ok(m)  => m,
        Err(e) => {
            tracing::error!(error = ?e, "TCP 프레임 파싱 실패");
            return;
        }
    };

    match state.dedup.is_new(&msg.id).await {
        Ok(true) => {
            if let Err(e) = state.producer.publish(&msg).await {
                tracing::error!(id = %msg.id, error = ?e, "TCP 이벤트 RabbitMQ 발행 실패");
            } else {
                tracing::info!(id = %msg.id, event_type = %msg.event_type, "TCP 이벤트 발행 완료");
            }
        }
        Ok(false) => {
            tracing::debug!(id = %msg.id, "중복 TCP 이벤트 무시");
        }
        Err(e) => {
            tracing::warn!(error = ?e, id = %msg.id, "dedup 실패, 중복 체크 없이 발행");
            let _ = state.producer.publish(&msg).await;
        }
    }
}

/// 버퍼에서 LLLL{payload} 형식의 완전한 프레임 하나를 꺼낸다.
///
/// 프레임 크기가 `max_frame_bytes`를 초과하면 버퍼 전체를 폐기하고 None을 반환한다.
fn extract_frame(buf: &mut Vec<u8>, max_frame_bytes: usize) -> Option<Vec<u8>> {
    if buf.len() < 4 {
        return None;
    }

    let len_str = std::str::from_utf8(&buf[0..4]).ok()?;
    let payload_len: usize = len_str.trim().parse().ok()?;

    // 비정상적으로 큰 프레임 — 버퍼를 폐기하고 연결을 계속 유지
    if payload_len > max_frame_bytes {
        tracing::warn!(payload_len, max_frame_bytes, "TCP 프레임 크기 초과 — 버퍼 폐기");
        buf.clear();
        return None;
    }

    let frame_end = 4 + payload_len;
    if buf.len() < frame_end {
        return None;
    }

    let frame = buf[..frame_end].to_vec();
    buf.drain(..frame_end);
    Some(frame)
}
