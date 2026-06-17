/// TCP 리스너 — 카드 단말기로부터 실시간 결제 전문을 수신한다.
///
/// 동작 방식:
///   1. TcpListener::bind → 포트 38701 대기
///   2. 연결마다 IP 화이트리스트 검사 (차단 시 즉시 drop)
///   3. 연결마다 tokio task 생성 → handle_connection
///   4. 수신 바이트를 버퍼에 누적 → LLLL{payload} 프레임 단위로 추출
///   5. parser::parse_frame → QueueMessage 변환
///   6. DedupChecker → 중복 확인 → Producer::publish
///
/// TCP는 스트림이므로 한 번의 read()에 여러 프레임이 오거나
/// 한 프레임이 여러 번에 나뉘어 올 수 있다.
/// extract_frame()이 이를 처리한다.
use std::net::IpAddr;
use std::sync::Arc;

use anyhow::Result;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;

use crate::receiver::AppState;
use crate::telegram::Notifier;

/// TCP 리스너를 시작한다.
///
/// # Arguments
/// - `addr`        : 바인딩 주소 (예: "0.0.0.0:38701")
/// - `allowed_ips` : 허용 IP 목록. 비어있으면 모든 IP 허용.
/// - `state`       : DedupChecker + Producer 공유 상태
/// - `notifier`    : 텔레그램 알림 (None이면 비활성화)
pub async fn start(
    addr: &str,
    allowed_ips: Vec<IpAddr>,
    state: Arc<AppState>,
    notifier: Option<Arc<Notifier>>,
) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    let allowed_ips = Arc::new(allowed_ips);
    tracing::info!("TCP 리스너 시작: {addr}");

    loop {
        let (stream, peer_addr) = listener.accept().await?;
        let peer_ip = peer_addr.ip();

        // 화이트리스트가 비어있지 않고 IP가 목록에 없으면 차단
        // stream을 drop하면 TCP 연결이 자동으로 종료됨
        if !allowed_ips.is_empty() && !allowed_ips.contains(&peer_ip) {
            tracing::warn!(ip = %peer_ip, "허용되지 않은 IP — 연결 차단");
            continue;
        }

        tracing::info!(ip = %peer_ip, "TCP 연결 수락");

        let state = state.clone();
        let notifier = notifier.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, &state).await {
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
/// 연결이 살아있는 동안 루프를 돌며:
///   - 소켓에서 데이터를 읽어 버퍼에 누적
///   - 완전한 프레임이 모이면 파싱 → dedup → 발행
///   - EOF(상대방 연결 종료) 또는 소켓 오류 시 반환
async fn handle_connection(
    mut stream: tokio::net::TcpStream,
    state: &AppState,
) -> Result<()> {
    // 수신 버퍼 — 프레임이 여러 TCP 패킷에 걸쳐 올 때 누적
    let mut buf: Vec<u8> = Vec::with_capacity(4096);
    let mut tmp = [0u8; 4096];

    loop {
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            // EOF — 단말기가 연결을 정상 종료
            tracing::info!("TCP 연결 종료 (EOF)");
            return Ok(());
        }
        buf.extend_from_slice(&tmp[..n]);

        // 버퍼에 완전한 프레임이 하나 이상 있으면 꺼내서 처리
        // 한 번의 read()에 여러 프레임이 도착할 수 있으므로 while로 반복
        while let Some(frame) = extract_frame(&mut buf) {
            process_frame(&frame, state).await;
        }
    }
}

/// 파싱된 프레임을 dedup 체크 후 RabbitMQ에 발행한다.
async fn process_frame(frame: &[u8], state: &AppState) {
    let msg = match crate::parser::parse_frame(frame) {
        Ok(m) => m,
        Err(e) => {
            tracing::error!(error = ?e, "TCP 프레임 파싱 실패");
            return;
        }
    };

    match state.dedup.is_new(&msg.id).await {
        Ok(true) => {
            // 새 이벤트 — RabbitMQ로 발행
            if let Err(e) = state.producer.publish(&msg).await {
                tracing::error!(id = %msg.id, error = ?e, "TCP 이벤트 RabbitMQ 발행 실패");
            } else {
                tracing::info!(
                    id = %msg.id,
                    event_type = %msg.event_type,
                    "TCP 이벤트 발행 완료"
                );
            }
        }
        Ok(false) => {
            // 중복 — 무시
            tracing::debug!(id = %msg.id, "중복 TCP 이벤트 무시");
        }
        Err(e) => {
            // Redis 장애 시 dedup 없이 발행 (데이터 유실 방지 우선)
            tracing::warn!(error = ?e, id = %msg.id, "dedup 실패, 중복 체크 없이 발행");
            let _ = state.producer.publish(&msg).await;
        }
    }
}

/// 버퍼에서 LLLL{payload} 형식의 완전한 프레임 하나를 꺼낸다.
///
/// 완전한 프레임이 없으면 None을 반환하고 버퍼를 그대로 둔다.
fn extract_frame(buf: &mut Vec<u8>) -> Option<Vec<u8>> {
    // 길이 필드(4바이트) 최소 필요
    if buf.len() < 4 {
        return None;
    }

    // 첫 4바이트가 payload 길이
    let len_str = std::str::from_utf8(&buf[0..4]).ok()?;
    let payload_len: usize = len_str.trim().parse().ok()?;

    // LLLL(4) + payload 전체가 도착해야 추출 가능
    let frame_end = 4 + payload_len;
    if buf.len() < frame_end {
        return None;
    }

    // 프레임 추출 및 버퍼에서 제거
    let frame = buf[..frame_end].to_vec();
    buf.drain(..frame_end);
    Some(frame)
}
