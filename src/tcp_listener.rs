/// TCP 리스너 — 카드 단말기로부터 실시간 결제 전문을 수신한다.
///
/// 요청-응답 흐름:
///   단말기 → 전문 송신 → 서버 수신·처리 → 응답 송신 → 단말기 다음 전문
///
///   응답을 보내지 않으면 단말기가 타임아웃 후 재전송하므로, 신규/중복 모두 응답한다.
///   msg_type: 0100(승인요청) → 0110(승인응답), 0200(취소요청) → 0210(취소응답)
///   response_code: "00" (정상)
use std::io::ErrorKind;
use std::net::IpAddr;
use std::sync::Arc;

use anyhow::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::receiver::AppState;
use crate::security::ConnectionTracker;
use crate::telegram::Notifier;

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

        if !allowed_ips.is_empty() && !allowed_ips.contains(&peer_ip) {
            tracing::warn!(ip = %peer_ip, "허용되지 않은 IP — 연결 차단");
            continue;
        }

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
            let _guard = guard;

            match handle_connection(stream, state, max_frame_bytes).await {
                Ok(()) => {}
                Err(e) => {
                    let kind = e.downcast_ref::<std::io::Error>().map(|e| e.kind());
                    match kind {
                        Some(ErrorKind::ConnectionReset | ErrorKind::ConnectionAborted) => {
                            tracing::warn!(ip = %peer_ip, "TCP 연결 강제 종료 (단말기 측)");
                        }
                        _ => {
                            tracing::error!(ip = %peer_ip, error = ?e, "TCP 연결 처리 오류");
                            if let Some(n) = &notifier {
                                n.notify(&format!(
                                    "🔴 <b>[TCP 리스너]</b> 연결 오류 (IP: {peer_ip}): {e}"
                                ))
                                .await;
                            }
                        }
                    }
                }
            }
        });
    }
}

/// 개별 TCP 연결 처리.
///
/// 프레임 단위로 순차 처리 (수신 → 처리 → 응답 송신).
/// 응답을 보내야 단말기가 재전송하지 않으므로 신규/중복 모두 응답한다.
async fn handle_connection(
    mut stream: tokio::net::TcpStream,
    state: Arc<AppState>,
    max_frame_bytes: usize,
) -> Result<()> {
    let mut buf: Vec<u8> = Vec::with_capacity(4096);
    let mut tmp = [0u8; 4096];
    let mut total_bytes: usize = 0;
    let mut frame_count: usize = 0;

    loop {
        let n = match tokio::time::timeout(
            tokio::time::Duration::from_secs(30),
            stream.read(&mut tmp),
        )
        .await
        {
            Ok(Ok(n))  => n,
            Ok(Err(e)) => return Err(e.into()),
            Err(_) => {
                tracing::warn!(total_bytes, frame_count, "TCP 연결 idle timeout (30초)");
                return Ok(());
            }
        };

        if n == 0 {
            if !buf.is_empty() {
                tracing::warn!(
                    pending_bytes = buf.len(),
                    total_bytes,
                    frame_count,
                    "TCP EOF: 미처리 버퍼 데이터 폐기 (불완전 프레임)"
                );
            } else {
                tracing::info!(total_bytes, frame_count, "TCP 연결 종료 (EOF)");
            }
            return Ok(());
        }

        total_bytes += n;
        buf.extend_from_slice(&tmp[..n]);

        while let Some((frame, had_llll)) = extract_frame(&mut buf, max_frame_bytes) {
            frame_count += 1;

            let response = process_frame(&frame, &state).await;

            // 신규/중복 모두 응답 — 보내지 않으면 단말기가 재전송
            let resp_bytes = if had_llll {
                let mut out = format!("{:04}", response.len()).into_bytes();
                out.extend_from_slice(&response);
                out
            } else {
                response
            };

            if let Err(e) = stream.write_all(&resp_bytes).await {
                tracing::warn!(error = ?e, "TCP 응답 전송 실패");
                return Err(e.into());
            }
        }

        // 데이터는 받았는데 프레임이 아직 불완전한 경우
        if buf.len() > 0 && frame_count == 0 {
            tracing::debug!(buffered = buf.len(), received = n, "TCP 데이터 수신 — 프레임 미완성, 대기 중");
        }
    }
}

/// 프레임을 처리하고 단말기에 돌려줄 응답 바이트를 반환한다.
///
/// 신규: dedup 등록 → RabbitMQ 발행 → 응답
/// 중복: 발행 없이 응답만 (단말기가 재전송하지 않도록)
async fn process_frame(frame: &[u8], state: &AppState) -> Vec<u8> {
    let msg = match crate::parser::parse_frame(frame) {
        Ok(m) => m,
        Err(e) => {
            let preview: String = frame.iter().take(16)
                .map(|b| format!("{b:02x}"))
                .collect::<Vec<_>>()
                .join(" ");
            tracing::error!(error = ?e, frame_len = frame.len(), preview, "TCP 프레임 파싱 실패");
            return build_response(frame, "");
        }
    };

    // 수신 데이터 로그
    tracing::info!(
        id            = %msg.id,
        event_type    = %msg.event_type,
        terminal_no   = %msg.payload["terminal_no"].as_str().unwrap_or(""),
        merchant_no   = %msg.payload["merchant_no"].as_str().unwrap_or(""),
        amount        = %msg.payload["amount"].as_str().unwrap_or(""),
        response_code = %msg.payload["response_code"].as_str().unwrap_or(""),
        cancel_flag   = %msg.payload["cancel_flag"].as_str().unwrap_or(""),
        approval_no   = %msg.payload["approval_no"].as_str().unwrap_or(""),
        approval_date = %msg.payload["approval_date"].as_str().unwrap_or(""),
        approval_time = %msg.payload["approval_time"].as_str().unwrap_or(""),
        "TCP 전문 수신"
    );

    let msg_type = msg.payload["msg_type"].as_str().unwrap_or("").to_string();

    match state.dedup.is_new(&msg.id).await {
        Ok(true) => {
            if let Err(e) = state.producer.publish(&msg).await {
                tracing::error!(id = %msg.id, error = ?e, "TCP 이벤트 RabbitMQ 발행 실패");
            } else {
                tracing::info!(id = %msg.id, event_type = %msg.event_type, "TCP 이벤트 발행 완료");
            }
        }
        Ok(false) => {
            tracing::info!(id = %msg.id, "중복 TCP 이벤트 — 응답만 송신");
        }
        Err(e) => {
            tracing::warn!(error = ?e, id = %msg.id, "dedup 실패, 이벤트 드롭 (Redis 장애)");
        }
    }

    build_response(frame, &msg_type)
}

/// 수신 프레임을 기반으로 응답 프레임을 생성한다.
///
/// - msg_type: 0100 → 0110, 0200 → 0210, 그 외 그대로
/// - response_code(offset 24, len 2): "00" (정상)
fn build_response(frame: &[u8], msg_type: &str) -> Vec<u8> {
    let mut resp = frame.to_vec();

    let resp_type = match msg_type {
        "0100" => "0110",
        "0200" => "0210",
        other  => other,
    };

    // msg_type 교체 (offset 8, len 4)
    if resp.len() >= 12 && !resp_type.is_empty() {
        resp[8..12].copy_from_slice(resp_type.as_bytes());
    }
    // response_code "00" (offset 24, len 2)
    if resp.len() >= 26 {
        resp[24..26].copy_from_slice(b"00");
    }

    resp
}

const FIXED_FRAME_SIZE: usize = 170;

/// 버퍼에서 완전한 페이로드 하나를 꺼낸다.
///
/// 반환: (payload, had_llll_header)
///   had_llll_header = true  → LLLL{payload} 포맷이었음 → 응답에도 LLLL 헤더 필요
///   had_llll_header = false → 고정 170바이트 포맷
fn extract_frame(buf: &mut Vec<u8>, max_frame_bytes: usize) -> Option<(Vec<u8>, bool)> {
    if buf.len() < 4 {
        return None;
    }

    if buf[0..4].iter().all(|b| b.is_ascii_digit()) {
        let len_str = std::str::from_utf8(&buf[0..4]).ok()?;
        let payload_len: usize = len_str.trim().parse().ok()?;

        if payload_len == 0 {
            buf.drain(..4);
            return None;
        }
        if payload_len > max_frame_bytes {
            tracing::warn!(payload_len, max_frame_bytes, "TCP 프레임 크기 초과 — 버퍼 폐기");
            buf.clear();
            return None;
        }
        let frame_end = 4 + payload_len;
        if buf.len() < frame_end {
            return None;
        }
        let payload = buf[4..frame_end].to_vec();
        buf.drain(..frame_end);
        return Some((payload, true));
    }

    // 고정 170바이트 포맷
    if buf.len() < FIXED_FRAME_SIZE {
        return None;
    }
    if FIXED_FRAME_SIZE > max_frame_bytes {
        tracing::warn!(FIXED_FRAME_SIZE, max_frame_bytes, "TCP 고정 프레임 크기 초과 — 버퍼 폐기");
        buf.clear();
        return None;
    }
    let payload = buf[..FIXED_FRAME_SIZE].to_vec();
    buf.drain(..FIXED_FRAME_SIZE);
    Some((payload, false))
}
