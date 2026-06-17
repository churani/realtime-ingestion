/// MySQL 소비자 — 트랜잭션 원본 이벤트 저장 담당.
///
/// 역할 분리 전략:
///   MySQL은 정규화된 트랜잭션 기록에 강하므로 원본 이벤트 레코드를 저장한다.
///   이후 결제 처리, 포인트 적립, 환불 등 row-level 트랜잭션 쿼리의 기준이 된다.
///
/// ACK 전략 (At-Least-Once):
///   DB 저장 성공 → ACK (브로커에서 메시지 삭제)
///   DB 저장 실패 → NACK + requeue=true (브로커가 메시지를 다시 큐에 넣음)
///   → ON DUPLICATE KEY UPDATE 로 재시도 시 멱등성 보장
use std::sync::Arc;

use anyhow::Result;
use futures::StreamExt;
use lapin::{
    options::{BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicQosOptions},
    types::FieldTable,
    Connection, ConnectionProperties,
};
use sqlx::MySqlPool;

use crate::models::QueueMessage;
use crate::telegram::Notifier;

/// MySQL 소비자 루프를 실행한다.
///
/// 연결이 끊기거나 치명적 오류가 발생하면 Err를 반환한다.
/// main.rs에서 `loop { run(...).await; sleep(5s) }` 패턴으로 자동 재시작한다.
pub async fn run(amqp_url: &str, queue_name: &str, pool: MySqlPool, notifier: Option<Arc<Notifier>>) -> Result<()> {
    // 소비자 전용 AMQP 연결 — 발행자와 연결을 분리하는 것이 lapin 권장 패턴
    // (연결 하나에 여러 채널을 열 수 있지만, 소비자는 전용 연결이 더 안전하다)
    let conn = Connection::connect(amqp_url, ConnectionProperties::default()).await?;
    let channel = conn.create_channel().await?;

    // Prefetch 설정: 소비자가 한 번에 미확인 상태로 받을 수 있는 최대 메시지 수
    // 너무 크면 소비자 OOM, 너무 작으면 브로커-소비자 왕복 지연이 생김
    // 100 req/s에서 DB 저장이 ~5ms라면 50개면 충분히 파이프라인됨
    channel
        .basic_qos(50, BasicQosOptions { global: false })
        .await?;

    // 소비자 등록 — "mysql-consumer" 태그로 RabbitMQ 관리 UI에서 식별 가능
    let mut consumer = channel
        .basic_consume(
            queue_name,
            "mysql-consumer",               // consumer tag
            BasicConsumeOptions::default(), // auto_ack=false (수동 ACK)
            FieldTable::default(),
        )
        .await?;

    tracing::info!(queue = %queue_name, "MySQL 소비자 시작");

    // Delivery 스트림에서 메시지를 하나씩 꺼내 처리
    while let Some(delivery) = consumer.next().await {
        let delivery = match delivery {
            Ok(d) => d,
            Err(e) => {
                // 채널/연결 레벨 오류 — 루프를 탈출해 재연결 유도
                tracing::error!(error = ?e, "MySQL 소비자 채널 오류");
                if let Some(n) = &notifier {
                    n.notify(&format!("🔴 <b>[MySQL 소비자]</b> 채널 오류\n<code>{e}</code>")).await;
                }
                return Err(e.into());
            }
        };

        match insert_event(&delivery.data, &pool).await {
            Ok(_) => {
                // 저장 성공 → 브로커에 ACK 전송 → 브로커가 큐에서 메시지 삭제
                delivery.ack(BasicAckOptions::default()).await?;
                tracing::debug!("MySQL ACK 완료");
            }
            Err(e) => {
                // 저장 실패 → NACK + 재큐잉
                // requeue=true: 브로커가 메시지를 큐 앞쪽에 다시 넣음
                // 주의: 영구적 오류(잘못된 JSON 등)는 무한 재시도가 됨
                //       운영 환경에서는 재시도 횟수 제한 + DLQ 패턴 추가 필요
                tracing::error!(error = ?e, "MySQL insert 실패, 재큐잉");
                if let Some(n) = &notifier {
                    n.notify(&format!("⚠️ <b>[MySQL 소비자]</b> insert 실패 (재큐잉)\n<code>{e}</code>")).await;
                }
                delivery
                    .nack(BasicNackOptions {
                        requeue: true,
                        multiple: false,
                    })
                    .await?;
            }
        }
    }

    Ok(())
}

/// 메시지 바이트를 파싱해 MySQL `events` 테이블에 저장한다.
async fn insert_event(data: &[u8], pool: &MySqlPool) -> Result<()> {
    // JSON 역직렬화
    let msg: QueueMessage = serde_json::from_slice(data)?;

    // MySQL JSON 컬럼은 내부적으로 바이너리 포맷으로 저장되지만
    // sqlx 0.7에서 serde_json::Value → MySQL JSON 직접 바인딩이 불안정하므로
    // 문자열로 직렬화한 뒤 바인딩한다 (MySQL이 알아서 JSON 유효성 검사)
    let payload_str = serde_json::to_string(&msg.payload)?;

    sqlx::query(
        r#"
        INSERT INTO events
            (id, event_type, payload, timestamp, received_at)
        VALUES
            (?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE
            -- 같은 id가 재시도로 들어오면 received_at만 업데이트
            -- (payload, event_type은 첫 기록이 정본이므로 변경 안 함)
            received_at = VALUES(received_at)
        "#,
    )
    .bind(&msg.id)
    .bind(&msg.event_type)
    .bind(&payload_str) // TEXT → MySQL이 JSON으로 저장
    .bind(msg.timestamp)
    .bind(msg.received_at)
    .execute(pool)
    .await?;

    tracing::debug!(event_id = %msg.id, "MySQL events 저장 완료");
    Ok(())
}
