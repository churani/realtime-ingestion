/// MySQL 소비자 — 배치 INSERT 모드.
///
/// 동작 방식:
///   메시지를 즉시 INSERT 하지 않고 배치를 쌓은 뒤 한 번에 처리한다.
///
///   flush 조건 (둘 중 먼저 충족되는 쪽):
///     1. 배치 크기(MYSQL_BATCH_SIZE) 도달
///     2. 배치 인터벌(MYSQL_BATCH_INTERVAL 초) 경과
///
///   INSERT … VALUES (r1),(r2),…,(rN) ON DUPLICATE KEY UPDATE
///   → 단일 왕복으로 N건 처리, 처리량 대폭 향상
///
/// ACK 전략:
///   INSERT 성공 → 배치 전체 ACK
///   INSERT 실패 → 배치 전체 NACK + requeue (텔레그램 알림)
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use futures::StreamExt;
use lapin::{
    message::Delivery,
    options::{BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicQosOptions},
    types::FieldTable,
    Connection, ConnectionProperties,
};
use sqlx::{MySqlPool, QueryBuilder};

use crate::models::QueueMessage;
use crate::telegram::Notifier;

pub async fn run(
    amqp_url: &str,
    queue_name: &str,
    pool: MySqlPool,
    notifier: Option<Arc<Notifier>>,
    batch_size: usize,
    batch_interval_secs: u64,
) -> Result<()> {
    let conn = Connection::connect(amqp_url, ConnectionProperties::default()).await?;
    let channel = conn.create_channel().await?;

    // prefetch: 배치 크기의 2배로 설정해 브로커로부터 충분히 미리 받아둠
    let prefetch = ((batch_size * 2) as u16).max(50);
    channel.basic_qos(prefetch, BasicQosOptions { global: false }).await?;

    let mut consumer = channel
        .basic_consume(
            queue_name,
            "mysql-consumer",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    tracing::info!(
        queue = %queue_name,
        batch_size,
        batch_interval_secs,
        "MySQL 소비자 시작 (배치 모드)"
    );

    let mut deliveries: Vec<Delivery> = Vec::with_capacity(batch_size);
    let mut rows: Vec<(String, String, String, i64, i64)> = Vec::with_capacity(batch_size);

    // 배치 인터벌 타이머 — 첫 tick은 즉시 발생하므로 미리 소비
    let mut ticker = tokio::time::interval(Duration::from_secs(batch_interval_secs));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;

    loop {
        tokio::select! {
            // ── 새 메시지 도착 ───────────────────────────────────────────
            item = consumer.next() => {
                match item {
                    None => break,
                    Some(Err(e)) => {
                        tracing::error!(error = ?e, "MySQL 소비자 채널 오류");
                        if let Some(n) = &notifier {
                            n.notify(&format!(
                                "🔴 <b>[MySQL 소비자]</b> 채널 오류\n<code>{e}</code>"
                            )).await;
                        }
                        return Err(e.into());
                    }
                    Some(Ok(delivery)) => {
                        match parse_delivery(&delivery.data) {
                            Ok(row) => {
                                rows.push(row);
                                deliveries.push(delivery);
                            }
                            Err(e) => {
                                // 파싱 실패 → 재큐 없이 버림 (깨진 메시지 무한루프 방지)
                                tracing::error!(error = ?e, "메시지 파싱 실패, 폐기");
                                let _ = delivery
                                    .nack(BasicNackOptions { requeue: false, multiple: false })
                                    .await;
                            }
                        }

                        // 배치 크기 도달 → 즉시 flush
                        if rows.len() >= batch_size {
                            flush(&mut deliveries, &mut rows, &pool, &notifier).await;
                        }
                    }
                }
            }

            // ── 배치 인터벌 경과 ─────────────────────────────────────────
            _ = ticker.tick() => {
                if !rows.is_empty() {
                    flush(&mut deliveries, &mut rows, &pool, &notifier).await;
                }
            }
        }
    }

    Ok(())
}

/// 누적된 배치를 MySQL에 단일 INSERT로 처리한다.
async fn flush(
    deliveries: &mut Vec<Delivery>,
    rows: &mut Vec<(String, String, String, i64, i64)>,
    pool: &MySqlPool,
    notifier: &Option<Arc<Notifier>>,
) {
    let count = rows.len();

    // INSERT ... VALUES (r1),(r2),... ON DUPLICATE KEY UPDATE
    let mut qb = QueryBuilder::new(
        "INSERT INTO events (id, event_type, payload, timestamp, received_at) ",
    );
    qb.push_values(rows.iter(), |mut b, (id, event_type, payload, ts, recv)| {
        b.push_bind(id)
         .push_bind(event_type)
         .push_bind(payload)
         .push_bind(ts)
         .push_bind(recv);
    });
    qb.push(" ON DUPLICATE KEY UPDATE received_at = VALUES(received_at)");

    match qb.build().execute(pool).await {
        Ok(_) => {
            tracing::info!(count, "MySQL 배치 INSERT 완료");
            for d in deliveries.drain(..) {
                let _ = d.ack(BasicAckOptions::default()).await;
            }
        }
        Err(e) => {
            tracing::error!(error = ?e, count, "MySQL 배치 INSERT 실패, 재큐잉");
            if let Some(n) = notifier {
                n.notify(&format!(
                    "⚠️ <b>[MySQL 소비자]</b> 배치 INSERT 실패 ({count}건)\n<code>{e}</code>"
                )).await;
            }
            for d in deliveries.drain(..) {
                let _ = d.nack(BasicNackOptions { requeue: true, multiple: false }).await;
            }
        }
    }

    rows.clear();
}

/// RabbitMQ 메시지 바이트를 파싱해 INSERT용 튜플로 변환한다.
fn parse_delivery(data: &[u8]) -> Result<(String, String, String, i64, i64)> {
    let msg: QueueMessage = serde_json::from_slice(data)?;
    let payload_str = serde_json::to_string(&msg.payload)?;
    Ok((msg.id, msg.event_type, payload_str, msg.timestamp, msg.received_at))
}
