/// PostgreSQL 소비자 — 배치 INSERT 모드.
///
/// MySQL 소비자와 동일한 배치 전략을 사용하되,
/// JSONB 바인딩과 ON CONFLICT DO NOTHING 처리가 다르다.
///
/// flush 조건 (둘 중 먼저 충족되는 쪽):
///   1. 배치 크기(PGSQL_BATCH_SIZE) 도달
///   2. 배치 인터벌(PGSQL_BATCH_INTERVAL 초) 경과
///
/// PostgreSQL UNNEST 방식으로 배치 INSERT:
///   INSERT INTO event_analytics (id, event_type, payload, timestamp, received_at)
///   SELECT * FROM UNNEST($1, $2, $3, $4, $5)
///   ON CONFLICT (id) DO NOTHING
///
///   UNNEST는 배열을 행으로 펼쳐주므로 QueryBuilder 없이도
///   단일 파라미터 바인딩으로 배치 처리가 가능하다.
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
use serde_json::Value;
use sqlx::PgPool;

use crate::models::QueueMessage;
use crate::telegram::Notifier;

pub async fn run(
    amqp_url: &str,
    queue_name: &str,
    pool: PgPool,
    notifier: Option<Arc<Notifier>>,
    batch_size: usize,
    batch_interval_secs: u64,
) -> Result<()> {
    let conn = Connection::connect(amqp_url, ConnectionProperties::default()).await?;
    let channel = conn.create_channel().await?;

    let prefetch = ((batch_size * 2) as u16).max(50);
    channel.basic_qos(prefetch, BasicQosOptions { global: false }).await?;

    let consumer_tag = format!(
        "postgres-consumer-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    );
    let mut consumer = channel
        .basic_consume(
            queue_name,
            &consumer_tag,
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    tracing::info!(
        queue = %queue_name,
        batch_size,
        batch_interval_secs,
        "PostgreSQL 소비자 시작 (배치 모드)"
    );

    let mut deliveries: Vec<Delivery> = Vec::with_capacity(batch_size);
    let mut ids:         Vec<String>  = Vec::with_capacity(batch_size);
    let mut event_types: Vec<String>  = Vec::with_capacity(batch_size);
    let mut payloads:    Vec<Value>   = Vec::with_capacity(batch_size);
    let mut timestamps:  Vec<i64>     = Vec::with_capacity(batch_size);
    let mut received_ats: Vec<i64>    = Vec::with_capacity(batch_size);

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
                        tracing::error!(error = ?e, "PostgreSQL 소비자 채널 오류");
                        if let Some(n) = &notifier {
                            n.notify("🔴 <b>[PostgreSQL 소비자]</b> 채널 오류 — 재시작 예정").await;
                        }
                        return Err(e.into());
                    }
                    Some(Ok(delivery)) => {
                        match serde_json::from_slice::<QueueMessage>(&delivery.data) {
                            Ok(msg) => {
                                ids.push(msg.id);
                                event_types.push(msg.event_type);
                                payloads.push(msg.payload);
                                timestamps.push(msg.timestamp);
                                received_ats.push(msg.received_at);
                                deliveries.push(delivery);
                            }
                            Err(e) => {
                                tracing::error!(error = ?e, "메시지 파싱 실패, 폐기");
                                let _ = delivery
                                    .nack(BasicNackOptions { requeue: false, multiple: false })
                                    .await;
                            }
                        }

                        if ids.len() >= batch_size {
                            flush(
                                &mut deliveries,
                                &mut ids, &mut event_types, &mut payloads,
                                &mut timestamps, &mut received_ats,
                                &pool, &notifier,
                            ).await;
                        }
                    }
                }
            }

            // ── 배치 인터벌 경과 ─────────────────────────────────────────
            _ = ticker.tick() => {
                if !ids.is_empty() {
                    flush(
                        &mut deliveries,
                        &mut ids, &mut event_types, &mut payloads,
                        &mut timestamps, &mut received_ats,
                        &pool, &notifier,
                    ).await;
                }
            }
        }
    }

    Ok(())
}

/// 누적된 배치를 UNNEST 방식으로 PostgreSQL에 단일 INSERT한다.
#[allow(clippy::too_many_arguments)]
async fn flush(
    deliveries:   &mut Vec<Delivery>,
    ids:          &mut Vec<String>,
    event_types:  &mut Vec<String>,
    payloads:     &mut Vec<Value>,
    timestamps:   &mut Vec<i64>,
    received_ats: &mut Vec<i64>,
    pool: &PgPool,
    notifier: &Option<Arc<Notifier>>,
) {
    let count = ids.len();

    // UNNEST로 배열을 행으로 펼쳐 단일 쿼리로 배치 INSERT
    // Json<&Value>: sqlx가 JSONB로 직렬화
    let result = sqlx::query(
        r#"
        INSERT INTO event_analytics (id, event_type, payload, timestamp, received_at)
        SELECT * FROM UNNEST($1::text[], $2::text[], $3::jsonb[], $4::bigint[], $5::bigint[])
        ON CONFLICT (id) DO NOTHING
        "#,
    )
    .bind(ids.as_slice())
    .bind(event_types.as_slice())
    .bind(payloads.as_slice())
    .bind(timestamps.as_slice())
    .bind(received_ats.as_slice())
    .execute(pool)
    .await;

    match result {
        Ok(r) => {
            tracing::info!(count, inserted = r.rows_affected(), "PostgreSQL 배치 INSERT 완료");
            for d in deliveries.drain(..) {
                let _ = d.ack(BasicAckOptions::default()).await;
            }
        }
        Err(e) => {
            tracing::error!(error = ?e, count, "PostgreSQL 배치 INSERT 실패, 재큐잉");
            if let Some(n) = notifier {
                let err_code = match &e {
                    sqlx::Error::Database(db) => format!("DB-{}", db.code().as_deref().unwrap_or("?")),
                    sqlx::Error::PoolTimedOut => "pool_timeout".to_string(),
                    sqlx::Error::PoolClosed   => "pool_closed".to_string(),
                    _ => "query_error".to_string(),
                };
                n.notify(&format!(
                    "⚠️ <b>[PostgreSQL 소비자]</b> 배치 INSERT 실패 ({count}건) — {err_code}"
                )).await;
            }
            for d in deliveries.drain(..) {
                let _ = d.nack(BasicNackOptions { requeue: true, multiple: false }).await;
            }
        }
    }

    ids.clear();
    event_types.clear();
    payloads.clear();
    timestamps.clear();
    received_ats.clear();
}
