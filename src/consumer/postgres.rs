/// PostgreSQL(TimescaleDB) 소비자 — kicc_daily_totals UPSERT 모드.
///
/// 카드 전문 1건을 받아 일별 집계 테이블에 누적한다.
///
///   INSERT INTO kicc_daily_totals (...) SELECT ... FROM UNNEST(...)
///   ON CONFLICT (timestamp, kicc_bid, kicc_mid, kicc_psn)
///   DO UPDATE SET card_amount = ... + EXCLUDED.card_amount, ...
///
/// 필드 매핑:
///   timestamp     ← approval_date(YYYYMMDD) → KST 자정 unix 초 → to_timestamp()
///   kicc_bid      ← table_key (사업자번호 raw)
///   kicc_mid      ← payload.merchant_no
///   kicc_psn      ← payload.terminal_no
///   cancel_flag != "0" → cancel 버킷
///   msg_type "0200"    → card 버킷 (그 외 → other)
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use chrono::{FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, TimeZone};
use futures::StreamExt;
use lapin::{
    message::Delivery,
    options::{BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicQosOptions},
    types::FieldTable,
    Connection, ConnectionProperties,
};
use sqlx::PgPool;

use crate::models::QueueMessage;
use crate::telegram::Notifier;

struct DailyRow {
    ts_unix:       i64,
    kicc_bid:      String,
    kicc_mid:      String,
    kicc_psn:      String,
    card_amount:   i64,
    card_count:    i32,
    cash_amount:   i64,
    cash_count:    i32,
    cancel_amount: i64,
    cancel_count:  i32,
    other_amount:  i64,
    other_count:   i32,
}

/// approval_date "YYYYMMDD" + approval_time "HHMMSS" → KST 시간 단위 unix 초 (분·초 = 0)
fn parse_datetime_unix(approval_date: &str, approval_time: &str) -> Option<i64> {
    let date = NaiveDate::parse_from_str(approval_date.trim(), "%Y%m%d").ok()?;
    let t    = approval_time.trim();
    let hour: u32 = if t.len() >= 2 { t[..2].parse().ok()? } else { 0 };
    let time = NaiveTime::from_hms_opt(hour, 0, 0)?;
    let dt   = NaiveDateTime::new(date, time);
    let kst  = FixedOffset::east_opt(9 * 3600)?;
    Some(kst.from_local_datetime(&dt).earliest()?.timestamp())
}

/// 금액 문자열 "0000001000" → 1000
fn parse_amount(s: &str) -> i64 {
    let t = s.trim().trim_start_matches('0');
    if t.is_empty() { 0 } else { t.parse().unwrap_or(0) }
}

/// QueueMessage → DailyRow 변환. table_key 없으면 None.
fn to_daily_row(msg: &QueueMessage) -> Option<DailyRow> {
    let p = &msg.payload;

    let kicc_bid = msg.table_key.as_deref()
        .filter(|s| !s.is_empty())?
        .to_string();

    let kicc_mid = p["merchant_no"].as_str().unwrap_or("").trim().to_string();
    let kicc_psn = p["terminal_no"].as_str().unwrap_or("").trim().to_string();

    let approval_date = p["approval_date"].as_str().unwrap_or("").trim();
    let approval_time = p["approval_time"].as_str().unwrap_or("").trim();
    if approval_date.len() != 8 {
        return None;
    }
    let ts_unix = parse_datetime_unix(approval_date, approval_time)?;

    let amount      = parse_amount(p["amount"].as_str().unwrap_or("0"));
    let cancel_flag = p["cancel_flag"].as_str().unwrap_or("").trim().to_string();
    let msg_type    = p["msg_type"].as_str().unwrap_or("").trim().to_string();

    let is_cancel = !cancel_flag.is_empty() && cancel_flag != "0";

    let (card_amount, card_count, cash_amount, cash_count,
         cancel_amount, cancel_count, other_amount, other_count) = if is_cancel {
        (0, 0, 0, 0, amount, 1, 0, 0)
    } else if msg_type == "0200" {
        (amount, 1, 0, 0, 0, 0, 0, 0)
    } else {
        (0, 0, 0, 0, 0, 0, amount, 1)
    };

    Some(DailyRow {
        ts_unix, kicc_bid, kicc_mid, kicc_psn,
        card_amount, card_count, cash_amount, cash_count,
        cancel_amount, cancel_count, other_amount, other_count,
    })
}

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
        "PostgreSQL 소비자 시작 (kicc_daily_totals UPSERT)"
    );

    let mut deliveries: Vec<Delivery>     = Vec::with_capacity(batch_size);
    let mut messages:   Vec<QueueMessage> = Vec::with_capacity(batch_size);

    let mut ticker = tokio::time::interval(Duration::from_secs(batch_interval_secs));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;

    loop {
        tokio::select! {
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
                                tracing::info!(
                                    id            = %msg.id,
                                    event_type    = %msg.event_type,
                                    table_key     = %msg.table_key.as_deref().unwrap_or(""),
                                    merchant_no   = %msg.payload["merchant_no"].as_str().unwrap_or(""),
                                    terminal_no   = %msg.payload["terminal_no"].as_str().unwrap_or(""),
                                    amount        = %msg.payload["amount"].as_str().unwrap_or(""),
                                    approval_date = %msg.payload["approval_date"].as_str().unwrap_or(""),
                                    cancel_flag   = %msg.payload["cancel_flag"].as_str().unwrap_or(""),
                                    "PostgreSQL 메시지 수신"
                                );
                                messages.push(msg);
                                deliveries.push(delivery);
                            }
                            Err(e) => {
                                tracing::error!(error = ?e, "메시지 파싱 실패, 폐기");
                                let _ = delivery
                                    .nack(BasicNackOptions { requeue: false, multiple: false })
                                    .await;
                            }
                        }

                        if messages.len() >= batch_size {
                            flush(&mut deliveries, &mut messages, &pool, &notifier).await;
                        }
                    }
                }
            }

            _ = ticker.tick() => {
                if !messages.is_empty() {
                    flush(&mut deliveries, &mut messages, &pool, &notifier).await;
                }
            }
        }
    }

    Ok(())
}

async fn flush(
    deliveries: &mut Vec<Delivery>,
    messages:   &mut Vec<QueueMessage>,
    pool: &PgPool,
    notifier: &Option<Arc<Notifier>>,
) {
    let pairs: Vec<_> = deliveries.drain(..).zip(messages.drain(..)).collect();

    // 배치 내 같은 키(ts+bid+mid+psn)가 여러 건이면 TimescaleDB ON CONFLICT가
    // "affect row a second time" 오류를 낸다. 미리 합산해서 유니크하게 만든다.
    type AggKey = (i64, String, String, String); // (ts_unix, bid, mid, psn)
    let mut agg: HashMap<AggKey, DailyRow> = HashMap::new();

    let mut group_deliveries = Vec::with_capacity(pairs.len());

    for (delivery, msg) in pairs {
        match to_daily_row(&msg) {
            Some(row) => {
                group_deliveries.push(delivery);
                let key: AggKey = (row.ts_unix, row.kicc_bid.clone(), row.kicc_mid.clone(), row.kicc_psn.clone());
                let entry = agg.entry(key).or_insert(DailyRow {
                    ts_unix: row.ts_unix,
                    kicc_bid: row.kicc_bid,
                    kicc_mid: row.kicc_mid,
                    kicc_psn: row.kicc_psn,
                    card_amount: 0, card_count: 0,
                    cash_amount: 0, cash_count: 0,
                    cancel_amount: 0, cancel_count: 0,
                    other_amount: 0, other_count: 0,
                });
                entry.card_amount   += row.card_amount;
                entry.card_count    += row.card_count;
                entry.cash_amount   += row.cash_amount;
                entry.cash_count    += row.cash_count;
                entry.cancel_amount += row.cancel_amount;
                entry.cancel_count  += row.cancel_count;
                entry.other_amount  += row.other_amount;
                entry.other_count   += row.other_count;
            }
            None => {
                tracing::warn!(id = %msg.id, "일별 집계 변환 실패 (table_key/날짜 없음) — 폐기");
                let _ = delivery.ack(BasicAckOptions::default()).await;
            }
        }
    }

    if group_deliveries.is_empty() {
        return;
    }

    // HashMap → 바인딩용 Vec 변환
    let rows: Vec<DailyRow> = agg.into_values().collect();
    let msg_count = group_deliveries.len();
    let row_count = rows.len();

    let mut ts_vec:     Vec<i64>   = Vec::with_capacity(row_count);
    let mut bid_vec:    Vec<String> = Vec::with_capacity(row_count);
    let mut mid_vec:    Vec<String> = Vec::with_capacity(row_count);
    let mut psn_vec:    Vec<String> = Vec::with_capacity(row_count);
    let mut card_a_vec: Vec<i64>   = Vec::with_capacity(row_count);
    let mut card_c_vec: Vec<i32>   = Vec::with_capacity(row_count);
    let mut cash_a_vec: Vec<i64>   = Vec::with_capacity(row_count);
    let mut cash_c_vec: Vec<i32>   = Vec::with_capacity(row_count);
    let mut canc_a_vec: Vec<i64>   = Vec::with_capacity(row_count);
    let mut canc_c_vec: Vec<i32>   = Vec::with_capacity(row_count);
    let mut othr_a_vec: Vec<i64>   = Vec::with_capacity(row_count);
    let mut othr_c_vec: Vec<i32>   = Vec::with_capacity(row_count);

    for r in rows {
        ts_vec.push(r.ts_unix);
        bid_vec.push(r.kicc_bid);
        mid_vec.push(r.kicc_mid);
        psn_vec.push(r.kicc_psn);
        card_a_vec.push(r.card_amount);
        card_c_vec.push(r.card_count);
        cash_a_vec.push(r.cash_amount);
        cash_c_vec.push(r.cash_count);
        canc_a_vec.push(r.cancel_amount);
        canc_c_vec.push(r.cancel_count);
        othr_a_vec.push(r.other_amount);
        othr_c_vec.push(r.other_count);
    }

    if msg_count != row_count {
        tracing::debug!(msg_count, row_count, "배치 내 중복 키 집계 완료");
    }

    let result = sqlx::query(
        r#"
        INSERT INTO kicc_daily_totals (
            timestamp, kicc_bid, kicc_mid, kicc_psn, kicc_port,
            card_amount, card_count, cash_amount, cash_count,
            cancel_amount, cancel_count, other_amount, other_count
        )
        SELECT to_timestamp(t), b, m, p, 0,
               ca, cc, ha, hc, na, nc, oa, oc
        FROM UNNEST(
            $1::bigint[], $2::text[], $3::text[], $4::text[],
            $5::bigint[], $6::int[],  $7::bigint[], $8::int[],
            $9::bigint[], $10::int[], $11::bigint[], $12::int[]
        ) AS u(t, b, m, p, ca, cc, ha, hc, na, nc, oa, oc)
        ON CONFLICT (timestamp, kicc_bid, kicc_mid, kicc_psn, kicc_port) DO UPDATE SET
            card_amount   = kicc_daily_totals.card_amount   + EXCLUDED.card_amount,
            card_count    = kicc_daily_totals.card_count    + EXCLUDED.card_count,
            cash_amount   = kicc_daily_totals.cash_amount   + EXCLUDED.cash_amount,
            cash_count    = kicc_daily_totals.cash_count    + EXCLUDED.cash_count,
            cancel_amount = kicc_daily_totals.cancel_amount + EXCLUDED.cancel_amount,
            cancel_count  = kicc_daily_totals.cancel_count  + EXCLUDED.cancel_count,
            other_amount  = kicc_daily_totals.other_amount  + EXCLUDED.other_amount,
            other_count   = kicc_daily_totals.other_count   + EXCLUDED.other_count
        "#,
    )
    .bind(ts_vec.as_slice())
    .bind(bid_vec.as_slice())
    .bind(mid_vec.as_slice())
    .bind(psn_vec.as_slice())
    .bind(card_a_vec.as_slice())
    .bind(card_c_vec.as_slice())
    .bind(cash_a_vec.as_slice())
    .bind(cash_c_vec.as_slice())
    .bind(canc_a_vec.as_slice())
    .bind(canc_c_vec.as_slice())
    .bind(othr_a_vec.as_slice())
    .bind(othr_c_vec.as_slice())
    .execute(pool)
    .await;

    match result {
        Ok(r) => {
            tracing::info!(msg_count, row_count, upserted = r.rows_affected(), "kicc_daily_totals UPSERT 완료");
            for d in group_deliveries {
                let _ = d.ack(BasicAckOptions::default()).await;
            }
        }
        Err(e) => {
            tracing::error!(error = ?e, msg_count, row_count, "kicc_daily_totals UPSERT 실패 — 폐기");
            if let Some(n) = notifier {
                let err_code = match &e {
                    sqlx::Error::Database(db) => format!("DB-{}", db.code().as_deref().unwrap_or("?")),
                    sqlx::Error::PoolTimedOut => "pool_timeout".to_string(),
                    sqlx::Error::PoolClosed   => "pool_closed".to_string(),
                    _ => "query_error".to_string(),
                };
                n.notify(&format!(
                    "⚠️ <b>[PostgreSQL 소비자]</b> UPSERT 실패 ({msg_count}건) — {err_code}"
                )).await;
            }
            for d in group_deliveries {
                let _ = d.nack(BasicNackOptions { requeue: false, multiple: false }).await;
            }
        }
    }
}
