/// MySQL 소비자 — 배치 INSERT 모드.
///
/// 테이블명은 `kicc_b{사업자번호}` 형태로 동적 결정된다.
/// table_key가 없는 메시지(HTTP 경로 이벤트 등)는 폐기한다.
///
/// 컬럼 매핑 (parser.rs → kicc_b* 테이블):
///   kicc_bid      ← table_key (사업자번호 raw)
///   kicc_tid      ← tran_unique_nbr (거래고유번호)
///   kicc_mid      ← merchant_no (가맹점번호, 최대 8자)
///   kicc_psn      ← terminal_no (단말기번호)
///   kicc_nick     ← issuer_name (발급사명)
///   kicc_trantype ← cancel_flag (취소구분, 1자)
///   kicc_cardnumber ← card_no (마스킹된 카드번호)
///   kicc_amount   ← amount (금액 문자열 → i64)
///   kicc_rescode  ← response_code (응답코드)
///   kicc_trno     ← tran_unique_nbr (거래번호)
///   kicc_acquirer ← acquirer_name (매입사명)
///   kicc_approve  ← approval_no (승인번호)
///   kicc_date     ← approval_date + approval_time (YYYYMMDDHHMMSS)
///   kicc_datetime ← approval_date + approval_time → "YYYY-MM-DD HH:MM:SS"
use std::collections::HashMap;
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

struct KiccRow {
    table:          String,
    kicc_bid:       String,
    kicc_tid:       String,
    kicc_mid:       String,
    kicc_psn:       String,
    kicc_nick:      String,
    kicc_trantype:  String,
    kicc_cardnumber:String,
    kicc_amount:    i64,
    kicc_rescode:   String,
    kicc_trno:      String,
    kicc_acquirer:  String,
    kicc_approve:   String,
    kicc_date:      String,  // YYYYMMDDHHMMSS (varchar 14)
    kicc_datetime:  String,  // YYYY-MM-DD HH:MM:SS
}

/// 금액 문자열 "0000001000" → 1000
fn parse_amount(s: &str) -> i64 {
    let t = s.trim().trim_start_matches('0');
    if t.is_empty() { 0 } else { t.parse().unwrap_or(0) }
}

/// "YYYYMMDD" + "HHMMSS" → "YYYY-MM-DD HH:MM:SS"
fn format_datetime(date: &str, time: &str) -> String {
    if date.len() == 8 && time.len() == 6 {
        format!(
            "{}-{}-{} {}:{}:{}",
            &date[0..4], &date[4..6], &date[6..8],
            &time[0..2], &time[2..4], &time[4..6]
        )
    } else {
        String::new()
    }
}

/// 사업자번호를 테이블명으로 변환. 숫자+영문자만 허용, 최대 20자.
fn resolve_table(key: Option<&str>) -> Option<String> {
    let k = key?.trim();
    if k.is_empty() || k.len() > 20 || !k.chars().all(|c| c.is_ascii_alphanumeric()) {
        return None;
    }
    Some(format!("kicc_b{}", k))
}

/// QueueMessage → KiccRow 변환. table_key 없으면 None.
fn to_kicc_row(msg: QueueMessage) -> Option<KiccRow> {
    let table = resolve_table(msg.table_key.as_deref())?;
    let p = &msg.payload;

    let approval_date = p["approval_date"].as_str().unwrap_or("").trim().to_string();
    let approval_time = p["approval_time"].as_str().unwrap_or("").trim().to_string();

    Some(KiccRow {
        table,
        kicc_bid:        msg.table_key.unwrap_or_default(),
        kicc_tid:        p["tran_unique_nbr"].as_str().unwrap_or("").trim().to_string(),
        kicc_mid:        p["merchant_no"].as_str().unwrap_or("").trim().chars().take(8).collect(),
        kicc_psn:        p["terminal_no"].as_str().unwrap_or("").trim().to_string(),
        kicc_nick:       p["issuer_name"].as_str().unwrap_or("").trim().to_string(),
        kicc_trantype:   p["cancel_flag"].as_str().unwrap_or("").trim().chars().take(1).collect(),
        kicc_cardnumber: p["card_no"].as_str().unwrap_or("").trim().chars().take(10).collect(),
        kicc_amount:     parse_amount(p["amount"].as_str().unwrap_or("0")),
        kicc_rescode:    p["response_code"].as_str().unwrap_or("").trim().to_string(),
        kicc_trno:       p["tran_unique_nbr"].as_str().unwrap_or("").trim().to_string(),
        kicc_acquirer:   p["acquirer_name"].as_str().unwrap_or("").trim().to_string(),
        kicc_approve:    p["approval_no"].as_str().unwrap_or("").trim().to_string(),
        kicc_date:       format!("{}{}", approval_date, approval_time),
        kicc_datetime:   format_datetime(&approval_date, &approval_time),
    })
}

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

    let prefetch = ((batch_size * 2) as u16).max(50);
    channel.basic_qos(prefetch, BasicQosOptions { global: false }).await?;

    let consumer_tag = format!(
        "mysql-consumer-{}",
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
        "MySQL 소비자 시작 (배치 모드)"
    );

    let mut deliveries: Vec<Delivery>  = Vec::with_capacity(batch_size);
    let mut rows:       Vec<KiccRow>   = Vec::with_capacity(batch_size);

    let mut ticker = tokio::time::interval(Duration::from_secs(batch_interval_secs));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;

    loop {
        tokio::select! {
            item = consumer.next() => {
                match item {
                    None => break,
                    Some(Err(e)) => {
                        tracing::error!(error = ?e, "MySQL 소비자 채널 오류");
                        if let Some(n) = &notifier {
                            n.notify("🔴 <b>[MySQL 소비자]</b> 채널 오류 — 재시작 예정").await;
                        }
                        return Err(e.into());
                    }
                    Some(Ok(delivery)) => {
                        match serde_json::from_slice::<QueueMessage>(&delivery.data) {
                            Ok(msg) => {
                                let msg_id     = msg.id.clone();
                                let event_type = msg.event_type.clone();
                                match to_kicc_row(msg) {
                                    Some(row) => {
                                        tracing::info!(
                                            id          = %msg_id,
                                            event_type  = %event_type,
                                            table       = %row.table,
                                            terminal_no = %row.kicc_psn,
                                            merchant_no = %row.kicc_mid,
                                            amount      = row.kicc_amount,
                                            approval_no = %row.kicc_approve,
                                            tran_type   = %row.kicc_trantype,
                                            "MySQL 메시지 수신"
                                        );
                                        rows.push(row);
                                        deliveries.push(delivery);
                                    }
                                    None => {
                                        tracing::warn!(id = %msg_id, "table_key 없음 — 폐기");
                                        let _ = delivery.ack(BasicAckOptions::default()).await;
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::error!(error = ?e, "메시지 파싱 실패, 폐기");
                                let _ = delivery
                                    .nack(BasicNackOptions { requeue: false, multiple: false })
                                    .await;
                            }
                        }

                        if rows.len() >= batch_size {
                            flush(&mut deliveries, &mut rows, &pool, &notifier).await;
                        }
                    }
                }
            }

            _ = ticker.tick() => {
                if !rows.is_empty() {
                    flush(&mut deliveries, &mut rows, &pool, &notifier).await;
                }
            }
        }
    }

    Ok(())
}

/// 테이블별로 그룹핑해 배치 INSERT.
async fn flush(
    deliveries: &mut Vec<Delivery>,
    rows: &mut Vec<KiccRow>,
    pool: &MySqlPool,
    notifier: &Option<Arc<Notifier>>,
) {
    let pairs: Vec<_> = deliveries.drain(..).zip(rows.drain(..)).collect();

    // 테이블별 그룹핑
    let mut groups: HashMap<String, Vec<(Delivery, KiccRow)>> = HashMap::new();
    for (delivery, row) in pairs {
        groups.entry(row.table.clone()).or_default().push((delivery, row));
    }

    for (table, group) in groups {
        let count = group.len();
        let (group_deliveries, group_rows): (Vec<_>, Vec<_>) = group.into_iter().unzip();

        let mut qb = QueryBuilder::new(format!(
            "INSERT INTO `{table}` \
             (kicc_bid, kicc_tid, kicc_mid, kicc_psn, kicc_nick, \
              kicc_trantype, kicc_cardnumber, kicc_amount, kicc_rescode, \
              kicc_trno, kicc_acquirer, kicc_approve, kicc_date, kicc_datetime) "
        ));
        qb.push_values(group_rows.iter(), |mut b, r| {
            b.push_bind(&r.kicc_bid)
             .push_bind(&r.kicc_tid)
             .push_bind(&r.kicc_mid)
             .push_bind(&r.kicc_psn)
             .push_bind(&r.kicc_nick)
             .push_bind(&r.kicc_trantype)
             .push_bind(&r.kicc_cardnumber)
             .push_bind(r.kicc_amount)
             .push_bind(&r.kicc_rescode)
             .push_bind(&r.kicc_trno)
             .push_bind(&r.kicc_acquirer)
             .push_bind(&r.kicc_approve)
             .push_bind(&r.kicc_date)
             .push_bind(&r.kicc_datetime);
        });

        match qb.build().execute(pool).await {
            Ok(_) => {
                tracing::info!(count, table = %table, "MySQL 배치 INSERT 완료");
                for d in group_deliveries {
                    let _ = d.ack(BasicAckOptions::default()).await;
                }
            }
            Err(e) => {
                tracing::error!(error = ?e, count, table = %table, "MySQL 배치 INSERT 실패 — 폐기");
                if let Some(n) = notifier {
                    let err_code = match &e {
                        sqlx::Error::Database(db) => format!("DB-{}", db.code().as_deref().unwrap_or("?")),
                        sqlx::Error::PoolTimedOut => "pool_timeout".to_string(),
                        sqlx::Error::PoolClosed   => "pool_closed".to_string(),
                        _ => "query_error".to_string(),
                    };
                    n.notify(&format!(
                        "⚠️ <b>[MySQL 소비자]</b> 배치 INSERT 실패 ({count}건, {table}) — {err_code}"
                    )).await;
                }
                for d in group_deliveries {
                    let _ = d.nack(BasicNackOptions { requeue: false, multiple: false }).await;
                }
            }
        }
    }
}
