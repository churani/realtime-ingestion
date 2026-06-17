/// PostgreSQL 소비자 — 분석용 이벤트 저장 담당.
///
/// 역할 분리 전략:
///   PostgreSQL은 JSONB 인덱스(GIN), 윈도 함수, CTE 등 분석 쿼리에 강하다.
///   MySQL이 트랜잭션 원본을 보관하는 동안,
///   PostgreSQL은 집계·통계·검색용 데이터를 저장한다.
///
///   예: 시간대별 이벤트 수, event_type 분포, payload.amount 합산 등
///
/// JSONB vs JSON (PostgreSQL):
///   JSONB = 파싱된 바이너리 저장 → 인덱스 생성 가능 (GIN), 쿼리 속도 우수
///   JSON  = 텍스트 원본 저장 → 입력 순서 보존, 인덱스 불가
///   분석 용도이므로 JSONB를 선택한다.
use anyhow::Result;
use futures::StreamExt;
use lapin::{
    options::{BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicQosOptions},
    types::FieldTable,
    Connection, ConnectionProperties,
};
use sqlx::{types::Json, PgPool};

use crate::models::QueueMessage;

/// PostgreSQL 소비자 루프를 실행한다.
///
/// MySQL 소비자와 동일한 재시작 패턴을 사용한다.
/// 두 소비자는 각자 독립된 AMQP 연결·채널을 가지므로 서로 영향을 주지 않는다.
pub async fn run(amqp_url: &str, queue_name: &str, pool: PgPool) -> Result<()> {
    let conn = Connection::connect(amqp_url, ConnectionProperties::default()).await?;
    let channel = conn.create_channel().await?;

    channel
        .basic_qos(50, BasicQosOptions { global: false })
        .await?;

    let mut consumer = channel
        .basic_consume(
            queue_name,
            "postgres-consumer", // RabbitMQ 관리 UI에서 식별할 태그
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    tracing::info!(queue = %queue_name, "PostgreSQL 소비자 시작");

    while let Some(delivery) = consumer.next().await {
        let delivery = match delivery {
            Ok(d) => d,
            Err(e) => {
                tracing::error!(error = ?e, "PostgreSQL 소비자 채널 오류");
                return Err(e.into());
            }
        };

        match insert_analytics(&delivery.data, &pool).await {
            Ok(_) => {
                delivery.ack(BasicAckOptions::default()).await?;
                tracing::debug!("PostgreSQL ACK 완료");
            }
            Err(e) => {
                tracing::error!(error = ?e, "PostgreSQL insert 실패, 재큐잉");
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

/// 메시지를 PostgreSQL `event_analytics` 테이블에 저장한다.
async fn insert_analytics(data: &[u8], pool: &PgPool) -> Result<()> {
    let msg: QueueMessage = serde_json::from_slice(data)?;

    sqlx::query(
        r#"
        INSERT INTO event_analytics
            (id, event_type, payload, timestamp, received_at)
        VALUES
            ($1, $2, $3, $4, $5)
        ON CONFLICT (id) DO NOTHING
        -- MySQL과 달리 DO NOTHING을 선택한 이유:
        --   분석 레코드는 처음 도착한 시점의 데이터가 정본.
        --   재시도로 같은 id가 들어와도 기존 데이터를 덮어쓰지 않는다.
        "#,
    )
    .bind(&msg.id)
    .bind(&msg.event_type)
    // Json<T> 래퍼: sqlx가 serde_json::Value를 JSONB 바이트로 직렬화해서 바인딩
    // 이렇게 해야 PostgreSQL이 JSONB로 파싱하며 GIN 인덱스를 활용할 수 있음
    .bind(Json(&msg.payload))
    .bind(msg.timestamp)
    .bind(msg.received_at)
    .execute(pool)
    .await?;

    tracing::debug!(event_id = %msg.id, "PostgreSQL event_analytics 저장 완료");
    Ok(())
}
