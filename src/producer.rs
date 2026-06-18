use anyhow::Result;
use lapin::{
    options::{
        BasicPublishOptions, ConfirmSelectOptions, ExchangeDeclareOptions, QueueBindOptions,
        QueueDeclareOptions,
    },
    types::{AMQPValue, FieldTable},
    BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind,
};

use crate::models::QueueMessage;

/// RabbitMQ 메시지 발행자.
///
/// 아키텍처:
///   Fanout Exchange "events"
///     ├── queue.transactions  → MySQL 소비자
///     └── queue.analytics     → PostgreSQL 소비자
///
///   Fanout을 쓰는 이유:
///   하나의 이벤트를 두 소비자가 동시에 받아야 하므로,
///   라우팅 키 없이 바인딩된 모든 큐에 복사 전달하는 fanout이 가장 적합하다.
///
///   Publisher Confirms(발행 확인) 활성화:
///   basic_publish 후 브로커가 ACK를 보낼 때까지 대기하므로
///   메시지 유실 없이 "최소 1회 전달" 을 보장한다.
///   (단, 소비자 측 중복 처리도 ON DUPLICATE KEY UPDATE 로 보완)
///
/// `Channel`은 내부적으로 Arc로 래핑되어 있어 Clone이 공짜다.
/// Mutex 없이도 concurrent publish가 가능하다.
#[derive(Clone)]
pub struct Producer {
    /// lapin AMQP 채널 (Arc 기반, Clone 가능)
    channel: Channel,

    /// 발행할 exchange 이름
    exchange: String,
}

impl Producer {
    /// AMQP 연결 → 채널 생성 → exchange·큐·DLQ 선언 → 바인딩을 순서대로 수행한다.
    ///
    /// # Arguments
    /// - `amqp_url`      : AMQP 연결 문자열
    /// - `exchange_name` : fanout exchange 이름
    /// - `queues`        : `(큐 이름, DLQ 이름)` 쌍 목록
    /// - `dlq_exchange`  : Dead Letter Exchange 이름
    ///
    /// DLQ 흐름:
    ///   consumer nack(requeue=false)
    ///     → RabbitMQ가 x-death 헤더(실패 이유·시각·원본 큐) 추가
    ///     → dlq_exchange(direct)로 라우팅
    ///     → dlq.{queue} 에 적재
    ///
    /// ⚠️  기존 큐가 DLX 인수 없이 선언되어 있으면 406 PRECONDITION_FAILED 발생.
    ///     배포 전 RabbitMQ 관리 콘솔에서 기존 큐를 삭제해야 한다.
    pub async fn connect(
        amqp_url: &str,
        exchange_name: &str,
        queues: &[(&str, &str)],
        dlq_exchange: &str,
    ) -> Result<Self> {
        let conn = Connection::connect(amqp_url, ConnectionProperties::default()).await?;
        let channel = conn.create_channel().await?;

        channel.confirm_select(ConfirmSelectOptions::default()).await?;

        // 메인 fanout exchange
        channel
            .exchange_declare(
                exchange_name,
                ExchangeKind::Fanout,
                ExchangeDeclareOptions { durable: true, ..Default::default() },
                FieldTable::default(),
            )
            .await?;

        // DLQ direct exchange
        channel
            .exchange_declare(
                dlq_exchange,
                ExchangeKind::Direct,
                ExchangeDeclareOptions { durable: true, ..Default::default() },
                FieldTable::default(),
            )
            .await?;

        for (queue_name, dlq_name) in queues {
            // 메인 큐: x-dead-letter-exchange + x-dead-letter-routing-key 설정
            let mut args = FieldTable::default();
            args.insert("x-dead-letter-exchange".into(),    AMQPValue::LongString(dlq_exchange.into()));
            args.insert("x-dead-letter-routing-key".into(), AMQPValue::LongString((*dlq_name).into()));

            channel
                .queue_declare(
                    queue_name,
                    QueueDeclareOptions { durable: true, ..Default::default() },
                    args,
                )
                .await?;

            channel
                .queue_bind(queue_name, exchange_name, "", QueueBindOptions::default(), FieldTable::default())
                .await?;

            // DLQ: durable, dlq_exchange에 dlq_name 라우팅 키로 바인딩
            channel
                .queue_declare(
                    dlq_name,
                    QueueDeclareOptions { durable: true, ..Default::default() },
                    FieldTable::default(),
                )
                .await?;

            channel
                .queue_bind(dlq_name, dlq_exchange, dlq_name, QueueBindOptions::default(), FieldTable::default())
                .await?;

            tracing::info!(queue = %queue_name, dlq = %dlq_name, "큐 선언 완료 (DLX: {dlq_exchange})");
        }

        Ok(Self {
            channel,
            exchange: exchange_name.to_string(),
        })
    }

    /// 메시지를 exchange에 발행하고 브로커 ACK를 기다린다.
    ///
    /// delivery_mode=2 로 설정해 디스크에 영속 저장(Persistent)시키므로
    /// 브로커가 재시작되더라도 큐에 남아있다.
    ///
    /// 브로커가 NACK를 보내거나 타임아웃이 발생하면 Err를 반환한다.
    pub async fn publish(&self, msg: &QueueMessage) -> Result<()> {
        // 구조체를 JSON 바이트로 직렬화
        let body = serde_json::to_vec(msg)?;

        // basic_publish 반환값은 PublisherConfirm (아직 ACK를 받지 않은 상태)
        let confirm = self
            .channel
            .basic_publish(
                &self.exchange, // fanout exchange
                "",             // routing key (fanout에서 무시됨)
                BasicPublishOptions::default(),
                &body,
                BasicProperties::default()
                    .with_content_type("application/json".into())
                    .with_delivery_mode(2), // 2 = Persistent (디스크 저장)
            )
            .await?;

        // 브로커로부터 실제 ACK를 기다림 — 여기서 블로킹
        // ACK = 메시지가 브로커 큐에 안전하게 기록됨
        // NACK = 브로커가 메시지를 거부함 (큐 꽉 참 등)
        confirm.await?;

        Ok(())
    }
}
