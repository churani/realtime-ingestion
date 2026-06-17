/// PostgreSQL 소비자 독립 바이너리.
///
/// RabbitMQ queue.analytics 를 구독해 PostgreSQL event_analytics 테이블에 저장한다.
/// 오류 발생 시 5초 후 자동 재시작한다.
use anyhow::Result;
use sqlx::postgres::PgPoolOptions;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use realtime_ingestion::{config::Config, consumer, logger, telegram};

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::from_path("/var/www/app/.env");

    // 파일 로그: /var/www/log/consumer-postgres/{년}/{월}/{일}/{시}.log
    let file_writer = logger::HourlyWriter::new("/var/www/log/consumer-postgres")?;
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_writer);
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "realtime_ingestion=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::fmt::layer().with_writer(non_blocking))
        .init();

    let cfg = Config::from_env();
    tracing::info!("PostgreSQL 소비자 시작");

    let pg_url = cfg.postgres_url.as_deref()
        .expect("POSTGRES_URL 또는 PGSQL_* 환경변수가 필요합니다");

    let notifier = match (cfg.telegram_bot_token.clone(), cfg.telegram_chat_id.clone()) {
        (Some(token), Some(chat_id)) => Some(telegram::Notifier::new(token, chat_id)),
        _ => None,
    };

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(pg_url)
        .await?;
    tracing::info!("PostgreSQL 풀 생성 완료");

    loop {
        match consumer::postgres::run(
            &cfg.rabbitmq_url,
            &cfg.postgres_queue,
            pool.clone(),
            notifier.clone(),
            cfg.pgsql_batch_size,
            cfg.pgsql_batch_interval_secs,
        ).await {
            Ok(_)  => tracing::warn!("PostgreSQL 소비자 정상 종료, 5초 후 재시작"),
            Err(e) => tracing::error!(error = ?e, "PostgreSQL 소비자 오류, 5초 후 재시작"),
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    }
}
