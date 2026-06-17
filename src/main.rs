/// 실시간 데이터 수집 서비스 진입점.
///
/// 전체 흐름:
///   HTTP POST /events
///     → Redis SET NX (중복 필터)
///     → RabbitMQ Fanout Exchange "events" 발행
///         ├── queue.transactions → [tokio task] MySQL 소비자
///         └── queue.analytics    → [tokio task] PostgreSQL 소비자
///
/// 단일 바이너리로 수신·발행·소비를 모두 처리한다.
/// (규모가 커지면 소비자를 별도 바이너리로 분리 가능)
mod config;
mod consumer;
mod dedup;
mod models;
mod producer;
mod receiver;

use std::sync::Arc;

use anyhow::Result;
use axum::{
    routing::{get, post},
    Router,
};
use sqlx::{mysql::MySqlPoolOptions, postgres::PgPoolOptions};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use config::Config;
use dedup::DedupChecker;
use producer::Producer;
use receiver::AppState;

#[tokio::main]
async fn main() -> Result<()> {
    // .env 파일이 있으면 로드 (없어도 무시 — 운영 환경에서는 실제 환경변수 사용)
    let _ = dotenvy::dotenv();

    // 구조화 로깅 초기화
    // RUST_LOG=realtime_ingestion=debug 로 상세 로그 활성화 가능
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "realtime_ingestion=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // 환경변수에서 설정 로드 (필수 항목 없으면 여기서 패닉)
    let cfg = Config::from_env();
    tracing::info!("설정 로드 완료: {:#?}", cfg);

    // ── 의존성 초기화 ─────────────────────────────────────────────────

    // Redis 연결 (ConnectionManager = 자동 재연결)
    let dedup = DedupChecker::new(&cfg.redis_url, cfg.dedup_ttl_secs).await?;
    tracing::info!("Redis 연결 완료");

    // RabbitMQ: exchange + 두 큐를 선언하고 바인딩까지 완료
    let producer = Producer::connect(
        &cfg.rabbitmq_url,
        &cfg.exchange_name,
        &[&cfg.mysql_queue, &cfg.postgres_queue],
    )
    .await?;
    tracing::info!("RabbitMQ 연결 완료 (exchange: {})", cfg.exchange_name);

    // MySQL 커넥션 풀
    // max_connections: 소비자 1개 기준 10개면 충분 (burst 대응)
    let mysql_pool = MySqlPoolOptions::new()
        .max_connections(10)
        .connect(&cfg.mysql_url)
        .await?;
    tracing::info!("MySQL 풀 생성 완료");

    // PostgreSQL 커넥션 풀
    let pg_pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&cfg.postgres_url)
        .await?;
    tracing::info!("PostgreSQL 풀 생성 완료");

    // ── 공유 상태 구성 ────────────────────────────────────────────────
    // Arc: 여러 tokio task가 소유권 없이 공유 가능
    let state = Arc::new(AppState { dedup, producer });

    // ── 소비자 태스크 시작 (자동 재시작 루프) ────────────────────────
    // 소비자가 AMQP 오류 등으로 종료되면 5초 후 재시작한다.
    // 이렇게 하면 RabbitMQ 일시 장애에도 서비스가 자동 회복된다.

    {
        let amqp_url = cfg.rabbitmq_url.clone();
        let queue    = cfg.mysql_queue.clone();
        let pool     = mysql_pool.clone();

        tokio::spawn(async move {
            loop {
                match consumer::mysql::run(&amqp_url, &queue, pool.clone()).await {
                    Ok(_)  => tracing::warn!("MySQL 소비자 정상 종료, 5초 후 재시작"),
                    Err(e) => tracing::error!(error = ?e, "MySQL 소비자 오류, 5초 후 재시작"),
                }
                // 재시작 전 대기 — 브로커가 일시 불능일 때 busy-loop 방지
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            }
        });
    }

    {
        let amqp_url = cfg.rabbitmq_url.clone();
        let queue    = cfg.postgres_queue.clone();
        let pool     = pg_pool.clone();

        tokio::spawn(async move {
            loop {
                match consumer::postgres::run(&amqp_url, &queue, pool.clone()).await {
                    Ok(_)  => tracing::warn!("PostgreSQL 소비자 정상 종료, 5초 후 재시작"),
                    Err(e) => tracing::error!(error = ?e, "PostgreSQL 소비자 오류, 5초 후 재시작"),
                }
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            }
        });
    }

    // ── HTTP 서버 시작 ────────────────────────────────────────────────
    let app = Router::new()
        .route("/events", post(receiver::handle_event)) // 이벤트 수신
        .route("/health", get(receiver::health_check))  // 헬스체크
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&cfg.server_addr).await?;
    tracing::info!("HTTP 서버 시작: http://{}", cfg.server_addr);

    // axum::serve: graceful shutdown 없이 단순 서빙 (운영 환경에서는 signal 핸들러 추가 권장)
    axum::serve(listener, app).await?;

    Ok(())
}
