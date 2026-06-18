/// 실시간 데이터 수신기 진입점.
///
/// 역할: 이벤트 수신 및 RabbitMQ 발행만 담당한다.
/// 소비자(MySQL·PostgreSQL)는 별도 바이너리로 분리되어 있다.
///
///   TCP  :38701  → parser → dedup → RabbitMQ fanout exchange "events"
///   HTTP :8081   → dedup  → RabbitMQ fanout exchange "events"
/// 
/// Arc Atomic Reference Counted
     // 여러 스레드에서 같은 데이터를 안전하게 공유
     // 참조 카운트가 0이 되면 자동으로 메모리 해제
use std::sync::Arc;
///Result  anyhow의 Result 에러 처리용 std::Result 보다 편리한 버전
use anyhow::Result;
/// axum = Rust의 웹 프레임워크 Python의 FastAPI, Node의 Express 같은 것
use axum::{
    extract::{DefaultBodyLimit, Request},  // extract 모듈에서 두 개
    middleware::Next,                       // middleware 모듈에서 하나
    routing::{get, post},                  // routing 모듈에서 두 개
    Router,                                // axum 직속
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use realtime_ingestion::{
    config::Config,
    dedup::DedupChecker,
    logger,
    producer::Producer,
    receiver::AppState,
    security,
    tcp_listener,
    telegram,
};

/// 비동기 런타임 실행 - 모든 함수를 동시에 처리, 순차처리 아님
#[tokio::main]
async fn main() -> Result<()> {
    match dotenvy::from_path("/var/www/app/.env") {
        Ok(_) => {}
        Err(dotenvy::Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => eprintln!("[경고] .env 로드 실패: {e}"),
    }

    // 파일 로그: /var/www/log/realtime-ingestion/{년}/{월}/{일}/{시}.log
    // 로그 발생
    //↓
    //├── 터미널 출력
    //└── 파일 출력 (논블로킹 = 로그 때문에 메인 스레드 안멈춤)
    let file_writer = logger::HourlyWriter::new("/var/www/log/realtime-ingestion")?;
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_writer);
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "realtime_ingestion=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        // 콘솔(터미널) 출력
        .with(tracing_subscriber::fmt::layer().with_writer(non_blocking))
        // 파일 출력 (논블로킹)
        .init();  // 전역 로거로 등록

    let cfg = Config::from_env();

    tracing::info!("수신기 설정 로드 완료");

    // clone 원본은 그대로 두고 복사본 생성
    let notifier = match (cfg.telegram_bot_token.clone(), cfg.telegram_chat_id.clone()) {
        (Some(token), Some(chat_id)) => {
            tracing::info!("텔레그램 알림 활성화");
            Some(telegram::Notifier::new(token, chat_id))
        }
        _ => {
            tracing::info!("텔레그램 알림 비활성화");
            None
        }
    };

    let dedup = DedupChecker::new(&cfg.redis_url, cfg.dedup_ttl_secs).await?;
    tracing::info!("Redis 연결 완료");

    let queues: Vec<(&str, &str)> = if cfg.postgres_url.is_some() {
        vec![
            (cfg.mysql_queue.as_str(),    cfg.mysql_dlq_queue.as_str()),
            (cfg.postgres_queue.as_str(), cfg.postgres_dlq_queue.as_str()),
        ]
    } else {
        vec![(cfg.mysql_queue.as_str(), cfg.mysql_dlq_queue.as_str())]
    };
    let producer = Producer::connect(
        &cfg.rabbitmq_url,
        &cfg.exchange_name,
        &queues,
        &cfg.dlq_exchange,
    ).await?;
    tracing::info!("RabbitMQ 연결 완료 (exchange: {})", cfg.exchange_name);

    //Arc 로 AppState 를 여러 스레드에서 안전하게 공유하는 코드 
    let state = Arc::new(AppState { dedup, producer });

    // TCP 리스너 태스크 (오류 시 5초 후 자동 재시작)
    {
        let addr              = cfg.tcp_addr.clone();
        let ips               = cfg.allowed_ips.clone();
        let state             = state.clone();
        let notifier          = notifier.clone();
        let max_connections   = cfg.tcp_max_connections;
        let max_conn_per_ip   = cfg.tcp_max_conn_per_ip;
        let max_frame_bytes   = cfg.tcp_max_frame_bytes;
        // 별도 태스크로 백그라운드 실행
        tokio::spawn(async move { 
            // 무한루프
            loop {
                match tcp_listener::start(
                    &addr,
                    ips.clone(),
                    state.clone(),
                    notifier.clone(),
                    max_connections,
                    max_conn_per_ip,
                    max_frame_bytes,
                ).await {
                    Ok(_)  => tracing::warn!("TCP 리스너 종료, 5초 후 재시작"),
                    Err(e) => tracing::error!(error = ?e, "TCP 리스너 오류, 5초 후 재시작"),
                }
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            }
        });
    }
    tracing::info!("TCP 리스너 시작: {} (허용 IP: {:?})", cfg.tcp_addr, cfg.allowed_ips);

    // ── HTTP 보안 레이어 ──────────────────────────────────────────────────────
    // 클라이언트 요청
    //      ↓
    //바디 크기 체크 (64KB?)
    //    ↓
    //Rate Limit 체크 (초당 몇 개?)
    //    ↓
    // /events  → API 키 체크 → handle_event
    // /health  → 바로 health_check

    // 1. API 키 검증 — /events 에만 적용 (/health 는 제외)
    let api_key = Arc::new(cfg.api_key.clone()); // Arc<String>
    let api_key_layer = {
        let key = api_key.clone();
        axum::middleware::from_fn(move |req: Request, next: Next| {
            let key = key.clone();
            async move { security::api_key_middleware(key, req, next).await }
        })
    };

    // 2. IP별 Rate Limiter — 모든 라우트에 적용
    let rate_limiter = security::new_rate_limiter(
        cfg.http_rate_per_second,
        cfg.http_rate_burst,
    );
    let rate_limit_layer = {
        let limiter = rate_limiter.clone();
        axum::middleware::from_fn(move |req: Request, next: Next| {
            let limiter = limiter.clone();
            async move { security::rate_limit_middleware(limiter, req, next).await }
        })
    };

    // 3. 페이로드 크기 제한 (기본 64 KB)
    let body_limit = DefaultBodyLimit::max(cfg.http_max_body_bytes);

    // ── HTTP 라우터 ───────────────────────────────────────────────────────────

    // receiver.rs의 handle_event 함수를 사용
    let app = Router::new()
        // /events: API 키 인증 필요
        .route(
            "/events",
            post(realtime_ingestion::receiver::handle_event).layer(api_key_layer),
        )
        // /health: 인증 불필요 (로드밸런서 헬스체크)
        .route("/health", get(realtime_ingestion::receiver::health_check))
        // 모든 라우트에 rate limiting + 바디 크기 제한 적용
        .layer(rate_limit_layer)
        .layer(body_limit)
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&cfg.server_addr).await?;
    tracing::info!("HTTP 서버 시작: http://{}", cfg.server_addr);
    tracing::info!(
        "보안 설정 — API 키: {}, Rate: {}req/s (burst {}), 바디: {}B",
        "활성화",
        cfg.http_rate_per_second,
        cfg.http_rate_burst,
        cfg.http_max_body_bytes,
    );

    // ConnectInfo로 실제 클라이언트 IP 추출 (rate limiter에 필요)
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        //                                           ^^^^^^^^^^^^^^^^^^^^^^
        //                                           클라이언트 실제 IP 추출
        //                                           Rate Limiter 가 IP 별로 추적하려면 필요
    )
    .await?;

    Ok(())
}
