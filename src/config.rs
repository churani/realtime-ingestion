/// URL 패스워드에 사용할 수 없는 특수문자를 percent-encoding으로 변환한다.
fn url_encode_password(s: &str) -> String {
    s.chars()
        .flat_map(|c| match c {
            '@' => vec!['%', '4', '0'],
            '#' => vec!['%', '2', '3'],
            ':' => vec!['%', '3', 'A'],
            '/' => vec!['%', '2', 'F'],
            '%' => vec!['%', '2', '5'],
            c   => vec![c],
        })
        .collect()
}

/// 서비스 전체 설정값.
/// 모든 값은 환경변수에서 읽어오며, 필수 항목이 없으면 패닉으로 즉시 종료한다.
/// (잘못된 설정으로 서버가 반쯤 뜨는 것보다 빠른 실패가 낫다.)
#[derive(Debug, Clone)]
pub struct Config {
    /// HTTP 서버 바인딩 주소 (예: "0.0.0.0:8080")
    pub server_addr: String,

    /// Redis 연결 URL (예: "redis://127.0.0.1:6379")
    pub redis_url: String,

    /// RabbitMQ AMQP URL (예: "amqp://guest:guest@127.0.0.1:5672")
    pub rabbitmq_url: String,

    /// MySQL 연결 URL (예: "mysql://user:pass@127.0.0.1:3306/dbname")
    pub mysql_url: String,

    /// PostgreSQL 연결 URL. None이면 PostgreSQL 소비자를 비활성화한다.
    pub postgres_url: Option<String>,

    /// Redis에서 중복 키를 유지할 TTL (초). 기본 300초(5분).
    /// 이 시간 안에 같은 id가 오면 중복으로 판단한다.
    pub dedup_ttl_secs: u64,

    /// RabbitMQ exchange 이름 (fanout 타입으로 선언됨)
    pub exchange_name: String,

    /// MySQL 소비자가 구독할 큐 이름
    pub mysql_queue: String,

    /// PostgreSQL 소비자가 구독할 큐 이름
    pub postgres_queue: String,

    /// 텔레그램 Bot 토큰 (미설정 시 알림 비활성화)
    pub telegram_bot_token: Option<String>,

    /// 텔레그램 알림을 받을 Chat ID
    pub telegram_chat_id: Option<String>,

    /// TCP 리스너 바인딩 주소 (예: "0.0.0.0:38701")
    pub tcp_addr: String,

    /// TCP 접속을 허용할 IP 목록. 비어있으면 모든 IP 허용.
    /// 환경변수 ALLOWED_IPS에 쉼표로 구분해 설정 (예: "192.168.1.10,10.0.0.5")
    pub allowed_ips: Vec<std::net::IpAddr>,

    /// MySQL 배치 INSERT 크기 (건수 도달 시 즉시 flush)
    pub mysql_batch_size: usize,

    /// MySQL 배치 인터벌 (초) — 크기 미달이어도 이 시간마다 flush
    pub mysql_batch_interval_secs: u64,

    /// PostgreSQL 배치 INSERT 크기
    pub pgsql_batch_size: usize,

    /// PostgreSQL 배치 인터벌 (초)
    pub pgsql_batch_interval_secs: u64,

    // ── 보안 설정 ────────────────────────────────────────────────────────────

    /// HTTP API 키 (X-API-Key 헤더). 필수값 — 미설정 시 서버 시작 거부.
    pub api_key: String,

    /// IP당 HTTP 초당 최대 요청 수 (rate limiting)
    pub http_rate_per_second: u32,

    /// HTTP rate limit 버스트 허용량
    pub http_rate_burst: u32,

    /// HTTP 요청 바디 최대 크기 (바이트). 초과 시 413 반환.
    pub http_max_body_bytes: usize,

    /// TCP 전체 최대 동시 연결 수
    pub tcp_max_connections: usize,

    /// IP당 최대 동시 TCP 연결 수
    pub tcp_max_conn_per_ip: usize,

    /// TCP 프레임 최대 크기 (바이트). 초과 시 연결 버퍼를 폐기.
    pub tcp_max_frame_bytes: usize,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            server_addr: std::env::var("SERVER_ADDR")
                .unwrap_or_else(|_| "0.0.0.0:8081".to_string()),

            // REDIS_URL 우선, 없으면 REDIS_HOST + REDIS_PORT 조합
            redis_url: std::env::var("REDIS_URL").unwrap_or_else(|_| {
                let host = std::env::var("REDIS_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
                let port = std::env::var("REDIS_PORT").unwrap_or_else(|_| "6379".to_string());
                format!("redis://{host}:{port}")
            }),

            // RABBITMQ_URL 우선, 없으면 RABBITMQ_V2_* 조합
            rabbitmq_url: std::env::var("RABBITMQ_URL").unwrap_or_else(|_| {
                let host = std::env::var("RABBITMQ_V2_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
                let port = std::env::var("RABBITMQ_V2_PORT").unwrap_or_else(|_| "5672".to_string());
                let user = std::env::var("RABBITMQ_V2_USER").unwrap_or_else(|_| "guest".to_string());
                let pass = url_encode_password(&std::env::var("RABBITMQ_V2_PASS").unwrap_or_default());
                format!("amqp://{user}:{pass}@{host}:{port}/%2F")
            }),

            // MYSQL_URL 우선, 없으면 MYSQL_* 조합
            mysql_url: std::env::var("MYSQL_URL").unwrap_or_else(|_| {
                let host = std::env::var("MYSQL_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
                let port = std::env::var("MYSQL_PORT").unwrap_or_else(|_| "3306".to_string());
                let user = std::env::var("MYSQL_USER").unwrap_or_else(|_| "root".to_string());
                let pass = url_encode_password(&std::env::var("MYSQL_PASS").unwrap_or_default());
                let db   = std::env::var("MYSQL_DB").unwrap_or_default();
                format!("mysql://{user}:{pass}@{host}:{port}/{db}")
            }),

            // POSTGRES_URL → PGSQL_* → POSTGRES_* 순으로 시도. 미설정 시 None → 소비자 비활성화
            postgres_url: std::env::var("POSTGRES_URL").ok().or_else(|| {
                let host = std::env::var("PGSQL_HOST")
                    .or_else(|_| std::env::var("POSTGRES_HOST")).ok()?;
                let port = std::env::var("PGSQL_PORT")
                    .or_else(|_| std::env::var("POSTGRES_PORT"))
                    .unwrap_or_else(|_| "5432".to_string());
                let user = std::env::var("PGSQL_USER")
                    .or_else(|_| std::env::var("POSTGRES_USER")).ok()?;
                let pass = url_encode_password(
                    &std::env::var("PGSQL_PASS")
                        .or_else(|_| std::env::var("POSTGRES_PASS"))
                        .unwrap_or_default()
                );
                let db = std::env::var("PGSQL_DB")
                    .or_else(|_| std::env::var("POSTGRES_DB"))
                    .unwrap_or_default();
                Some(format!("postgres://{user}:{pass}@{host}:{port}/{db}"))
            }),

            // DEDUP_TTL_SECS 우선, 없으면 REDIS_TTL, 없으면 300초
            dedup_ttl_secs: std::env::var("DEDUP_TTL_SECS")
                .or_else(|_| std::env::var("REDIS_TTL"))
                .unwrap_or_else(|_| "300".to_string())
                .parse()
                .expect("DEDUP_TTL_SECS 는 양의 정수여야 합니다"),

            exchange_name: std::env::var("EXCHANGE_NAME")
                .unwrap_or_else(|_| "events".to_string()),

            mysql_queue: std::env::var("MYSQL_QUEUE")
                .unwrap_or_else(|_| "queue.transactions".to_string()),

            postgres_queue: std::env::var("POSTGRES_QUEUE")
                .unwrap_or_else(|_| "queue.analytics".to_string()),

            // TELEGRAM_BOT_TOKEN 우선, 없으면 TELE_TOKEN
            telegram_bot_token: std::env::var("TELEGRAM_BOT_TOKEN")
                .or_else(|_| std::env::var("TELE_TOKEN")).ok(),
            // TELEGRAM_CHAT_ID 우선, 없으면 TELE_CHATID
            telegram_chat_id: std::env::var("TELEGRAM_CHAT_ID")
                .or_else(|_| std::env::var("TELE_CHATID")).ok(),

            tcp_addr: std::env::var("TCP_ADDR")
                .unwrap_or_else(|_| "127.0.0.1:38701".to_string()),

            allowed_ips: std::env::var("ALLOWED_IPS")
                .unwrap_or_default()
                .split(',')
                .filter_map(|s| s.trim().parse().ok())
                .collect(),

            mysql_batch_size: std::env::var("MYSQL_BATCH_SIZE")
                .unwrap_or_else(|_| "100".to_string())
                .parse()
                .expect("MYSQL_BATCH_SIZE 는 양의 정수여야 합니다"),

            mysql_batch_interval_secs: std::env::var("MYSQL_BATCH_INTERVAL")
                .unwrap_or_else(|_| "1".to_string())
                .parse()
                .expect("MYSQL_BATCH_INTERVAL 는 양의 정수여야 합니다"),

            pgsql_batch_size: std::env::var("PGSQL_BATCH_SIZE")
                .unwrap_or_else(|_| "100".to_string())
                .parse()
                .expect("PGSQL_BATCH_SIZE 는 양의 정수여야 합니다"),

            pgsql_batch_interval_secs: std::env::var("PGSQL_BATCH_INTERVAL")
                .unwrap_or_else(|_| "1".to_string())
                .parse()
                .expect("PGSQL_BATCH_INTERVAL 는 양의 정수여야 합니다"),

            api_key: std::env::var("API_KEY")
                .ok()
                .filter(|s| !s.is_empty())
                .expect("API_KEY 환경변수가 필수입니다 — X-API-Key 인증에 사용됩니다"),

            http_rate_per_second: std::env::var("HTTP_RATE_PER_SECOND")
                .unwrap_or_else(|_| "20".to_string())
                .parse()
                .expect("HTTP_RATE_PER_SECOND 는 양의 정수여야 합니다"),

            http_rate_burst: std::env::var("HTTP_RATE_BURST")
                .unwrap_or_else(|_| "50".to_string())
                .parse()
                .expect("HTTP_RATE_BURST 는 양의 정수여야 합니다"),

            http_max_body_bytes: std::env::var("HTTP_MAX_BODY_BYTES")
                .unwrap_or_else(|_| "65536".to_string())
                .parse()
                .expect("HTTP_MAX_BODY_BYTES 는 양의 정수여야 합니다"),

            tcp_max_connections: std::env::var("TCP_MAX_CONNECTIONS")
                .unwrap_or_else(|_| "1000".to_string())
                .parse()
                .expect("TCP_MAX_CONNECTIONS 는 양의 정수여야 합니다"),

            tcp_max_conn_per_ip: std::env::var("TCP_MAX_CONN_PER_IP")
                .unwrap_or_else(|_| "10".to_string())
                .parse()
                .expect("TCP_MAX_CONN_PER_IP 는 양의 정수여야 합니다"),

            tcp_max_frame_bytes: std::env::var("TCP_MAX_FRAME_BYTES")
                .unwrap_or_else(|_| "8192".to_string())
                .parse()
                .expect("TCP_MAX_FRAME_BYTES 는 양의 정수여야 합니다"),
        }
    }
}
