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

    /// PostgreSQL 연결 URL (예: "postgres://user:pass@127.0.0.1:5432/dbname")
    pub postgres_url: String,

    /// Redis에서 중복 키를 유지할 TTL (초). 기본 300초(5분).
    /// 이 시간 안에 같은 id가 오면 중복으로 판단한다.
    pub dedup_ttl_secs: u64,

    /// RabbitMQ exchange 이름 (fanout 타입으로 선언됨)
    pub exchange_name: String,

    /// MySQL 소비자가 구독할 큐 이름
    pub mysql_queue: String,

    /// PostgreSQL 소비자가 구독할 큐 이름
    pub postgres_queue: String,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            server_addr: std::env::var("SERVER_ADDR")
                .unwrap_or_else(|_| "0.0.0.0:8080".to_string()),

            redis_url: std::env::var("REDIS_URL")
                .expect("환경변수 REDIS_URL 이 설정되지 않았습니다"),

            rabbitmq_url: std::env::var("RABBITMQ_URL")
                .expect("환경변수 RABBITMQ_URL 이 설정되지 않았습니다"),

            mysql_url: std::env::var("MYSQL_URL")
                .expect("환경변수 MYSQL_URL 이 설정되지 않았습니다"),

            postgres_url: std::env::var("POSTGRES_URL")
                .expect("환경변수 POSTGRES_URL 이 설정되지 않았습니다"),

            dedup_ttl_secs: std::env::var("DEDUP_TTL_SECS")
                .unwrap_or_else(|_| "300".to_string())
                .parse()
                .expect("DEDUP_TTL_SECS 는 양의 정수여야 합니다"),

            exchange_name: std::env::var("EXCHANGE_NAME")
                .unwrap_or_else(|_| "events".to_string()),

            mysql_queue: std::env::var("MYSQL_QUEUE")
                .unwrap_or_else(|_| "queue.transactions".to_string()),

            postgres_queue: std::env::var("POSTGRES_QUEUE")
                .unwrap_or_else(|_| "queue.analytics".to_string()),
        }
    }
}
