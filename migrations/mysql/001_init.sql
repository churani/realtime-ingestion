-- MySQL 초기 스키마
-- 역할: 트랜잭션 원본 이벤트 영구 보관
--
-- 컬럼 설계 근거:
--   id         : VARCHAR(255) — UUID, 단말기 시퀀스 등 다양한 형식 수용
--   event_type : 이벤트 종류별 조회에 인덱스 활용
--   payload    : JSON 타입 — MySQL 5.7+에서 지원, 유효성 검사 + 경로 쿼리 가능
--   timestamp  : 클라이언트 발생 시각 (unix ms) — 이벤트 순서 정렬 기준
--   received_at: 서버 수신 시각 — 지연 측정, 감사 로그 용도

CREATE TABLE IF NOT EXISTS events (
    id          VARCHAR(255)    NOT NULL                    COMMENT '이벤트 고유 ID (dedup 기준)',
    event_type  VARCHAR(100)    NOT NULL                    COMMENT '이벤트 종류',
    payload     JSON            NOT NULL                    COMMENT '원본 JSON 페이로드',
    timestamp   BIGINT          NOT NULL                    COMMENT '클라이언트 발생 시각 (unix ms)',
    received_at BIGINT          NOT NULL                    COMMENT '서버 수신 시각 (unix ms)',
    created_at  TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'DB 저장 시각',

    PRIMARY KEY (id),

    -- event_type 별 최근 이벤트 조회 최적화
    INDEX idx_event_type  (event_type),

    -- 시간 범위 조회 최적화 (예: 특정 시간대 이벤트 집계)
    INDEX idx_timestamp   (timestamp),

    -- 수신 지연 분석용
    INDEX idx_received_at (received_at)

) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci
  COMMENT = '트랜잭션 원본 이벤트 테이블';
