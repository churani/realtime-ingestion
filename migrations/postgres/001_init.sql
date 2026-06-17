-- PostgreSQL 초기 스키마
-- 역할: 분석용 이벤트 저장
--
-- MySQL과의 차이점:
--   payload : JSONB (바이너리 JSON) 사용
--     - GIN 인덱스로 JSON 내부 키/값 검색 가능
--     - 예: payload @> '{"amount": 5000}' 로 특정 금액 이벤트 조회
--   created_at : TIMESTAMPTZ (시간대 포함) — 글로벌 서비스 대응

CREATE TABLE IF NOT EXISTS event_analytics (
    id          VARCHAR(255)    NOT NULL,
    event_type  VARCHAR(100)    NOT NULL,

    -- JSONB: 파싱된 바이너리 저장 → GIN 인덱스 + 연산자(@>, ?, #>) 지원
    payload     JSONB           NOT NULL,

    -- unix ms로 받아 BIGINT 저장 (타임존 변환 없이 정확한 원본 보존)
    timestamp   BIGINT          NOT NULL,
    received_at BIGINT          NOT NULL,

    -- DB 저장 시각 (타임존 포함)
    created_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    PRIMARY KEY (id)
);

-- event_type 별 집계 쿼리 최적화
CREATE INDEX IF NOT EXISTS idx_ea_event_type
    ON event_analytics (event_type);

-- 시간 범위 분석 최적화
CREATE INDEX IF NOT EXISTS idx_ea_timestamp
    ON event_analytics (timestamp);

-- JSONB 내부 검색을 위한 GIN 인덱스
-- 예: SELECT * FROM event_analytics WHERE payload @> '{"store_id": "S001"}'
CREATE INDEX IF NOT EXISTS idx_ea_payload_gin
    ON event_analytics USING GIN (payload);
