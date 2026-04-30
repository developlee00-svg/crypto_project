-- ============================================================
-- 암호화폐 아비트라지 파이프라인 - 테이블 초기화 (ADR v9)
-- ============================================================

USE crypto_arbitrage;

-- ------------------------------------------------------------
-- 환율 캐싱 테이블
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS exchange_rates (
    id INT AUTO_INCREMENT PRIMARY KEY,
    base_currency VARCHAR(10) NOT NULL,
    target_currency VARCHAR(10) NOT NULL,
    rate DECIMAL(15, 6) NOT NULL,
    fetched_at DATETIME NOT NULL,
    INDEX idx_fetched (fetched_at DESC)
);

-- ------------------------------------------------------------
-- 아비트라지 탐지 결과 (Warm Layer, 7일 보관)
-- ------------------------------------------------------------
-- v9 변경:
--   1) detected_at: DATETIME → DATETIME(3) (밀리초 보존, Flink TIMESTAMP(3) 매핑)
--   2) idx_detected: 시간순 조회 인덱스 (/opportunities 최근 N건)
--   3) UNIQUE KEY uk_dedup: kinesis-mysql-bridge 재시작 시 INSERT IGNORE 중복 방지
CREATE TABLE IF NOT EXISTS arbitrage_opportunities (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    buy_exchange VARCHAR(20) NOT NULL,
    buy_price_krw DECIMAL(20, 2) NOT NULL,
    sell_exchange VARCHAR(20) NOT NULL,
    sell_price_krw DECIMAL(20, 2) NOT NULL,
    spread_krw DECIMAL(20, 2) NOT NULL,
    spread_pct DECIMAL(8, 4) NOT NULL,
    exchange_rate DECIMAL(15, 6) NOT NULL,
    detected_at DATETIME(3) NOT NULL,
    INDEX idx_symbol_detected (symbol, detected_at),
    INDEX idx_detected (detected_at DESC),
    UNIQUE KEY uk_dedup (symbol, buy_exchange, sell_exchange, detected_at)
);

-- ------------------------------------------------------------
-- 일별 통계 (Gold Layer, 영구 보관)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS daily_stats (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    date DATE NOT NULL,
    avg_spread_pct DECIMAL(8, 4),
    max_spread_pct DECIMAL(8, 4),
    min_spread_pct DECIMAL(8, 4),
    opportunity_count INT,
    UNIQUE KEY uk_symbol_date (symbol, date)
);