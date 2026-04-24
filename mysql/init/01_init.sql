-- ============================================================
-- 암호화폐 아비트라지 파이프라인 - 테이블 초기화
-- ============================================================

USE crypto_arbitrage;

-- 환율 캐싱 테이블
CREATE TABLE IF NOT EXISTS exchange_rates (
    id INT AUTO_INCREMENT PRIMARY KEY,
    base_currency VARCHAR(10) NOT NULL,
    target_currency VARCHAR(10) NOT NULL,
    rate DECIMAL(15, 6) NOT NULL,
    fetched_at DATETIME NOT NULL,
    INDEX idx_fetched (fetched_at DESC)
);

-- 아비트라지 탐지 결과 (Gold Data)
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
    detected_at DATETIME NOT NULL,
    INDEX idx_symbol_detected (symbol, detected_at)
);

-- 일별 통계 (Airflow 배치 집계)
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