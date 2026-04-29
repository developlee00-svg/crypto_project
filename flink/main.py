"""
Crypto Arbitrage Detection - Flink SQL Application
- 3 exchanges (Binance, Upbit, Bithumb) interval join
- Detects arbitrage opportunities (Kimchi premium)
"""

import os
import json
from pyflink.table import EnvironmentSettings, TableEnvironment


# ============================================================
# Runtime Properties 로드 (Managed Flink 환경)
# ============================================================
def get_application_properties():
    """Managed Flink Runtime Properties 읽기"""
    if os.environ.get("IS_LOCAL"):
        return {}
    props_path = "/etc/flink/application_properties.json"
    if not os.path.exists(props_path):
        return {}
    with open(props_path, "r") as f:
        return {p["PropertyGroupId"]: p["PropertyMap"] for p in json.load(f)}


# ============================================================
# main 함수
# ============================================================
def main():
    # --- 환경 설정 ---
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)

    # JAR 의존성 (Managed Flink는 자동, 로컬 테스트는 필요시 설정)
    # table_env.get_config().set(
    #     "pipeline.jars",
    #     "file:///opt/flink/usrlib/pyflink-dependencies.jar"
    # )

    # --- Runtime Property에서 환율 읽기 ---
    props = get_application_properties()
    s3_props = props.get("s3", {})
    EXCHANGE_RATE = float(s3_props.get("usd.krw.rate", "1370.0"))
    print(f"[INFO] Using exchange rate: {EXCHANGE_RATE}")

    # ============================================================
    # 1. Source 테이블 (입력 KDS)
    # ============================================================
    table_env.execute_sql("""
        CREATE TABLE crypto_input (
            `exchange`   VARCHAR(20),
            symbol       VARCHAR(20),
            price        DECIMAL(20, 8),
            currency     VARCHAR(10),
            `timestamp`  VARCHAR(30),
            event_time AS TO_TIMESTAMP(
                REPLACE(`timestamp`, 'Z', ''),
                'yyyy-MM-dd''T''HH:mm:ss'
            ),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector'           = 'kinesis-legacy',
            'stream'              = 'crypto-stream-input',
            'aws.region'          = 'ap-northeast-2',
            'scan.stream.initpos' = 'LATEST',
            'format'              = 'json',
            'json.timestamp-format.standard' = 'ISO-8601'
        )
    """)

    # ============================================================
    # 2. Sink 테이블 (출력 KDS)
    # ============================================================
    table_env.execute_sql("""
        CREATE TABLE arbitrage_output (
            symbol           VARCHAR(20),
            buy_exchange     VARCHAR(20),
            buy_price_krw    DECIMAL(20, 2),
            sell_exchange    VARCHAR(20),
            sell_price_krw   DECIMAL(20, 2),
            spread_krw       DECIMAL(20, 2),
            spread_pct       DECIMAL(8, 4),
            exchange_rate    DECIMAL(15, 6),
            detected_at      TIMESTAMP(3)
        ) WITH (
            'connector'        = 'kinesis',
            'stream.arn'       = 'arn:aws:kinesis:ap-northeast-2:827913617635:stream/crypto-stream-output',
            'aws.region'       = 'ap-northeast-2',
            'sink.partitioner' = 'random',
            'format'           = 'json'
        )
    """)

    # ============================================================
    # 3. 거래소별 뷰 분리 (Binance만 환율 적용)
    # ============================================================
    table_env.execute_sql(f"""
        CREATE TEMPORARY VIEW binance_enriched AS
        SELECT
            symbol,
            `exchange`,
            CAST(price * {EXCHANGE_RATE} AS DECIMAL(20, 2)) AS price_krw,
            event_time
        FROM crypto_input
        WHERE `exchange` = 'binance'
    """)

    table_env.execute_sql("""
        CREATE TEMPORARY VIEW upbit_enriched AS
        SELECT
            symbol,
            `exchange`,
            CAST(price AS DECIMAL(20, 2)) AS price_krw,
            event_time
        FROM crypto_input
        WHERE `exchange` = 'upbit'
    """)

    table_env.execute_sql("""
        CREATE TEMPORARY VIEW bithumb_enriched AS
        SELECT
            symbol,
            `exchange`,
            CAST(price AS DECIMAL(20, 2)) AS price_krw,
            event_time
        FROM crypto_input
        WHERE `exchange` = 'bithumb'
    """)

    # ============================================================
    # 4. INSERT INTO ... (3쌍 Interval Join + UNION ALL)
    # ============================================================
    table_env.execute_sql(f"""
        INSERT INTO arbitrage_output

        -- Pair 1: Upbit ↔ Binance
        SELECT
            u.symbol,
            CASE WHEN u.price_krw < b.price_krw THEN 'upbit'   ELSE 'binance' END AS buy_exchange,
            CASE WHEN u.price_krw < b.price_krw THEN u.price_krw ELSE b.price_krw END AS buy_price_krw,
            CASE WHEN u.price_krw < b.price_krw THEN 'binance' ELSE 'upbit'   END AS sell_exchange,
            CASE WHEN u.price_krw < b.price_krw THEN b.price_krw ELSE u.price_krw END AS sell_price_krw,
            ABS(u.price_krw - b.price_krw) AS spread_krw,
            CAST(
                ABS(u.price_krw - b.price_krw) / LEAST(u.price_krw, b.price_krw) * 100
                AS DECIMAL(8, 4)
            ) AS spread_pct,
            CAST({EXCHANGE_RATE} AS DECIMAL(15, 6)) AS exchange_rate,
            u.event_time AS detected_at
        FROM upbit_enriched AS u
        JOIN binance_enriched AS b
          ON u.symbol = b.symbol
         AND u.event_time BETWEEN b.event_time - INTERVAL '3' SECOND
                              AND b.event_time + INTERVAL '3' SECOND

        UNION ALL

        -- Pair 2: Bithumb ↔ Binance
        SELECT
            bt.symbol,
            CASE WHEN bt.price_krw < b.price_krw THEN 'bithumb' ELSE 'binance' END AS buy_exchange,
            CASE WHEN bt.price_krw < b.price_krw THEN bt.price_krw ELSE b.price_krw END AS buy_price_krw,
            CASE WHEN bt.price_krw < b.price_krw THEN 'binance' ELSE 'bithumb' END AS sell_exchange,
            CASE WHEN bt.price_krw < b.price_krw THEN b.price_krw ELSE bt.price_krw END AS sell_price_krw,
            ABS(bt.price_krw - b.price_krw) AS spread_krw,
            CAST(
                ABS(bt.price_krw - b.price_krw) / LEAST(bt.price_krw, b.price_krw) * 100
                AS DECIMAL(8, 4)
            ) AS spread_pct,
            CAST({EXCHANGE_RATE} AS DECIMAL(15, 6)) AS exchange_rate,
            bt.event_time AS detected_at
        FROM bithumb_enriched AS bt
        JOIN binance_enriched AS b
          ON bt.symbol = b.symbol
         AND bt.event_time BETWEEN b.event_time - INTERVAL '3' SECOND
                               AND b.event_time + INTERVAL '3' SECOND

        UNION ALL

        -- Pair 3: Upbit ↔ Bithumb
        SELECT
            u.symbol,
            CASE WHEN u.price_krw < bt.price_krw THEN 'upbit'   ELSE 'bithumb' END AS buy_exchange,
            CASE WHEN u.price_krw < bt.price_krw THEN u.price_krw ELSE bt.price_krw END AS buy_price_krw,
            CASE WHEN u.price_krw < bt.price_krw THEN 'bithumb' ELSE 'upbit'   END AS sell_exchange,
            CASE WHEN u.price_krw < bt.price_krw THEN bt.price_krw ELSE u.price_krw END AS sell_price_krw,
            ABS(u.price_krw - bt.price_krw) AS spread_krw,
            CAST(
                ABS(u.price_krw - bt.price_krw) / LEAST(u.price_krw, bt.price_krw) * 100
                AS DECIMAL(8, 4)
            ) AS spread_pct,
            CAST({EXCHANGE_RATE} AS DECIMAL(15, 6)) AS exchange_rate,
            u.event_time AS detected_at
        FROM upbit_enriched AS u
        JOIN bithumb_enriched AS bt
          ON u.symbol = bt.symbol
         AND u.event_time BETWEEN bt.event_time - INTERVAL '3' SECOND
                              AND bt.event_time + INTERVAL '3' SECOND
    """)


if __name__ == "__main__":
    main()