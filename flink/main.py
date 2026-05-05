"""
Crypto Arbitrage Detection - Flink SQL Application (v13)
- 3 exchanges (Binance, Upbit, Bithumb)
- v13: Tumble Window Aggregation + Window Join
  → 1초 슬라이스 단위로 거래소별 평균가 산출 후 같은 슬라이스끼리 매칭
  → Interval Join Cartesian 폭증 차단, append-only 유지
- Detects arbitrage opportunities (Kimchi premium)
- Exchange rate: S3 직접 읽기 (Airflow가 매 1시간 갱신)
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
# 환율 로드 (S3 → 폴백: Runtime Property → 폴백: 1370.0)
# ============================================================
def load_exchange_rate(s3_props: dict) -> float:
    """
    S3에서 환율 JSON 읽기. 실패 시 Runtime Property 폴백, 그것도 실패 시 1370.0.
    """
    bucket = s3_props.get("rate.bucket")
    key = s3_props.get("rate.key")
    fallback = float(s3_props.get("usd.krw.rate", "1370.0"))

    if not bucket or not key:
        print(f"[WARN] S3 bucket/key not configured. Using fallback: {fallback}")
        return fallback

    try:
        import boto3
        s3 = boto3.client("s3", region_name="ap-northeast-2")
        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read().decode("utf-8")
        data = json.loads(body)
        rate = float(data["rate"])
        print(f"[INFO] Loaded exchange rate from s3://{bucket}/{key}: {rate}")
        print(f"[INFO] Fetched at: {data.get('fetched_at', 'N/A')}")
        return rate
    except Exception as e:
        print(f"[ERROR] Failed to load rate from S3: {e}")
        print(f"[WARN] Using fallback rate: {fallback}")
        return fallback


# ============================================================
# main 함수
# ============================================================
def main():
    # --- 환경 설정 ---
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)

    # --- 환율 로드 (S3 우선, 폴백: Runtime Property) ---
    props = get_application_properties()
    s3_props = props.get("s3", {})
    EXCHANGE_RATE = load_exchange_rate(s3_props)
    print(f"[INFO] Final exchange rate applied to SQL: {EXCHANGE_RATE}")

    # ============================================================
    # 1. Source 테이블 (입력 KDS) - 변경 없음
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
    # 2. Sink 테이블 (출력 KDS) - 변경 없음
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
    # 3. 거래소별 KRW 환산 뷰 (event_time, watermark 보존)
    # ============================================================
    table_env.execute_sql(f"""
        CREATE TEMPORARY VIEW binance_enriched AS
        SELECT
            symbol,
            CAST(price * {EXCHANGE_RATE} AS DECIMAL(20, 2)) AS price_krw,
            event_time
        FROM crypto_input
        WHERE `exchange` = 'binance'
    """)

    table_env.execute_sql("""
        CREATE TEMPORARY VIEW upbit_enriched AS
        SELECT
            symbol,
            CAST(price AS DECIMAL(20, 2)) AS price_krw,
            event_time
        FROM crypto_input
        WHERE `exchange` = 'upbit'
    """)

    table_env.execute_sql("""
        CREATE TEMPORARY VIEW bithumb_enriched AS
        SELECT
            symbol,
            CAST(price AS DECIMAL(20, 2)) AS price_krw,
            event_time
        FROM crypto_input
        WHERE `exchange` = 'bithumb'
    """)

    # ============================================================
    # 4. v13: 1초 Tumble Window 집약 (심볼당 1초 1건, append-only)
    #    - AVG로 1초 안 여러 메시지 평균
    #    - window_start: 1초 슬라이스의 시작 시각 (join key로 사용)
    # ============================================================
    table_env.execute_sql("""
        CREATE TEMPORARY VIEW binance_1s AS
        SELECT
            symbol,
            CAST(AVG(price_krw) AS DECIMAL(20, 2)) AS price_krw,
            window_start,
            window_end
        FROM TABLE(
            TUMBLE(TABLE binance_enriched, DESCRIPTOR(event_time), INTERVAL '1' SECOND)
        )
        GROUP BY symbol, window_start, window_end
    """)

    table_env.execute_sql("""
        CREATE TEMPORARY VIEW upbit_1s AS
        SELECT
            symbol,
            CAST(AVG(price_krw) AS DECIMAL(20, 2)) AS price_krw,
            window_start,
            window_end
        FROM TABLE(
            TUMBLE(TABLE upbit_enriched, DESCRIPTOR(event_time), INTERVAL '1' SECOND)
        )
        GROUP BY symbol, window_start, window_end
    """)

    table_env.execute_sql("""
        CREATE TEMPORARY VIEW bithumb_1s AS
        SELECT
            symbol,
            CAST(AVG(price_krw) AS DECIMAL(20, 2)) AS price_krw,
            window_start,
            window_end
        FROM TABLE(
            TUMBLE(TABLE bithumb_enriched, DESCRIPTOR(event_time), INTERVAL '1' SECOND)
        )
        GROUP BY symbol, window_start, window_end
    """)

    # ============================================================
    # 5. INSERT INTO ... (3쌍 Window Join + UNION ALL)
    #    같은 1초 슬라이스끼리만 매칭 (window_start 일치)
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
            u.window_start AS detected_at
        FROM upbit_1s AS u
        JOIN binance_1s AS b
          ON u.symbol = b.symbol
         AND u.window_start = b.window_start

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
            bt.window_start AS detected_at
        FROM bithumb_1s AS bt
        JOIN binance_1s AS b
          ON bt.symbol = b.symbol
         AND bt.window_start = b.window_start

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
            u.window_start AS detected_at
        FROM upbit_1s AS u
        JOIN bithumb_1s AS bt
          ON u.symbol = bt.symbol
         AND u.window_start = bt.window_start
    """)


if __name__ == "__main__":
    main()