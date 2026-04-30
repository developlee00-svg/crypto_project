"""
일별 통계 집계 DAG (매일 자정 실행) — ADR v9

변경 사유 (v8 → v9):
  v8: S3 Bronze GZIP → arbitrage_opportunities 적재 → daily_stats 집계 (3단계)
  v9: arbitrage_opportunities는 kinesis-mysql-bridge가 실시간으로 이미 적재함.
      따라서 S3 추출/적재 단계 불필요. MySQL에서 직접 GROUP BY (1단계).

  Bronze(S3)는 영구 archive 전용으로 분리 (재처리/Athena 분석용).
"""
# 1. 모듈 가져오기
import os
import logging
from datetime import datetime, timedelta

import pymysql
from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

# 2. 환경 변수 정의
MYSQL_CONFIG = {
    "host": os.environ["CRYPTO_MYSQL_HOST"],
    "port": int(os.environ["CRYPTO_MYSQL_PORT"]),
    "user": os.environ["CRYPTO_MYSQL_USER"],
    "password": os.environ["CRYPTO_MYSQL_PASSWORD"],
    "db": os.environ["CRYPTO_MYSQL_DB"],
}


# 5. 콜백함수 정의
# Task 1: 어제 날짜 결정
def resolve_target_date(**context):
    """실행일(logical_date) 기준 어제 날짜를 다음 task로 전달"""
    logical_date = context["logical_date"]
    target_date = (logical_date - timedelta(days=1)).strftime("%Y-%m-%d")
    logger.info(f"집계 대상 날짜: {target_date}")
    context["ti"].xcom_push(key="target_date", value=target_date)


# Task 2: daily_stats 집계 (멱등)
def aggregate_daily_stats(**context):
    """arbitrage_opportunities (Warm) → daily_stats (Gold) 일별 집계"""
    target_date = context["ti"].xcom_pull(
        task_ids="resolve_target_date", key="target_date"
    )

    conn = pymysql.connect(**MYSQL_CONFIG)
    try:
        with conn.cursor() as cur:
            sql = """
                INSERT INTO daily_stats
                    (symbol, date,
                     avg_spread_pct, max_spread_pct, min_spread_pct,
                     opportunity_count)
                SELECT
                    symbol,
                    DATE(detected_at) AS date,
                    AVG(spread_pct),
                    MAX(spread_pct),
                    MIN(spread_pct),
                    COUNT(*)
                FROM arbitrage_opportunities
                WHERE DATE(detected_at) = %s
                GROUP BY symbol, DATE(detected_at)
                ON DUPLICATE KEY UPDATE
                    avg_spread_pct    = VALUES(avg_spread_pct),
                    max_spread_pct    = VALUES(max_spread_pct),
                    min_spread_pct    = VALUES(min_spread_pct),
                    opportunity_count = VALUES(opportunity_count)
            """
            cur.execute(sql, (target_date,))
            affected = cur.rowcount
        conn.commit()
        logger.info(
            f"daily_stats 집계 완료: date={target_date}, affected={affected} rows"
        )
    finally:
        conn.close()


# 3. DAG 정의
with DAG(
    dag_id="02_dag_daily_stats",
    description="MySQL Warm (arbitrage_opportunities) → MySQL Gold (daily_stats) 일별 집계 [ADR v9]",
    default_args={
        "owner": "crypto",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval="0 0 * * *",      # 매일 자정 UTC
    start_date=datetime(2026, 4, 29),
    catchup=False,
    tags=["crypto", "daily-stats", "medallion"],
) as dag:

    # 4. Task 정의
    t1 = PythonOperator(
        task_id="resolve_target_date",
        python_callable=resolve_target_date,
    )

    t2 = PythonOperator(
        task_id="aggregate_daily_stats",
        python_callable=aggregate_daily_stats,
    )

    t1 >> t2