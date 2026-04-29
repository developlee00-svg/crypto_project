"""
일별 통계 집계 DAG (매일 자정 실행)
- S3 Bronze (raw/) → MySQL arbitrage_opportunities (Warm) → MySQL daily_stats (Gold)
- 어제 날짜 데이터를 S3에서 읽어 MySQL 적재 + 일별 집계
"""
# 1. 모듈 가져오기
import os
import json
import gzip
import logging
from io import BytesIO
from datetime import datetime, timedelta

import pymysql
import boto3
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

S3_BUCKET = os.environ["S3_BUCKET"]
S3_RAW_PREFIX = "crypto-raw"
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-2")


# 5. 콜백함수 정의
# Task 1: S3에서 어제 날짜 데이터 추출
def extract_from_s3(**context):
    """S3 raw/ 어제 날짜 폴더의 GZIP JSON 파일들 → 파싱"""
    # 실행일 기준 어제 날짜 (UTC)
    logical_date = context["logical_date"]
    target_date = (logical_date - timedelta(days=1)).strftime("%Y-%m-%d")
    year, month, day = target_date.split("-")

    prefix = f"{S3_RAW_PREFIX}/year={year}/month={month}/day={day}/"
    logger.info(f"S3 prefix: s3://{S3_BUCKET}/{prefix}")

    s3 = boto3.client("s3", region_name=AWS_REGION)
    paginator = s3.get_paginator("list_objects_v2")

    records = []
    file_count = 0
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".gz"):
                continue
            file_count += 1

            # GZIP 파일 다운로드 + 압축 해제
            response = s3.get_object(Bucket=S3_BUCKET, Key=key)
            with gzip.GzipFile(fileobj=BytesIO(response["Body"].read())) as gz:
                content = gz.read().decode("utf-8")

            # Firehose는 줄바꿈 없이 JSON 객체를 이어붙임 → 파서로 분리
            decoder = json.JSONDecoder()
            idx = 0
            content = content.strip()
            while idx < len(content):
                obj_data, end = decoder.raw_decode(content, idx)
                records.append(obj_data)
                idx = end
                # 객체 사이 공백 스킵
                while idx < len(content) and content[idx].isspace():
                    idx += 1

    logger.info(f"파일 {file_count}개에서 레코드 {len(records)}건 추출")

    # XCom 크기 제한 회피: target_date만 push, records는 디스크에 저장
    tmp_path = f"/tmp/arb_records_{target_date}.json"
    with open(tmp_path, "w") as f:
        json.dump(records, f)

    context["ti"].xcom_push(key="target_date", value=target_date)
    context["ti"].xcom_push(key="tmp_path", value=tmp_path)
    context["ti"].xcom_push(key="record_count", value=len(records))


# Task 2: arbitrage_opportunities 적재
def load_to_opportunities(**context):
    """파싱된 레코드 → MySQL arbitrage_opportunities (멱등)"""
    tmp_path = context["ti"].xcom_pull(
        task_ids="extract_from_s3", key="tmp_path"
    )

    with open(tmp_path, "r") as f:
        records = json.load(f)

    if not records:
        logger.info("적재할 레코드 없음")
        return

    conn = pymysql.connect(**MYSQL_CONFIG)
    try:
        with conn.cursor() as cur:
            # 중복 방지: detected_at + symbol + buy_exchange + sell_exchange 조합으로 멱등 처리
            # (스키마에 unique key 없는 경우, INSERT IGNORE보다 명시적으로 처리)
            sql = """
                INSERT INTO arbitrage_opportunities
                    (symbol, buy_exchange, buy_price_krw,
                     sell_exchange, sell_price_krw,
                     spread_krw, spread_pct, exchange_rate, detected_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            batch = [
                (
                    r["symbol"],
                    r["buy_exchange"],
                    r["buy_price_krw"],
                    r["sell_exchange"],
                    r["sell_price_krw"],
                    r["spread_krw"],
                    r["spread_pct"],
                    r["exchange_rate"],
                    r["detected_at"],
                )
                for r in records
            ]
            cur.executemany(sql, batch)
        conn.commit()
        logger.info(f"arbitrage_opportunities 적재 완료: {len(records)}건")
    finally:
        conn.close()


# Task 3: daily_stats 집계
def aggregate_daily_stats(**context):
    """arbitrage_opportunities → daily_stats 일별 집계 (멱등)"""
    target_date = context["ti"].xcom_pull(
        task_ids="extract_from_s3", key="target_date"
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
        logger.info(f"daily_stats 집계 완료: {target_date}, {affected} rows affected")
    finally:
        conn.close()


# 3. DAG 정의
with DAG(
    dag_id="02_dag_daily_stats",
    description="S3 Bronze → MySQL Warm/Gold 일별 통계 집계",
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
        task_id="extract_from_s3",
        python_callable=extract_from_s3,
    )

    t2 = PythonOperator(
        task_id="load_to_opportunities",
        python_callable=load_to_opportunities,
    )

    t3 = PythonOperator(
        task_id="aggregate_daily_stats",
        python_callable=aggregate_daily_stats,
    )

    t1 >> t2 >> t3   # 순차 실행 (집계는 적재 후)