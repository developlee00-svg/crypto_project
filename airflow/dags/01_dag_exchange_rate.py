"""
환율 수집 DAG (매시간 실행)
- FreeExchangeRateApi → MySQL exchange_rates 테이블 + S3 업로드
"""
# 1. 모듈 가져오기
import os
import json
import logging
from datetime import datetime, timedelta

import requests
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
S3_RATE_KEY = os.environ["S3_RATE_KEY"]
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-2")

EXCHANGE_API_URL = "https://api.exchangerate.fun/latest?base=USD"

# 5. 콜백함수 정의
# Task 1: 환율 API 호출
def fetch_exchange_rate(**context):
    """FreeExchangeRateApi에서 USD/KRW 환율 조회"""
    response = requests.get(EXCHANGE_API_URL, timeout=10)
    response.raise_for_status()
    data = response.json()

    krw_rate = data["rates"]["KRW"]
    api_timestamp = data.get("timestamp")
    fetched_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    logger.info(f"USD/KRW = {krw_rate} (api_ts: {api_timestamp})")

    # 다음 task로 전달 (XCom)
    context["ti"].xcom_push(key="krw_rate", value=krw_rate)
    context["ti"].xcom_push(key="fetched_at", value=fetched_at)


# Task 2: MySQL 적재
def save_to_mysql(**context):
    """exchange_rates 테이블에 INSERT"""
    krw_rate = context["ti"].xcom_pull(
        task_ids="fetch_exchange_rate", key="krw_rate"
    )
    fetched_at = context["ti"].xcom_pull(
        task_ids="fetch_exchange_rate", key="fetched_at"
    )

    conn = pymysql.connect(**MYSQL_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO exchange_rates
                    (base_currency, target_currency, rate, fetched_at)
                VALUES (%s, %s, %s, %s)
                """,
                ("USD", "KRW", krw_rate, fetched_at),
            )
        conn.commit()
        logger.info(f"MySQL 적재 완료: USD/KRW = {krw_rate}")
    finally:
        conn.close()


# Task 3: S3 업로드 (Flink가 읽을 환율 파일)
def upload_to_s3(**context):
    """S3에 환율 JSON 업로드"""
    krw_rate = context["ti"].xcom_pull(
        task_ids="fetch_exchange_rate", key="krw_rate"
    )
    fetched_at = context["ti"].xcom_pull(
        task_ids="fetch_exchange_rate", key="fetched_at"
    )

    payload = {
        "base": "USD",
        "target": "KRW",
        "rate": krw_rate,
        "fetched_at": fetched_at,
    }

    s3 = boto3.client("s3", region_name=AWS_REGION)
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=S3_RATE_KEY,
        Body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )
    logger.info(f"S3 업로드 완료: s3://{S3_BUCKET}/{S3_RATE_KEY}")


# 3. DAG 정의
with DAG(
    dag_id="01_dag_exchange_rate",
    description="USD/KRW 환율 수집 → MySQL + S3",
    default_args = {
        "owner": "crypto",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    schedule_interval="0 * * * *",      # 매시 정각
    start_date=datetime(2026, 4, 29),
    catchup=False,
    tags=["crypto", "exchange-rate"],
) as dag:
    
    # 4. Task 정의
    t1 = PythonOperator(
        task_id="fetch_exchange_rate",
        python_callable=fetch_exchange_rate,
    )

    t2 = PythonOperator(
        task_id="save_to_mysql",
        python_callable=save_to_mysql,
    )

    t3 = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
    )

    t1 >> [t2, t3]   # fetch 후 MySQL/S3 병렬 실행