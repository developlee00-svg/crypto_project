"""
데이터 정리 DAG (매일 새벽 3시 실행)
- arbitrage_opportunities 테이블에서 7일 이상 지난 데이터 삭제
- daily_stats에 이미 집계 완료된 raw 데이터를 정리하여 MySQL 용량 관리
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

RETENTION_DAYS = 7


# 5. 콜백함수 정의
# Task 1: arbitrage_opportunities 7일 이상 삭제
def cleanup_opportunities(**context):
    """7일 이상 지난 arbitrage_opportunities 삭제"""
    cutoff_date = (datetime.utcnow() - timedelta(days=RETENTION_DAYS)).strftime("%Y-%m-%d")
    logger.info(f"삭제 기준일: {cutoff_date} 이전 데이터")

    conn = pymysql.connect(**MYSQL_CONFIG)
    try:
        with conn.cursor() as cur:
            # 삭제 전 건수 확인
            cur.execute(
                "SELECT COUNT(*) FROM arbitrage_opportunities WHERE detected_at < %s",
                (cutoff_date,),
            )
            target_count = cur.fetchone()[0]
            logger.info(f"삭제 대상: {target_count:,}건")

            if target_count == 0:
                logger.info("삭제할 데이터 없음")
                context["ti"].xcom_push(key="deleted_count", value=0)
                return

            # 배치 삭제 (대량 삭제 시 락 시간 줄이기 위해 1만 건씩)
            total_deleted = 0
            while True:
                cur.execute(
                    """
                    DELETE FROM arbitrage_opportunities
                    WHERE detected_at < %s
                    LIMIT 10000
                    """,
                    (cutoff_date,),
                )
                deleted = cur.rowcount
                conn.commit()
                total_deleted += deleted
                logger.info(f"진행: {total_deleted:,} / {target_count:,}")
                if deleted < 10000:
                    break

            logger.info(f"삭제 완료: {total_deleted:,}건")
            context["ti"].xcom_push(key="deleted_count", value=total_deleted)
            context["ti"].xcom_push(key="cutoff_date", value=cutoff_date)
    finally:
        conn.close()


# Task 2: 정리 결과 로깅
def log_summary(**context):
    """삭제 결과 요약 로그"""
    deleted_count = context["ti"].xcom_pull(
        task_ids="cleanup_opportunities", key="deleted_count"
    )
    cutoff_date = context["ti"].xcom_pull(
        task_ids="cleanup_opportunities", key="cutoff_date"
    )

    # 현재 테이블 상태 확인
    conn = pymysql.connect(**MYSQL_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*), MIN(detected_at), MAX(detected_at) FROM arbitrage_opportunities")
            row_count, min_date, max_date = cur.fetchone()
    finally:
        conn.close()

    logger.info("=" * 50)
    logger.info(f"Cleanup 결과")
    logger.info(f"  - 삭제 기준일: {cutoff_date}")
    logger.info(f"  - 삭제 건수: {deleted_count:,}")
    logger.info(f"  - 잔여 건수: {row_count:,}")
    logger.info(f"  - 잔여 기간: {min_date} ~ {max_date}")
    logger.info("=" * 50)


# 3. DAG 정의
with DAG(
    dag_id="03_dag_data_cleanup",
    description=f"arbitrage_opportunities {RETENTION_DAYS}일 이상 데이터 정리",
    default_args={
        "owner": "crypto",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval="0 3 * * *",      # 매일 새벽 3시 UTC
    start_date=datetime(2026, 4, 29),
    catchup=False,
    tags=["crypto", "cleanup", "maintenance"],
) as dag:

    # 4. Task 정의
    t1 = PythonOperator(
        task_id="cleanup_opportunities",
        python_callable=cleanup_opportunities,
    )

    t2 = PythonOperator(
        task_id="log_summary",
        python_callable=log_summary,
    )

    t1 >> t2