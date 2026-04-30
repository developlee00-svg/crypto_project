"""
Kinesis Data Streams → MySQL 브릿지 (ADR v9, Day 9 신설)

- KDS crypto-stream-output에서 Flink가 발행한 아비트라지 결과 읽기
- MySQL arbitrage_opportunities (Warm Layer)에 실시간 적재
- Shard Iterator 전략: LATEST (재시작 시 최신부터, 유실분은 S3 Bronze에 보관)
- 단일 샤드 가정 (학습 환경, 첫 번째 샤드만 폴링)
- 중복 방지: UNIQUE KEY uk_dedup + INSERT IGNORE

Flink Sink JSON 스키마:
{
  "symbol": "BTC",
  "buy_exchange": "upbit",
  "buy_price_krw": 156000000.00,
  "sell_exchange": "binance",
  "sell_price_krw": 156500000.00,
  "spread_krw": 500000.00,
  "spread_pct": 0.3205,
  "exchange_rate": 1473.250000,
  "detected_at": "2026-04-30 05:54:21.123"
}
"""
import os
import json
import time
import signal
import logging
from threading import Event

import boto3
import pymysql
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# 설정
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("kinesis-mysql-bridge")

# AWS / KDS
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "ap-northeast-2")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
KDS_STREAM = os.getenv("KDS_STREAM_NAME", "crypto-stream-output")

# MySQL
MYSQL_CONFIG = {
    "host": os.environ["CRYPTO_MYSQL_HOST"],
    "port": int(os.environ["CRYPTO_MYSQL_PORT"]),
    "user": os.environ["CRYPTO_MYSQL_USER"],
    "password": os.environ["CRYPTO_MYSQL_PASSWORD"],
    "db": os.environ["CRYPTO_MYSQL_DB"],
    "charset": "utf8mb4",
    "autocommit": False,
}

# 폴링 주기 (Kinesis GetRecords 제한: 샤드당 최대 5 TPS)
POLL_INTERVAL_SEC = 1.0

shutdown_event = Event()


# ============================================================
# Kinesis 클라이언트
# ============================================================

def create_kinesis_client():
    """boto3 Kinesis 클라이언트 생성"""
    kwargs = {
        "service_name": "kinesis",
        "region_name": AWS_REGION,
    }
    if AWS_ACCESS_KEY and AWS_SECRET_KEY:
        kwargs["aws_access_key_id"] = AWS_ACCESS_KEY
        kwargs["aws_secret_access_key"] = AWS_SECRET_KEY

    client = boto3.client(**kwargs)
    logger.info(f"Kinesis 클라이언트 생성 완료 (region: {AWS_REGION}, stream: {KDS_STREAM})")
    return client


def get_first_shard_iterator(kinesis_client) -> str:
    """
    스트림의 첫 번째 샤드에 대한 LATEST iterator 반환.
    (단일 샤드 가정 — 학습 환경)
    """
    resp = kinesis_client.describe_stream(StreamName=KDS_STREAM)
    shards = resp["StreamDescription"]["Shards"]
    if not shards:
        raise RuntimeError(f"스트림 '{KDS_STREAM}'에 샤드가 없습니다.")

    shard_id = shards[0]["ShardId"]
    if len(shards) > 1:
        logger.warning(
            f"샤드가 {len(shards)}개 발견됨. 단일 샤드 모드로 첫 샤드({shard_id})만 폴링합니다."
        )
    else:
        logger.info(f"샤드 ID: {shard_id}")

    iter_resp = kinesis_client.get_shard_iterator(
        StreamName=KDS_STREAM,
        ShardId=shard_id,
        ShardIteratorType="LATEST",
    )
    return iter_resp["ShardIterator"]


# ============================================================
# MySQL 연결
# ============================================================

def create_mysql_connection() -> pymysql.connections.Connection:
    """MySQL 커넥션 생성 (재시도 포함)"""
    while not shutdown_event.is_set():
        try:
            conn = pymysql.connect(**MYSQL_CONFIG)
            logger.info(
                f"MySQL 연결 완료 ({MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_CONFIG['db']})"
            )
            return conn
        except pymysql.MySQLError as e:
            logger.error(f"MySQL 연결 실패: {e}, 5초 후 재시도...")
            shutdown_event.wait(5)
    raise RuntimeError("MySQL 연결 중 종료 신호 수신")


# ============================================================
# 적재
# ============================================================

INSERT_SQL = """
INSERT IGNORE INTO arbitrage_opportunities
    (symbol, buy_exchange, buy_price_krw, sell_exchange, sell_price_krw,
     spread_krw, spread_pct, exchange_rate, detected_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


def parse_record(record: dict) -> tuple | None:
    """
    KDS Record → MySQL INSERT 튜플 변환.
    - record["Data"]: bytes (JSON)
    - 파싱 실패 시 None 반환 (해당 레코드 스킵)
    """
    try:
        data = json.loads(record["Data"].decode("utf-8"))
        return (
            data["symbol"],
            data["buy_exchange"],
            data["buy_price_krw"],
            data["sell_exchange"],
            data["sell_price_krw"],
            data["spread_krw"],
            data["spread_pct"],
            data["exchange_rate"],
            data["detected_at"],  # "YYYY-MM-DD HH:MM:SS.fff" 문자열 → DATETIME(3) 자동 캐스팅
        )
    except (json.JSONDecodeError, KeyError, UnicodeDecodeError) as e:
        logger.warning(f"레코드 파싱 실패: {type(e).__name__}: {e}")
        return None


def insert_batch(conn: pymysql.connections.Connection, rows: list[tuple]) -> int:
    """
    executemany로 일괄 INSERT IGNORE.
    - 중복(UNIQUE KEY uk_dedup)은 자동 무시
    - 실제 적재된 행 수 반환
    """
    if not rows:
        return 0

    with conn.cursor() as cur:
        cur.executemany(INSERT_SQL, rows)
        affected = cur.rowcount
    conn.commit()
    return affected


# ============================================================
# 메인 루프
# ============================================================

def run_bridge():
    kinesis_client = create_kinesis_client()
    shard_iterator = get_first_shard_iterator(kinesis_client)
    conn = create_mysql_connection()

    total_received = 0
    total_inserted = 0
    total_skipped = 0  # 파싱 실패 + 중복

    logger.info("=== Kinesis → MySQL 브릿지 시작 ===")

    try:
        while not shutdown_event.is_set():
            # GetRecords
            try:
                resp = kinesis_client.get_records(
                    ShardIterator=shard_iterator,
                    Limit=500,  # KDS GetRecords 한계
                )
            except Exception as e:
                logger.error(f"Kinesis GetRecords 실패: {type(e).__name__}: {e}")
                shutdown_event.wait(5)
                # iterator 재발급 시도
                try:
                    shard_iterator = get_first_shard_iterator(kinesis_client)
                except Exception as e2:
                    logger.error(f"샤드 iterator 재발급 실패: {e2}")
                    shutdown_event.wait(5)
                continue

            records = resp.get("Records", [])
            shard_iterator = resp.get("NextShardIterator")

            if shard_iterator is None:
                logger.error("NextShardIterator가 None — 샤드가 닫혔거나 만료됨. 재발급 시도")
                shard_iterator = get_first_shard_iterator(kinesis_client)
                continue

            # 파싱
            if records:
                rows = []
                for r in records:
                    parsed = parse_record(r)
                    if parsed is not None:
                        rows.append(parsed)
                    else:
                        total_skipped += 1

                total_received += len(records)

                # MySQL 적재
                try:
                    inserted = insert_batch(conn, rows)
                    duplicated = len(rows) - inserted
                    total_inserted += inserted
                    total_skipped += duplicated
                    logger.info(
                        f"수신 {len(records)} | 적재 {inserted} | 중복/스킵 {duplicated + (len(records) - len(rows))} "
                        f"| 누적 적재 {total_inserted}"
                    )
                except pymysql.MySQLError as e:
                    logger.error(f"MySQL 적재 실패: {e}, 재연결 시도...")
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = create_mysql_connection()

            # 폴링 주기
            shutdown_event.wait(POLL_INTERVAL_SEC)

    except Exception as e:
        logger.error(f"브릿지 오류: {e}", exc_info=True)
    finally:
        try:
            conn.close()
        except Exception:
            pass
        logger.info(
            f"=== 브릿지 종료 (수신 {total_received} / 적재 {total_inserted} / 중복+스킵 {total_skipped}) ==="
        )


# ============================================================
# 엔트리포인트
# ============================================================

def main():
    def shutdown(sig, frame):
        logger.info(f"종료 신호 수신 (signal={sig})")
        shutdown_event.set()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    run_bridge()


if __name__ == "__main__":
    main()