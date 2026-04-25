"""
Kafka → Kinesis Data Streams 브릿지 Consumer
- 3개 Kafka 토픽(crypto-prices-binance/upbit/bithumb) 구독
- 정규화된 메시지를 그대로 KDS(crypto-stream-input)에 배치 전송
"""
import os
import json
import time
import signal
import sys
import logging
from threading import Event

import boto3
from kafka import KafkaConsumer

# ============================================================
# 설정
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("kafka-kinesis-bridge")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "kinesis-bridge-group")

KINESIS_STREAM = os.getenv("KINESIS_STREAM_NAME", "crypto-stream-input")
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-2")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")

TOPICS = [
    "crypto-prices-binance",
    "crypto-prices-upbit",
    "crypto-prices-bithumb",
]

# 배치 설정
BATCH_SIZE = 100          # Kinesis put_records 최대 500, 여유 있게 100
BATCH_TIMEOUT_SEC = 1.0   # 배치가 안 차도 1초마다 전송

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
    # 로컬 테스트 시 자격 증명이 없으면 환경 변수 / IAM 역할에 위임
    if AWS_ACCESS_KEY and AWS_SECRET_KEY:
        kwargs["aws_access_key_id"] = AWS_ACCESS_KEY
        kwargs["aws_secret_access_key"] = AWS_SECRET_KEY

    client = boto3.client(**kwargs)
    logger.info(f"Kinesis 클라이언트 생성 완료 (region: {AWS_REGION}, stream: {KINESIS_STREAM})")
    return client


# ============================================================
# Kafka Consumer
# ============================================================

def create_consumer() -> KafkaConsumer:
    """Kafka Consumer 생성 (3개 토픽 구독)"""
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        auto_commit_interval_ms=5000,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=1000,   # poll 타임아웃 → 배치 주기 제어용
    )
    logger.info(f"Kafka Consumer 생성 완료 (topics: {TOPICS})")
    return consumer


# ============================================================
# 배치 전송
# ============================================================

def send_batch(kinesis_client, records: list[dict]) -> int:
    """
    Kinesis put_records 배치 전송.
    - PartitionKey: symbol (같은 심볼은 같은 샤드로)
    - 실패 레코드는 재시도
    - 전송 성공 건수 반환
    """
    if not records:
        return 0

    entries = []
    for rec in records:
        entries.append({
            "Data": json.dumps(rec, ensure_ascii=False).encode("utf-8"),
            "PartitionKey": rec.get("symbol", "unknown"),
        })

    total_sent = 0
    retry_count = 0
    max_retries = 3

    while entries and retry_count < max_retries:
        resp = kinesis_client.put_records(
            StreamName=KINESIS_STREAM,
            Records=entries,
        )

        failed_count = resp.get("FailedRecordCount", 0)
        sent_count = len(entries) - failed_count
        total_sent += sent_count

        if failed_count == 0:
            break

        # 실패한 레코드만 추출하여 재시도
        retry_entries = []
        for i, result in enumerate(resp["Records"]):
            if "ErrorCode" in result:
                retry_entries.append(entries[i])
                logger.warning(
                    f"Kinesis 전송 실패: {result['ErrorCode']} - {result.get('ErrorMessage', '')}"
                )

        entries = retry_entries
        retry_count += 1

        if entries:
            backoff = 0.1 * (2 ** retry_count)
            logger.info(f"재시도 {retry_count}/{max_retries} ({len(entries)}건, {backoff:.1f}초 대기)")
            time.sleep(backoff)

    if entries:
        logger.error(f"최종 전송 실패: {len(entries)}건 (max retries 초과)")

    return total_sent


# ============================================================
# 메인 루프
# ============================================================

def run_bridge():
    """Kafka → Kinesis 브릿지 메인 루프"""
    kinesis_client = create_kinesis_client()
    consumer = create_consumer()

    batch: list[dict] = []
    last_flush = time.time()
    total_forwarded = 0

    logger.info("=== Kafka → Kinesis 브릿지 시작 ===")

    try:
        while not shutdown_event.is_set():
            # consumer_timeout_ms=1000 이므로 최대 1초 대기 후 StopIteration
            try:
                for message in consumer:
                    batch.append(message.value)

                    # 배치 사이즈 도달 시 전송
                    if len(batch) >= BATCH_SIZE:
                        sent = send_batch(kinesis_client, batch)
                        total_forwarded += sent
                        logger.info(
                            f"배치 전송: {sent}/{len(batch)}건 (총 {total_forwarded}건)"
                        )
                        batch.clear()
                        last_flush = time.time()

                    if shutdown_event.is_set():
                        break

            except StopIteration:
                pass

            # 타임아웃 기반 플러시: 배치에 데이터가 있으면 전송
            elapsed = time.time() - last_flush
            if batch and elapsed >= BATCH_TIMEOUT_SEC:
                sent = send_batch(kinesis_client, batch)
                total_forwarded += sent
                logger.info(
                    f"타임아웃 플러시: {sent}/{len(batch)}건 (총 {total_forwarded}건)"
                )
                batch.clear()
                last_flush = time.time()

    except Exception as e:
        logger.error(f"브릿지 오류: {e}", exc_info=True)
    finally:
        # 잔여 배치 전송
        if batch:
            sent = send_batch(kinesis_client, batch)
            total_forwarded += sent
            logger.info(f"종료 전 잔여 플러시: {sent}건")

        consumer.close()
        logger.info(f"=== 브릿지 종료 (총 전달: {total_forwarded}건) ===")


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