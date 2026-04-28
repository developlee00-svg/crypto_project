"""
Kafka → Kinesis Data Streams 브릿지 Consumer
- 3개 Kafka 토픽(crypto-prices-binance/upbit/bithumb) 구독
- 정규화된 메시지를 그대로 KDS(crypto-stream-input)에 배치 전송
- confluent-kafka 사용 (Python 3.14 호환)
"""
import os
import json
import time
import signal
import sys
import logging
from threading import Event

import boto3
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
load_dotenv()

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
    if AWS_ACCESS_KEY and AWS_SECRET_KEY:
        kwargs["aws_access_key_id"] = AWS_ACCESS_KEY
        kwargs["aws_secret_access_key"] = AWS_SECRET_KEY

    client = boto3.client(**kwargs)
    logger.info(f"Kinesis 클라이언트 생성 완료 (region: {AWS_REGION}, stream: {KINESIS_STREAM})")
    return client


# ============================================================
# Kafka Consumer (confluent-kafka)
# ============================================================

def create_consumer() -> Consumer:
    """confluent-kafka Consumer 생성 (3개 토픽 구독)"""
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 5000,
    }
    consumer = Consumer(conf)
    consumer.subscribe(TOPICS)
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
        try:
            resp = kinesis_client.put_records(
                StreamName=KINESIS_STREAM,
                Records=entries,
            )
        except Exception as e:
            logger.error(f"Kinesis put_records 호출 실패: {type(e).__name__}: {e}")
            return total_sent

        failed_count = resp.get("FailedRecordCount", 0)
        sent_count = len(entries) - failed_count
        total_sent += sent_count

        if failed_count == 0:
            entries = []
            break

        retry_entries = []
        for i, result in enumerate(resp["Records"]):
            if "ErrorCode" in result:
                retry_entries.append(entries[i])
                if i < 3:
                    logger.warning(
                        f"Kinesis 전송 실패 [{i}]: {result['ErrorCode']} - {result.get('ErrorMessage', '')}"
                    )

        if len(retry_entries) > 3:
            logger.warning(f"  ... 외 {len(retry_entries) - 3}건 동일 에러")

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
            msg = consumer.poll(timeout=1.0)

            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logger.warning(f"Consumer 에러: {msg.error()}")
                else:
                    data = json.loads(msg.value().decode("utf-8"))
                    batch.append(data)

                    # 배치 사이즈 도달 시 전송
                    if len(batch) >= BATCH_SIZE:
                        sent = send_batch(kinesis_client, batch)
                        total_forwarded += sent
                        logger.info(
                            f"배치 전송: {sent}/{len(batch)}건 (총 {total_forwarded}건)"
                        )
                        batch.clear()
                        last_flush = time.time()

            # 타임아웃 기반 플러시
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