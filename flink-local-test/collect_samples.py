"""
Step 1: Kafka 시세 데이터 수집기
- 3개 토픽에서 지정 시간 동안 데이터를 수집하여 CSV로 저장
- 이후 pandas 조인 검증에 사용

사용법:
  pip install confluent-kafka
  python collect_samples.py          # 기본 60초 수집
  python collect_samples.py --seconds 120
"""
import argparse
import csv
import json
import time
import logging
from confluent_kafka import Consumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("collector")

TOPICS = [
    "crypto-prices-binance",
    "crypto-prices-upbit",
    "crypto-prices-bithumb",
]
OUTPUT_FILE = "sample_data.csv"


def collect(seconds: int, bootstrap: str):
    conf = {
        "bootstrap.servers": bootstrap,
        "group.id": f"collector-{int(time.time())}",
        "auto.offset.reset": "latest",
    }
    consumer = Consumer(conf)
    consumer.subscribe(TOPICS)

    rows = []
    start = time.time()
    logger.info(f"{seconds}초 동안 데이터 수집 시작...")

    try:
        while time.time() - start < seconds:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logger.warning(f"Consumer 에러: {msg.error()}")
                continue

            data = json.loads(msg.value().decode("utf-8"))
            rows.append(data)
    finally:
        consumer.close()

    # CSV 저장
    if not rows:
        logger.warning("수집된 데이터가 없습니다. Producer가 실행 중인지 확인하세요.")
        return

    fieldnames = ["exchange", "symbol", "price", "currency", "timestamp"]
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    # 통계
    exchanges = {}
    for r in rows:
        ex = r["exchange"]
        exchanges[ex] = exchanges.get(ex, 0) + 1

    logger.info(f"수집 완료: 총 {len(rows)}건 → {OUTPUT_FILE}")
    for ex, cnt in sorted(exchanges.items()):
        logger.info(f"  {ex}: {cnt}건")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka 시세 데이터 수집")
    parser.add_argument("--seconds", type=int, default=60, help="수집 시간(초)")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap 서버")
    args = parser.parse_args()

    collect(args.seconds, args.bootstrap)