"""
Kafka 토픽 수신 테스트 스크립트
3개 토픽의 메시지를 실시간으로 출력하여 Producer 동작 확인

사용법:
  pip install kafka-python-ng
  python test_consumer.py
"""
import json
from kafka import KafkaConsumer

TOPICS = [
    "crypto-prices-binance",
    "crypto-prices-upbit",
    "crypto-prices-bithumb",
]

consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="latest",
    group_id="test-consumer",
)

print(f"구독 토픽: {TOPICS}")
print("메시지 대기 중... (Ctrl+C로 종료)\n")

try:
    for message in consumer:
        data = message.value
        topic = message.topic.replace("crypto-prices-", "")
        print(
            f"[{topic:>8}] {data['symbol']:>5} | "
            f"{data['price']:>15,.2f} {data['currency']} | "
            f"{data['timestamp']}"
        )
except KeyboardInterrupt:
    print("\n종료")
finally:
    consumer.close()