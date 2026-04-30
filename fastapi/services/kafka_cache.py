"""
Kafka 시세 캐시 서비스
- 3개 토픽(crypto-prices-binance/upbit/bithumb)을 aiokafka로 consume
- 메모리 dict에 거래소별 최신 시세 저장
- /prices, /prices/{symbol} 엔드포인트의 데이터 소스

ADR 2.13:
- Consumer Group: "fastapi-prices-{uuid}" — 인스턴스별 독립 group, 매 시작마다 새 group
- 시작 위치: latest
- 동기화: asyncio.Lock (조회/갱신 race 방지)
"""
import os
import json
import uuid
import asyncio
import logging

from aiokafka import AIOKafkaConsumer

logger = logging.getLogger("kafka_cache")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

TOPICS = [
    "crypto-prices-binance",
    "crypto-prices-upbit",
    "crypto-prices-bithumb",
]


class PriceCache:
    """
    거래소별 최신 시세 메모리 캐시.

    내부 구조:
        _cache[symbol][exchange] = {
            "price": float,        # 원본 가격 (binance: USDT, upbit/bithumb: KRW)
            "currency": str,       # "USDT" | "KRW"
            "timestamp": str,      # ISO 8601
        }
    """

    def __init__(self):
        self._cache: dict[str, dict[str, dict]] = {}
        self._lock = asyncio.Lock()
        self._consumer: AIOKafkaConsumer | None = None
        self._task: asyncio.Task | None = None

    async def start(self):
        """Kafka consumer 시작 + 백그라운드 consume 태스크 등록"""
        group_id = f"fastapi-prices-{uuid.uuid4()}"
        self._consumer = AIOKafkaConsumer(
            *TOPICS,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id=group_id,
            auto_offset_reset="latest",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            enable_auto_commit=True,
        )
        await self._consumer.start()
        logger.info(f"Kafka consumer 시작 (group={group_id}, topics={TOPICS})")

        self._task = asyncio.create_task(self._consume_loop())

    async def stop(self):
        """Kafka consumer 정리"""
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._consumer:
            await self._consumer.stop()
            logger.info("Kafka consumer 종료")

    async def _consume_loop(self):
        """백그라운드: Kafka 메시지를 받아 캐시 갱신"""
        try:
            async for msg in self._consumer:
                data = msg.value
                symbol = data.get("symbol")
                exchange = data.get("exchange")
                price = data.get("price")
                currency = data.get("currency")
                timestamp = data.get("timestamp")

                if not all([symbol, exchange, price is not None, currency, timestamp]):
                    continue

                async with self._lock:
                    if symbol not in self._cache:
                        self._cache[symbol] = {}
                    self._cache[symbol][exchange] = {
                        "price": float(price),
                        "currency": currency,
                        "timestamp": timestamp,
                    }
        except asyncio.CancelledError:
            logger.info("Kafka consume 루프 취소")
            raise
        except Exception as e:
            logger.error(f"Kafka consume 루프 오류: {e}", exc_info=True)

    async def get_all(self) -> dict[str, dict[str, dict]]:
        """전체 캐시 스냅샷 (얕은 복사)"""
        async with self._lock:
            # symbol → exchange → dict 까지 복사
            return {
                sym: {ex: dict(d) for ex, d in ex_map.items()}
                for sym, ex_map in self._cache.items()
            }

    async def get_symbol(self, symbol: str) -> dict[str, dict] | None:
        """특정 심볼의 거래소별 시세"""
        async with self._lock:
            ex_map = self._cache.get(symbol)
            if ex_map is None:
                return None
            return {ex: dict(d) for ex, d in ex_map.items()}


# 전역 싱글톤 (main.py의 lifespan에서 start/stop)
price_cache = PriceCache()