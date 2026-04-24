"""
Bithumb WebSocket Producer
- wss://ws-api.bithumb.com/websocket/v1 에서 ticker 구독
- 정규화 후 crypto-prices-bithumb 토픽으로 발행

참고: Bithumb WebSocket API v2.1.5 기준
- Upbit과 유사한 구독 형식 (ticket + type + codes)
- 응답도 유사한 JSON 구조
"""
import asyncio
import json
import logging
import signal
import sys
import uuid

import websockets

from common import (
    create_kafka_producer,
    get_common_symbols,
    get_bithumb_krw_symbols,
    normalize_message,
)

logger = logging.getLogger("producer-bithumb")

TOPIC = "crypto-prices-bithumb"
WS_URL = "wss://ws-api.bithumb.com/websocket/v1"


async def run_producer():
    symbols = get_common_symbols()
    producer = create_kafka_producer()

    # Bithumb에 상장된 공통 종목만 필터
    try:
        bithumb_symbols = get_bithumb_krw_symbols()
        target_symbols = [s for s in symbols if s in bithumb_symbols]
    except Exception as e:
        logger.warning(f"Bithumb 종목 조회 실패, 전체 공통 종목 사용: {e}")
        target_symbols = symbols

    # Bithumb 코드 형식: "KRW-BTC", "KRW-ETH"
    codes = [f"KRW-{s}" for s in target_symbols]
    logger.info(f"Bithumb 구독 종목 수: {len(codes)}")

    while True:
        try:
            async with websockets.connect(
                WS_URL,
                ping_interval=30,
                ping_timeout=10,
                close_timeout=5,
            ) as ws:
                # Bithumb WebSocket 구독 메시지 (Upbit과 유사한 형식)
                subscribe_msg = [
                    {"ticket": str(uuid.uuid4())},
                    {
                        "type": "ticker",
                        "codes": codes,
                        "isOnlyRealtime": True,
                    },
                    {"format": "DEFAULT"},
                ]
                await ws.send(json.dumps(subscribe_msg))
                logger.info("Bithumb WebSocket 연결 및 구독 완료")

                async for raw in ws:
                    try:
                        # 바이너리 응답 처리
                        if isinstance(raw, bytes):
                            data = json.loads(raw.decode("utf-8"))
                        else:
                            data = json.loads(raw)

                        # ticker 타입만 처리
                        if data.get("type") != "ticker":
                            continue

                        # 심볼 파싱: "KRW-BTC" → "BTC"
                        code = data.get("code", "")
                        if not code.startswith("KRW-"):
                            continue
                        symbol = code.replace("KRW-", "")

                        price = float(data["trade_price"])  # 현재가

                        msg = normalize_message(
                            exchange="bithumb",
                            symbol=symbol,
                            price=price,
                            currency="KRW",
                        )

                        producer.send(TOPIC, key=symbol, value=msg)
                        logger.debug(f"Bithumb | {symbol}: {price} KRW")

                    except (KeyError, ValueError) as e:
                        logger.warning(f"메시지 파싱 오류: {e}")

        except websockets.ConnectionClosed as e:
            logger.warning(f"Bithumb WebSocket 연결 끊김: {e}, 5초 후 재연결...")
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"Bithumb Producer 오류: {e}, 5초 후 재연결...")
            await asyncio.sleep(5)


def main():
    loop = asyncio.new_event_loop()

    def shutdown(sig, frame):
        logger.info("종료 신호 수신, Producer 중지...")
        loop.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    logger.info("Bithumb Producer 시작")
    loop.run_until_complete(run_producer())


if __name__ == "__main__":
    main()