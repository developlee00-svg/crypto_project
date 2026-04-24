"""
Upbit WebSocket Producer
- wss://api.upbit.com/websocket/v1 에서 ticker 구독
- 정규화 후 crypto-prices-upbit 토픽으로 발행
"""
import asyncio
import json
import logging
import signal
import sys
import uuid

import websockets

from common import create_kafka_producer, get_common_symbols, normalize_message

logger = logging.getLogger("producer-upbit")

TOPIC = "crypto-prices-upbit"
WS_URL = "wss://api.upbit.com/websocket/v1"


async def run_producer():
    symbols = get_common_symbols()
    producer = create_kafka_producer()

    # Upbit 코드 형식: "KRW-BTC", "KRW-ETH"
    codes = [f"KRW-{s}" for s in symbols]
    logger.info(f"Upbit 구독 종목 수: {len(codes)}")

    while True:
        try:
            async with websockets.connect(
                WS_URL,
                ping_interval=30,
                ping_timeout=10,
                close_timeout=5,
            ) as ws:
                # Upbit WebSocket 구독 메시지
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
                logger.info("Upbit WebSocket 연결 및 구독 완료")

                async for raw in ws:
                    try:
                        # Upbit은 바이너리(bytes)로 응답
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
                            exchange="upbit",
                            symbol=symbol,
                            price=price,
                            currency="KRW",
                        )

                        producer.send(TOPIC, key=symbol, value=msg)
                        logger.debug(f"Upbit | {symbol}: {price} KRW")

                    except (KeyError, ValueError) as e:
                        logger.warning(f"메시지 파싱 오류: {e}")

        except websockets.ConnectionClosed as e:
            logger.warning(f"Upbit WebSocket 연결 끊김: {e}, 5초 후 재연결...")
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"Upbit Producer 오류: {e}, 5초 후 재연결...")
            await asyncio.sleep(5)


def main():
    loop = asyncio.new_event_loop()

    def shutdown(sig, frame):
        logger.info("종료 신호 수신, Producer 중지...")
        loop.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    logger.info("Upbit Producer 시작")
    loop.run_until_complete(run_producer())


if __name__ == "__main__":
    main()