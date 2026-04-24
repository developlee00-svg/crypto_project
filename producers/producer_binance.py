"""
Binance WebSocket Producer
- wss://stream.binance.com:9443/ws 에서 miniTicker 스트림 구독
- 정규화 후 crypto-prices-binance 토픽으로 발행
"""
import asyncio
import json
import logging
import signal
import sys

import websockets

from common import create_kafka_producer, get_common_symbols, normalize_message

logger = logging.getLogger("producer-binance")

TOPIC = "crypto-prices-binance"
WS_URL = "wss://stream.binance.com:9443/ws"


async def run_producer():
    symbols = get_common_symbols()
    producer = create_kafka_producer()

    # Binance miniTicker 스트림 이름: <symbol>usdt@miniTicker
    streams = [f"{s.lower()}usdt@miniTicker" for s in symbols]

    logger.info(f"Binance 구독 스트림 수: {len(streams)}")

    while True:
        try:
            async with websockets.connect(
                WS_URL,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=5,
            ) as ws:
                # 구독 요청 (combined stream 방식)
                subscribe_msg = {
                    "method": "SUBSCRIBE",
                    "params": streams,
                    "id": 1,
                }
                await ws.send(json.dumps(subscribe_msg))
                logger.info("Binance WebSocket 연결 및 구독 완료")

                async for raw in ws:
                    try:
                        data = json.loads(raw)

                        # 구독 응답은 무시
                        if "result" in data or "id" in data:
                            continue

                        # miniTicker 이벤트: e="24hrMiniTicker"
                        if data.get("e") != "24hrMiniTicker":
                            continue

                        # 심볼 파싱: "BTCUSDT" → "BTC"
                        raw_symbol = data["s"]  # e.g. "BTCUSDT"
                        if not raw_symbol.endswith("USDT"):
                            continue
                        symbol = raw_symbol.replace("USDT", "")

                        price = float(data["c"])  # 현재가 (close)

                        msg = normalize_message(
                            exchange="binance",
                            symbol=symbol,
                            price=price,
                            currency="USDT",
                        )

                        producer.send(TOPIC, key=symbol, value=msg)
                        logger.debug(f"Binance | {symbol}: {price} USDT")

                    except (KeyError, ValueError) as e:
                        logger.warning(f"메시지 파싱 오류: {e}")

        except websockets.ConnectionClosed as e:
            logger.warning(f"Binance WebSocket 연결 끊김: {e}, 5초 후 재연결...")
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"Binance Producer 오류: {e}, 5초 후 재연결...")
            await asyncio.sleep(5)


def main():
    loop = asyncio.new_event_loop()

    def shutdown(sig, frame):
        logger.info("종료 신호 수신, Producer 중지...")
        loop.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    logger.info("Binance Producer 시작")
    loop.run_until_complete(run_producer())


if __name__ == "__main__":
    main()