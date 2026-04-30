"""
/prices, /prices/{symbol}
- 데이터 소스: Kafka 직접 consume (메모리 dict)
- binance는 USDT → KRW 환산 추가 (exchange_rates 테이블 최신값)
"""
from fastapi import APIRouter, HTTPException

from services.kafka_cache import price_cache
from services.mysql_pool import get_latest_usd_krw

router = APIRouter(tags=["prices"])


def _to_response_entry(exchange: str, raw: dict, usd_krw: float | None) -> dict:
    """
    캐시 raw 항목 → 응답 entry 변환.
    - upbit/bithumb: price (KRW) → price_krw 그대로
    - binance: price (USDT) → price_krw = price * usd_krw
              + price_usdt 원본 보존
    """
    price = raw["price"]
    currency = raw["currency"]
    timestamp = raw["timestamp"]

    if exchange == "binance":
        price_usdt = price
        if usd_krw is None:
            # 환율이 없으면 KRW 환산 불가 → 0으로 두지 말고 None
            return {
                "price_krw": None,
                "price_usdt": price_usdt,
                "timestamp": timestamp,
            }
        return {
            "price_krw": round(price_usdt * usd_krw, 2),
            "price_usdt": price_usdt,
            "timestamp": timestamp,
        }
    else:
        # upbit, bithumb은 이미 KRW
        return {
            "price_krw": price,
            "price_usdt": None,
            "timestamp": timestamp,
        }


def _build_symbol_block(ex_map: dict, usd_krw: float | None) -> dict:
    """단일 심볼의 거래소별 dict 생성"""
    return {
        ex: _to_response_entry(ex, raw, usd_krw)
        for ex, raw in ex_map.items()
    }


@router.get("/prices")
async def get_all_prices() -> dict:
    """
    50개 코인 전체 시세 (거래소별).

    응답 예:
    {
      "BTC": {
        "upbit":   {"price_krw": 156000000, "price_usdt": null, "timestamp": "..."},
        "binance": {"price_krw": 99450000,  "price_usdt": 67500, "timestamp": "..."},
        "bithumb": {"price_krw": 155900000, "price_usdt": null, "timestamp": "..."}
      },
      ...
    }

    데이터가 아직 안 들어온 거래소는 키가 없음.
    """
    snapshot = await price_cache.get_all()
    usd_krw = await get_latest_usd_krw()

    return {
        symbol: _build_symbol_block(ex_map, usd_krw)
        for symbol, ex_map in snapshot.items()
    }


@router.get("/prices/{symbol}")
async def get_symbol_price(symbol: str) -> dict:
    """
    특정 코인의 거래소별 시세.

    응답 예 (/prices/BTC):
    {
      "upbit":   {"price_krw": 156000000, "price_usdt": null, "timestamp": "..."},
      "binance": {"price_krw": 99450000,  "price_usdt": 67500, "timestamp": "..."},
      "bithumb": {"price_krw": 155900000, "price_usdt": null, "timestamp": "..."}
    }
    """
    symbol = symbol.upper()
    ex_map = await price_cache.get_symbol(symbol)
    if ex_map is None:
        raise HTTPException(
            status_code=404,
            detail=f"심볼 '{symbol}' 시세가 캐시에 없습니다 (아직 수신 전이거나 미상장).",
        )

    usd_krw = await get_latest_usd_krw()
    return _build_symbol_block(ex_map, usd_krw)