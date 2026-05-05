"""
공통 유틸리티 모듈
- 시가총액 상위 50개 코인 (하드코딩)
- 거래소별 실제 상장 종목 필터링
- Kafka Producer 생성
- 정규화된 메시지 스키마 생성
"""
import os
import json
import logging
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("common")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


# ============================================================
# 시가총액 상위 코인 (2026-03 기준 하드코딩)
# ============================================================
# 정책: Upbit/Binance/Bithumb 중 2개 이상 상장된 코인만 김프 산출 가능.
# 1곳만 상장된 코인은 데이터가 1개 거래소에서만 들어와 프론트에서 빈 셀로 보임.
# 이는 의도된 동작 — 슬롯은 고정, 데이터 있는 칸만 채워짐.
#
# v11 (2026-05-05): 39 → 97개로 확장 (시총 상위 + 활발한 알트 포함).
#   거래소별 미상장 심볼은 get_symbols_for_exchange()에서 자동 필터링되므로
#   3거래소 모두 미상장이어도 producer 에러 없이 자연스럽게 제외됨.
#   상수명은 의미 유지를 위해 TOP_50_BY_MARKET_CAP 그대로 둠.
TOP_50_BY_MARKET_CAP: list[str] = [
    "BTC", "ETH", "XRP", "USDC", "SOL", "TRX", "DOGE", "USDS", "ADA", "BCH",
    "LINK", "XLM", "USD1", "AVAX", "USDE", "HBAR", "SHIB", "SUI", "XAUT", "TAO",
    "UNI", "DOT", "SKY", "WLFI", "NEAR", "PEPE", "AAVE", "ONDO", "ETC", "ICP",
    "POL", "ALGO", "ATOM", "RENDER", "ENA", "APT", "WLD", "ARB", "JST", "FIL",
    "PUMP", "PENGU", "VET", "JUP", "BONK", "TRUMP", "VIRTUAL", "CHZ", "STX", "XTZ",
    "SEI", "INJ", "SUN", "ZRO", "ETHFI", "TIA", "SYRUP", "2Z", "PYTH", "KITE",
    "GRT", "PENDLE", "OP", "IOTA", "AXS", "ENS", "XPL", "RAY", "COMP", "THETA",
    "NEO", "SAND", "JTO", "MANA", "WAL", "MEGA", "ZK", "A", "FF", "BAT",
    "XEC", "IMX", "GLM", "CHIP", "1INCH", "ORCA", "BIO",
    "USDT", "BNB", "TON", "CRO", "MNT", "PAXG", "CC", "ZEC", "LTC", "ASTER",
]


# ============================================================
# 거래소별 상장 종목 조회
# ============================================================

def get_upbit_krw_symbols() -> set[str]:
    """Upbit KRW 마켓 심볼 목록 (예: {'BTC', 'ETH', ...})"""
    url = "https://api.upbit.com/v1/market/all"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    markets = resp.json()
    return {
        m["market"].replace("KRW-", "")
        for m in markets
        if m["market"].startswith("KRW-")
    }


def get_binance_usdt_symbols() -> set[str]:
    """Binance USDT 마켓 심볼 목록 (예: {'BTC', 'ETH', ...})"""
    url = "https://api.binance.com/api/v3/exchangeInfo"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    info = resp.json()
    return {
        s["baseAsset"]
        for s in info["symbols"]
        if s["quoteAsset"] == "USDT" and s["status"] == "TRADING"
    }


def get_bithumb_krw_symbols() -> set[str]:
    """Bithumb KRW 마켓 심볼 목록 (예: {'BTC', 'ETH', ...})"""
    url = "https://api.bithumb.com/public/ticker/ALL_KRW"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    data = resp.json().get("data", {})
    return {
        symbol
        for symbol in data.keys()
        if symbol != "date"
    }


# ============================================================
# 거래소별 구독 대상 산출
# ============================================================

def get_symbols_for_exchange(exchange: str) -> list[str]:
    """
    TOP_50 중 해당 거래소에 실제 상장된 심볼만 반환.
    - exchange: "upbit" | "binance" | "bithumb"
    - TOP_50 순서를 유지 (시가총액 순)
    """
    if exchange == "upbit":
        listed = get_upbit_krw_symbols()
    elif exchange == "binance":
        listed = get_binance_usdt_symbols()
    elif exchange == "bithumb":
        listed = get_bithumb_krw_symbols()
    else:
        raise ValueError(f"Unknown exchange: {exchange}")

    result = [s for s in TOP_50_BY_MARKET_CAP if s in listed]
    excluded = [s for s in TOP_50_BY_MARKET_CAP if s not in listed]

    logger.info(f"[{exchange}] TOP_50 중 상장 {len(result)}개, 미상장 {len(excluded)}개")
    if excluded:
        logger.info(f"[{exchange}] 미상장 제외: {excluded}")
    return result


# ============================================================
# Kafka Producer
# ============================================================

def create_kafka_producer() -> KafkaProducer:
    """Kafka Producer 생성 (JSON 직렬화)"""
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
        linger_ms=10,
    )
    logger.info(f"Kafka Producer 생성 완료 (bootstrap: {KAFKA_BOOTSTRAP})")
    return producer


# ============================================================
# 정규화된 메시지 스키마
# ============================================================

def normalize_message(
    exchange: str,
    symbol: str,
    price: float,
    currency: str,
    timestamp: str | None = None,
) -> dict:
    """
    ADR 2.3 정규화 스키마:
    {
        "exchange": "binance",
        "symbol": "BTC",
        "price": 67542.10,
        "currency": "USDT",
        "timestamp": "2026-04-23T14:00:00Z"
    }
    """
    if timestamp is None:
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {
        "exchange": exchange,
        "symbol": symbol,
        "price": price,
        "currency": currency,
        "timestamp": timestamp,
    }