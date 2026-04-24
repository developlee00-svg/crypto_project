"""
공통 유틸리티 모듈
- Upbit/Binance/Bithumb 공통 상장 종목 조회
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
# 공통 상장 종목 조회
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
    # data 안에 'date' 키는 제외
    return {
        symbol
        for symbol in data.keys()
        if symbol != "date"
    }


def get_common_symbols(limit: int = 100) -> list[str]:
    """
    Upbit-Binance 공통 상장 종목 조회.
    Bithumb 공통 종목도 포함하되, 기본은 Upbit-Binance 교집합.
    limit: 최대 종목 수
    """
    upbit = get_upbit_krw_symbols()
    binance = get_binance_usdt_symbols()

    try:
        bithumb = get_bithumb_krw_symbols()
    except Exception as e:
        logger.warning(f"Bithumb 종목 조회 실패, Upbit-Binance 교집합만 사용: {e}")
        bithumb = set()

    # Upbit-Binance 공통 종목
    common = upbit & binance
    logger.info(f"Upbit({len(upbit)}) ∩ Binance({len(binance)}) = {len(common)}개")

    if bithumb:
        bithumb_common = common & bithumb
        logger.info(f"Bithumb 공통 종목: {len(bithumb_common)}개")

    symbols = sorted(common)[:limit]
    logger.info(f"최종 수집 대상: {len(symbols)}개 → {symbols[:10]}...")
    return symbols


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