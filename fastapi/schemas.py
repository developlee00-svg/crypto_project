"""
FastAPI 응답 스키마 (Pydantic v2)
"""
from datetime import datetime, date
from pydantic import BaseModel, Field


# ============================================================
# /prices, /prices/{symbol}
# ============================================================

class ExchangePrice(BaseModel):
    """거래소별 시세. binance만 price_usdt 추가 보유."""
    price_krw: float = Field(..., description="KRW 환산 가격")
    price_usdt: float | None = Field(None, description="원본 USDT 가격 (binance 전용)")
    timestamp: str = Field(..., description="시세 수신 시각 (ISO 8601)")


# /prices 응답: { "BTC": { "upbit": {...}, "binance": {...}, "bithumb": {...} }, ... }
# /prices/{symbol} 응답: { "upbit": {...}, "binance": {...}, "bithumb": {...} }
# Pydantic 모델로 강제하지 않고 dict로 반환 (50개 코인 동적 키)


# ============================================================
# /opportunities
# ============================================================

class Opportunity(BaseModel):
    id: int
    symbol: str
    buy_exchange: str
    buy_price_krw: float
    sell_exchange: str
    sell_price_krw: float
    spread_krw: float
    spread_pct: float
    exchange_rate: float
    detected_at: datetime


# ============================================================
# /exchange-rate
# ============================================================

class ExchangeRate(BaseModel):
    base_currency: str
    target_currency: str
    rate: float
    fetched_at: datetime


# ============================================================
# /stats/daily
# ============================================================

class DailyStat(BaseModel):
    symbol: str
    date: date
    avg_spread_pct: float | None
    max_spread_pct: float | None
    min_spread_pct: float | None
    opportunity_count: int | None