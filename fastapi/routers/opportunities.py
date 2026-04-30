"""
/opportunities
- 데이터 소스: MySQL arbitrage_opportunities (kinesis-mysql-bridge가 적재)
- 필터: symbol, hours (최근 N시간), limit
"""
from datetime import datetime, timedelta

from fastapi import APIRouter, Query

from services.mysql_pool import mysql_pool
from schemas import Opportunity

router = APIRouter(tags=["opportunities"])


@router.get("/opportunities", response_model=list[Opportunity])
async def get_opportunities(
    symbol: str | None = Query(None, description="심볼 필터 (예: BTC)"),
    hours: int = Query(1, ge=1, le=168, description="최근 N시간 (기본 1, 최대 168)"),
    limit: int = Query(100, ge=1, le=1000, description="최대 행 수"),
) -> list[Opportunity]:
    """
    아비트라지 기회 조회.
    - 최근 N시간 내 detected_at 기준 내림차순
    - symbol 필터 옵션
    """
    since = datetime.utcnow() - timedelta(hours=hours)

    where_clauses = ["detected_at >= %s"]
    params: list = [since]
    if symbol:
        where_clauses.append("symbol = %s")
        params.append(symbol.upper())

    where_sql = " AND ".join(where_clauses)
    sql = f"""
        SELECT id, symbol, buy_exchange, buy_price_krw,
               sell_exchange, sell_price_krw,
               spread_krw, spread_pct, exchange_rate, detected_at
        FROM arbitrage_opportunities
        WHERE {where_sql}
        ORDER BY detected_at DESC
        LIMIT %s
    """
    params.append(limit)

    async with mysql_pool.pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, params)
            rows = await cur.fetchall()

    return [
        Opportunity(
            id=r[0],
            symbol=r[1],
            buy_exchange=r[2],
            buy_price_krw=float(r[3]),
            sell_exchange=r[4],
            sell_price_krw=float(r[5]),
            spread_krw=float(r[6]),
            spread_pct=float(r[7]),
            exchange_rate=float(r[8]),
            detected_at=r[9],
        )
        for r in rows
    ]