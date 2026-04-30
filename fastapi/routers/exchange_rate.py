"""
/exchange-rate
- 데이터 소스: MySQL exchange_rates 최신값
- 현재 적용 중인 USD/KRW 환율 반환
"""
from fastapi import APIRouter, HTTPException

from services.mysql_pool import mysql_pool
from schemas import ExchangeRate

router = APIRouter(tags=["exchange-rate"])


@router.get("/exchange-rate", response_model=ExchangeRate)
async def get_exchange_rate() -> ExchangeRate:
    """현재 적용 중인 USD/KRW 환율 (가장 최근 fetched_at)"""
    sql = """
        SELECT base_currency, target_currency, rate, fetched_at
        FROM exchange_rates
        WHERE base_currency = 'USD' AND target_currency = 'KRW'
        ORDER BY fetched_at DESC
        LIMIT 1
    """
    async with mysql_pool.pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql)
            row = await cur.fetchone()

    if row is None:
        raise HTTPException(
            status_code=404,
            detail="환율 데이터가 없습니다 (Airflow 01_dag_exchange_rate 실행 확인 필요).",
        )

    return ExchangeRate(
        base_currency=row[0],
        target_currency=row[1],
        rate=float(row[2]),
        fetched_at=row[3],
    )