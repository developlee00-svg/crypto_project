"""
/stats/daily
- 데이터 소스: MySQL daily_stats (Gold Layer, Airflow 02_dag가 매일 자정 적재)
- ADR 변경 합의: '오늘 평균'은 daily_stats에 없으므로 arbitrage_opportunities
  (Warm Layer)에서 실시간 GROUP BY 하여 오늘치를 합쳐서 반환
"""
from datetime import date, datetime, timedelta

from fastapi import APIRouter, Query

from services.mysql_pool import mysql_pool
from schemas import DailyStat

router = APIRouter(tags=["stats"])


@router.get("/stats/daily", response_model=list[DailyStat])
async def get_daily_stats(
    symbol: str | None = Query(None, description="심볼 필터 (예: BTC)"),
    days: int = Query(7, ge=1, le=90, description="최근 N일 (기본 7)"),
    include_today: bool = Query(True, description="오늘치를 arbitrage_opportunities에서 실시간 집계해 포함"),
) -> list[DailyStat]:
    """
    일별 아비트라지 통계.
    - 어제까지: daily_stats 테이블 (Airflow 02_dag가 매일 자정 적재)
    - 오늘치(option): arbitrage_opportunities에서 실시간 GROUP BY
    """
    since_date = date.today() - timedelta(days=days - 1)

    # ----- 1) daily_stats (어제까지) -----
    daily_where = ["date >= %s", "date < %s"]
    daily_params: list = [since_date, date.today()]
    if symbol:
        daily_where.append("symbol = %s")
        daily_params.append(symbol.upper())

    daily_sql = f"""
        SELECT symbol, date, avg_spread_pct, max_spread_pct, min_spread_pct, opportunity_count
        FROM daily_stats
        WHERE {' AND '.join(daily_where)}
        ORDER BY date DESC, symbol ASC
    """

    async with mysql_pool.pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(daily_sql, daily_params)
            daily_rows = await cur.fetchall()

    results: list[DailyStat] = [
        DailyStat(
            symbol=r[0],
            date=r[1],
            avg_spread_pct=float(r[2]) if r[2] is not None else None,
            max_spread_pct=float(r[3]) if r[3] is not None else None,
            min_spread_pct=float(r[4]) if r[4] is not None else None,
            opportunity_count=r[5],
        )
        for r in daily_rows
    ]

    # ----- 2) 오늘치 실시간 집계 -----
    if include_today:
        today_start = datetime.combine(date.today(), datetime.min.time())

        today_where = ["detected_at >= %s"]
        today_params: list = [today_start]
        if symbol:
            today_where.append("symbol = %s")
            today_params.append(symbol.upper())

        today_sql = f"""
            SELECT symbol,
                   AVG(spread_pct) AS avg_spread_pct,
                   MAX(spread_pct) AS max_spread_pct,
                   MIN(spread_pct) AS min_spread_pct,
                   COUNT(*) AS opportunity_count
            FROM arbitrage_opportunities
            WHERE {' AND '.join(today_where)}
            GROUP BY symbol
            ORDER BY symbol ASC
        """

        async with mysql_pool.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(today_sql, today_params)
                today_rows = await cur.fetchall()

        today_stats = [
            DailyStat(
                symbol=r[0],
                date=date.today(),
                avg_spread_pct=float(r[1]) if r[1] is not None else None,
                max_spread_pct=float(r[2]) if r[2] is not None else None,
                min_spread_pct=float(r[3]) if r[3] is not None else None,
                opportunity_count=r[4],
            )
            for r in today_rows
        ]
        # 오늘치를 맨 앞에 (date DESC 유지)
        results = today_stats + results

    return results