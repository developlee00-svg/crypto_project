"""
MySQL 커넥션 풀 서비스 (aiomysql)
- /opportunities, /exchange-rate, /stats/daily 엔드포인트가 사용
- 환율 최신값 헬퍼 (Binance KRW 환산용, 60초 TTL 메모리 캐시)
"""
import os
import time
import asyncio
import logging

import aiomysql

logger = logging.getLogger("mysql_pool")

MYSQL_CONFIG = {
    "host": os.environ["CRYPTO_MYSQL_HOST"],
    "port": int(os.environ["CRYPTO_MYSQL_PORT"]),
    "user": os.environ["CRYPTO_MYSQL_USER"],
    "password": os.environ["CRYPTO_MYSQL_PASSWORD"],
    "db": os.environ["CRYPTO_MYSQL_DB"],
    "charset": "utf8mb4",
    "autocommit": True,
}


class MySQLPool:
    """aiomysql 커넥션 풀 래퍼"""

    def __init__(self):
        self._pool: aiomysql.Pool | None = None

    async def start(self):
        self._pool = await aiomysql.create_pool(
            minsize=1,
            maxsize=10,
            **MYSQL_CONFIG,
        )
        logger.info(
            f"MySQL pool 생성 완료 ({MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_CONFIG['db']})"
        )

    async def stop(self):
        if self._pool:
            self._pool.close()
            await self._pool.wait_closed()
            logger.info("MySQL pool 종료")

    @property
    def pool(self) -> aiomysql.Pool:
        if self._pool is None:
            raise RuntimeError("MySQL pool 미초기화 (lifespan start 호출 필요)")
        return self._pool


# 전역 싱글톤
mysql_pool = MySQLPool()


# ============================================================
# 환율 헬퍼 (Binance KRW 환산용)
# ============================================================

_rate_cache: dict = {"rate": None, "fetched_at": 0.0}
_rate_lock = asyncio.Lock()
_RATE_TTL_SEC = 60.0  # 60초 캐시 (환율은 매시간 갱신, 60초 캐시면 충분)


async def get_latest_usd_krw() -> float | None:
    """
    exchange_rates 테이블에서 USD/KRW 최신 환율 조회.
    60초 TTL 메모리 캐시. 환율이 없으면 None.
    """
    now = time.time()
    if _rate_cache["rate"] is not None and (now - _rate_cache["fetched_at"]) < _RATE_TTL_SEC:
        return _rate_cache["rate"]

    async with _rate_lock:
        # double-check
        now = time.time()
        if _rate_cache["rate"] is not None and (now - _rate_cache["fetched_at"]) < _RATE_TTL_SEC:
            return _rate_cache["rate"]

        async with mysql_pool.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT rate
                    FROM exchange_rates
                    WHERE base_currency = 'USD' AND target_currency = 'KRW'
                    ORDER BY fetched_at DESC
                    LIMIT 1
                    """
                )
                row = await cur.fetchone()

        if row is None:
            return None

        rate = float(row[0])
        _rate_cache["rate"] = rate
        _rate_cache["fetched_at"] = now
        return rate