"""
FastAPI 앱 진입점 (ADR v9 Day 9)
- Kafka consumer + MySQL pool 라이프사이클 관리
- 5개 엔드포인트 등록
"""
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from services.kafka_cache import price_cache
from services.mysql_pool import mysql_pool

from routers import prices, opportunities, exchange_rate, stats

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("fastapi-main")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ---------- startup ----------
    logger.info("=== FastAPI startup ===")
    await mysql_pool.start()
    await price_cache.start()
    logger.info("=== FastAPI ready ===")

    yield

    # ---------- shutdown ----------
    logger.info("=== FastAPI shutdown ===")
    await price_cache.stop()
    await mysql_pool.stop()
    logger.info("=== FastAPI stopped ===")


app = FastAPI(
    title="Crypto Arbitrage API",
    description="실시간 암호화폐 거래소 간 아비트라지(김프) 탐지 API",
    version="0.9.0",
    lifespan=lifespan,
)

# Streamlit 등 프론트가 다른 포트에서 호출하므로 CORS 허용
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================
# 라우터 등록
# ============================================================
app.include_router(prices.router)
app.include_router(opportunities.router)
app.include_router(exchange_rate.router)
app.include_router(stats.router)


@app.get("/health", tags=["health"])
async def health():
    return {"status": "ok"}