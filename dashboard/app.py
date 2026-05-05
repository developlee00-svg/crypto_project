"""
암호화폐 아비트라지 탐지 대시보드 (Streamlit)

- 50개 코인 × 9컬럼 단일 페이지
- 1초 폴링
- FastAPI 호출 3회/초 (/prices, /opportunities, /stats/daily)
"""
import os
from datetime import date
from typing import Any

import httpx
import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

# ------------------------------------------------------------------
# 설정
# ------------------------------------------------------------------
FASTAPI_URL = os.getenv("FASTAPI_URL", "http://fastapi:8000")
POLL_INTERVAL_MS = 1000  # 1초

# ADR 2.1 (v10.1) — 시가총액 상위 + 3거래소 중 1곳 이상 상장 (39개)
# producers/common.py의 TOP_50_BY_MARKET_CAP과 반드시 동일 순서 유지
TOP_50 = [
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

EXCHANGES = ["upbit", "binance", "bithumb"]
EMPTY = "-"

# ------------------------------------------------------------------
# 페이지 설정
# ------------------------------------------------------------------
st.set_page_config(
    page_title="Crypto Arbitrage Dashboard",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ------------------------------------------------------------------
# API 호출 (3회/초, 동기 httpx로 충분 — Streamlit 자체가 단일 스레드)
# ------------------------------------------------------------------
def fetch_json(path: str, params: dict | None = None, timeout: float = 2.0) -> Any:
    """FastAPI 호출. 실패 시 None 반환 (대시보드는 fail-open)."""
    try:
        with httpx.Client(timeout=timeout) as client:
            r = client.get(f"{FASTAPI_URL}{path}", params=params)
            r.raise_for_status()
            return r.json()
    except Exception as e:
        # 1초마다 재시도되므로 한 번 실패해도 다음 사이클에 복구
        st.session_state["last_error"] = f"{path}: {e}"
        return None


def fetch_all() -> tuple[dict, list, list]:
    """3개 API 한꺼번에 호출. 모두 fail-open (실패 시 빈 컨테이너)."""
    prices = fetch_json("/prices") or {}
    opportunities = fetch_json(
        "/opportunities", params={"hours": 3, "limit": 1000}
    ) or []
    stats = fetch_json(
        "/stats/daily", params={"days": 1, "include_today": "true"}
    ) or []
    return prices, opportunities, stats


# ------------------------------------------------------------------
# 머지 로직
# ------------------------------------------------------------------
def latest_opportunity_by_symbol(opportunities: list[dict]) -> dict[str, dict]:
    """
    /opportunities 응답에서 심볼별 최신 1건 추출.
    응답이 detected_at 내림차순 정렬이라고 가정 (FastAPI 쪽에서 ORDER BY DESC).
    """
    out: dict[str, dict] = {}
    for row in opportunities:
        sym = row.get("symbol")
        if sym and sym not in out:
            out[sym] = row
    return out


def today_stats_by_symbol(stats: list) -> dict[str, dict]:
    """
    /stats/daily?days=1&include_today=true 응답 → 오늘치 심볼별 통계 추출.

    실제 응답 포맷 (schemas.DailyStat 평탄 list):
      [
        {"symbol": "BTC", "date": "2026-04-30", "avg_spread_pct": ..., ...},
        ...
      ]
    오늘 날짜인 행만 picking — days=2 이상으로 호출돼도 안전.
    """
    if not isinstance(stats, list):
        return {}

    today_str = date.today().isoformat()
    out: dict[str, dict] = {}
    for row in stats:
        # row["date"]는 "YYYY-MM-DD" 문자열 (Pydantic date → JSON 직렬화)
        if row.get("date") == today_str:
            sym = row.get("symbol")
            if sym:
                out[sym] = row
    return out


def build_dataframe(
    prices: dict, opportunities: list, stats: list
) -> pd.DataFrame:
    """
    50행 × 9컬럼 DataFrame 생성.
    - 정렬용 원본 숫자 컬럼은 '_raw' 접미사로 따로 보관 후 표시 직전 제거
    """
    opp_by_symbol = latest_opportunity_by_symbol(opportunities)
    today_by_symbol = today_stats_by_symbol(stats)

    rows = []
    for idx, sym in enumerate(TOP_50):
        ex_map = prices.get(sym, {})

        # 거래소별 KRW 가격
        upbit_krw = ex_map.get("upbit", {}).get("price_krw")
        binance_krw = ex_map.get("binance", {}).get("price_krw")
        bithumb_krw = ex_map.get("bithumb", {}).get("price_krw")

        # 최신 아비트라지 기회
        opp = opp_by_symbol.get(sym, {})
        buy_ex = opp.get("buy_exchange")
        sell_ex = opp.get("sell_exchange")
        flow = f"{buy_ex} → {sell_ex}" if buy_ex and sell_ex else None
        spread_pct = opp.get("spread_pct")
        spread_krw = opp.get("spread_krw")

        # 오늘 통계
        today = today_by_symbol.get(sym, {})
        avg_pct = today.get("avg_spread_pct")
        count = today.get("opportunity_count")

        rows.append({
            "_rank": idx,  # 시가총액 순 정렬용 (숨김)
            "코인": sym,
            "Upbit (KRW)": upbit_krw,
            "Binance (KRW)": binance_krw,
            "Bithumb (KRW)": bithumb_krw,
            "매수 → 매도": flow,
            "김프 %": spread_pct,
            "김프 KRW": spread_krw,
            "오늘 평균 %": avg_pct,
            "빈도": count,
        })

    return pd.DataFrame(rows)


# ------------------------------------------------------------------
# 표시 포맷팅 — 빈 셀은 마지막 단계에서만 "-"로
# ------------------------------------------------------------------
def fmt_krw(v) -> str:
    """단가에 따라 소수점 자릿수 동적 조정 — SHIB/PEPE 같은 1원 미만 코인 대응."""
    if pd.isna(v) or v is None:
        return EMPTY
    if v >= 1000:
        return f"{v:,.0f}"      # 1,000원 이상: 정수 (BTC 116,500,000)
    if v >= 1:
        return f"{v:,.2f}"      # 1원 이상: 소수 둘째 (DOGE 162.45)
    if v >= 0.01:
        return f"{v:.4f}"       # 0.01원 이상: 소수 넷째 (SHIB 0.0234)
    return f"{v:.5f}"           # 더 작으면 소수 다섯째 (PEPE 0.00001)


def fmt_pct(v) -> str:
    if pd.isna(v) or v is None:
        return EMPTY
    return f"{v:.2f}%"


def fmt_int(v) -> str:
    if pd.isna(v) or v is None:
        return EMPTY
    return f"{int(v):,}"


def fmt_str(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return EMPTY
    return str(v)


def format_for_display(df: pd.DataFrame) -> pd.DataFrame:
    """정렬 끝난 후 마지막에 호출. 표시용 문자열로 변환."""
    out = df.drop(columns=["_rank"]).copy()
    out["Upbit (KRW)"] = out["Upbit (KRW)"].apply(fmt_krw)
    out["Binance (KRW)"] = out["Binance (KRW)"].apply(fmt_krw)
    out["Bithumb (KRW)"] = out["Bithumb (KRW)"].apply(fmt_krw)
    out["매수 → 매도"] = out["매수 → 매도"].apply(fmt_str)
    out["김프 %"] = out["김프 %"].apply(fmt_pct)
    out["김프 KRW"] = out["김프 KRW"].apply(fmt_krw)
    out["오늘 평균 %"] = out["오늘 평균 %"].apply(fmt_pct)
    out["빈도"] = out["빈도"].apply(fmt_int)
    return out


# ------------------------------------------------------------------
# UI
# ------------------------------------------------------------------
st.title("암호화폐 아비트라지 탐지")

# 1초 자동 새로고침
st_autorefresh(interval=POLL_INTERVAL_MS, key="auto_refresh")

# 정렬 컨트롤
col1, col2, col3 = st.columns([2, 1, 3])
with col1:
    sort_options = {
        "시가총액 순 (기본)": "_rank",
        "김프 % (높은 순)": "김프 %",
        "김프 KRW (높은 순)": "김프 KRW",
        "오늘 평균 % (높은 순)": "오늘 평균 %",
        "빈도 (많은 순)": "빈도",
        "코인명 (가나다)": "코인",
    }
    sort_label = st.selectbox("정렬", list(sort_options.keys()), index=0)
    sort_col = sort_options[sort_label]

with col2:
    ascending = st.checkbox("오름차순", value=(sort_col in ("_rank", "코인")))

# 데이터 패치 + 머지
prices, opportunities, stats = fetch_all()
df = build_dataframe(prices, opportunities, stats)

# 정렬 (NaN은 항상 끝으로)
df = df.sort_values(by=sort_col, ascending=ascending, na_position="last")

# 표시
st.dataframe(
    format_for_display(df),
    hide_index=True,
    use_container_width=True,
    height=1800,  # 50행 다 보이게
)

# 푸터: 마지막 에러만 작게 표시 (디버그용)
last_err = st.session_state.get("last_error")
if last_err:
    st.caption(f"⚠ last error: {last_err}")