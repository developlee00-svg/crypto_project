"""
Step 2: Flink Interval Join 로직 로컬 검증
- sample_data.csv를 읽어서 pandas로 Flink와 동일한 조인 로직 수행
- 같은 symbol 기준, 앞뒤 3초 이내 Interval Join
- Binance(USDT) 가격 × 환율 → KRW 변환
- 거래소 간 스프레드(원), 스프레드 비율(%) 산출

사용법:
  pip install pandas requests
  python verify_join.py
  python verify_join.py --input sample_data.csv --interval 3
"""
import argparse
import logging
from datetime import timedelta

import pandas as pd
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("verify-join")

# ============================================================
# 환율 조회
# ============================================================

def fetch_usd_krw_rate() -> float:
    """FreeExchangeRateApi에서 USD/KRW 환율 조회"""
    url = "https://open.er-api.com/v6/latest/USD"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        rate = resp.json()["rates"]["KRW"]
        logger.info(f"환율 조회 완료: 1 USD = {rate:,.2f} KRW")
        return float(rate)
    except Exception as e:
        fallback = 1370.0
        logger.warning(f"환율 API 실패 ({e}), 기본값 사용: {fallback}")
        return fallback


# ============================================================
# Interval Join (pandas 구현)
# ============================================================

def interval_join(
    left: pd.DataFrame,
    right: pd.DataFrame,
    interval_sec: int = 3,
) -> pd.DataFrame:
    """
    Flink Interval Join과 동일한 로직을 pandas로 구현.
    - left와 right를 symbol 기준으로 매칭
    - right.timestamp가 left.timestamp ± interval_sec 이내인 경우만 조인
    - 각 left 레코드에 대해 가장 가까운 right 레코드 1개만 매칭 (nearest)
    """
    results = []

    for symbol in left["symbol"].unique():
        l = left[left["symbol"] == symbol].copy()
        r = right[right["symbol"] == symbol].copy()

        if l.empty or r.empty:
            continue

        # merge_asof: 가장 가까운 시간 기준 매칭
        l = l.sort_values("timestamp")
        r = r.sort_values("timestamp")

        merged = pd.merge_asof(
            l,
            r,
            on="timestamp",
            suffixes=("_left", "_right"),
            tolerance=timedelta(seconds=interval_sec),
            direction="nearest",
        )

        # symbol 컬럼 복원 (suffixes로 symbol_left/right가 됨)
        merged["symbol"] = symbol

        # tolerance 밖이면 NaN → 제거
        merged = merged.dropna(subset=["price_right"])
        results.append(merged)

    if not results:
        return pd.DataFrame()

    return pd.concat(results, ignore_index=True)


# ============================================================
# 아비트라지 계산
# ============================================================

def calculate_arbitrage(joined: pd.DataFrame, usd_krw: float) -> pd.DataFrame:
    """
    조인된 데이터에서 아비트라지 스프레드 계산.
    - KRW 거래소 가격은 그대로 사용
    - USDT 거래소 가격은 환율 적용하여 KRW로 변환
    """
    df = joined.copy()

    # 각 거래소 가격을 KRW로 통일
    df["price_left_krw"] = df.apply(
        lambda row: row["price_left"] if row["currency_left"] == "KRW"
        else row["price_left"] * usd_krw,
        axis=1,
    )
    df["price_right_krw"] = df.apply(
        lambda row: row["price_right"] if row["currency_right"] == "KRW"
        else row["price_right"] * usd_krw,
        axis=1,
    )

    # 스프레드 계산
    df["spread_krw"] = df["price_left_krw"] - df["price_right_krw"]
    df["spread_pct"] = (df["spread_krw"] / df["price_right_krw"]) * 100

    # 매매 방향 결정
    df["buy_exchange"] = df.apply(
        lambda row: row["exchange_left"] if row["spread_krw"] < 0
        else row["exchange_right"],
        axis=1,
    )
    df["sell_exchange"] = df.apply(
        lambda row: row["exchange_right"] if row["spread_krw"] < 0
        else row["exchange_left"],
        axis=1,
    )

    df["abs_spread_pct"] = df["spread_pct"].abs()

    return df


# ============================================================
# 메인
# ============================================================

def main(input_file: str, interval: int):
    # 1. 데이터 로드
    logger.info(f"데이터 로드: {input_file}")
    df = pd.read_csv(input_file)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    logger.info(f"총 {len(df)}건 로드")

    for ex in df["exchange"].unique():
        cnt = len(df[df["exchange"] == ex])
        logger.info(f"  {ex}: {cnt}건")

    # 2. 환율 조회
    usd_krw = fetch_usd_krw_rate()

    # 3. 거래소 쌍별 Interval Join
    exchanges = df["exchange"].unique().tolist()
    krw_exchanges = [e for e in exchanges if df[df["exchange"] == e]["currency"].iloc[0] == "KRW"]
    usdt_exchanges = [e for e in exchanges if df[df["exchange"] == e]["currency"].iloc[0] == "USDT"]

    logger.info(f"KRW 거래소: {krw_exchanges}")
    logger.info(f"USDT 거래소: {usdt_exchanges}")
    logger.info(f"Interval Join 윈도우: ±{interval}초")

    all_results = []

    # KRW 거래소 vs USDT 거래소 (김프 계산의 핵심)
    for krw_ex in krw_exchanges:
        for usdt_ex in usdt_exchanges:
            left = df[df["exchange"] == krw_ex].copy()
            right = df[df["exchange"] == usdt_ex].copy()

            joined = interval_join(left, right, interval)
            if joined.empty:
                logger.warning(f"  {krw_ex} ↔ {usdt_ex}: 매칭 없음")
                continue

            result = calculate_arbitrage(joined, usd_krw)
            logger.info(
                f"  {krw_ex} ↔ {usdt_ex}: {len(result)}건 매칭, "
                f"평균 스프레드 {result['spread_pct'].mean():.2f}%"
            )
            all_results.append(result)

    # KRW 거래소 간 비교 (Upbit vs Bithumb)
    for i, ex1 in enumerate(krw_exchanges):
        for ex2 in krw_exchanges[i + 1:]:
            left = df[df["exchange"] == ex1].copy()
            right = df[df["exchange"] == ex2].copy()

            joined = interval_join(left, right, interval)
            if joined.empty:
                logger.warning(f"  {ex1} ↔ {ex2}: 매칭 없음")
                continue

            result = calculate_arbitrage(joined, usd_krw)
            logger.info(
                f"  {ex1} ↔ {ex2}: {len(result)}건 매칭, "
                f"평균 스프레드 {result['spread_pct'].mean():.2f}%"
            )
            all_results.append(result)

    if not all_results:
        logger.error("매칭된 결과가 없습니다.")
        return

    # 4. 결과 합산 및 출력
    final = pd.concat(all_results, ignore_index=True)

    # 아비트라지 기회 (스프레드 1% 이상)
    opportunities = final[final["abs_spread_pct"] >= 1.0].copy()
    opportunities = opportunities.sort_values("abs_spread_pct", ascending=False)

    print("\n" + "=" * 80)
    print("검증 결과 요약")
    print("=" * 80)
    print(f"적용 환율: 1 USD = {usd_krw:,.2f} KRW")
    print(f"Interval Join 윈도우: ±{interval}초")
    print(f"총 매칭 건수: {len(final):,}건")
    print(f"아비트라지 기회 (|스프레드| ≥ 1%): {len(opportunities):,}건")

    # 심볼별 통계
    print(f"\n{'symbol':>8} | {'평균 스프레드(%)':>14} | {'최대(%)':>8} | {'최소(%)':>8} | {'건수':>6}")
    print("-" * 60)

    stats = final.groupby("symbol").agg(
        avg_pct=("spread_pct", "mean"),
        max_pct=("spread_pct", "max"),
        min_pct=("spread_pct", "min"),
        count=("spread_pct", "count"),
    ).sort_values("avg_pct", ascending=False)

    for sym, row in stats.head(20).iterrows():
        print(
            f"{sym:>8} | {row['avg_pct']:>14.2f} | {row['max_pct']:>8.2f} | "
            f"{row['min_pct']:>8.2f} | {row['count']:>6.0f}"
        )

    # Top 10 아비트라지 기회
    if not opportunities.empty:
        print(f"\nTop 10 아비트라지 기회:")
        print(f"{'symbol':>8} | {'매수':>10} | {'매도':>10} | {'스프레드(%)':>11} | {'스프레드(원)':>14}")
        print("-" * 70)
        for _, row in opportunities.head(10).iterrows():
            print(
                f"{row['symbol']:>8} | {row['buy_exchange']:>10} | "
                f"{row['sell_exchange']:>10} | {row['abs_spread_pct']:>11.2f} | "
                f"{abs(row['spread_krw']):>14,.0f}"
            )

    # 결과 CSV 저장
    output_file = "arbitrage_results.csv"
    output_cols = [
        "symbol", "exchange_left", "price_left_krw",
        "exchange_right", "price_right_krw",
        "spread_krw", "spread_pct",
        "buy_exchange", "sell_exchange", "timestamp",
    ]
    final[output_cols].to_csv(output_file, index=False)
    logger.info(f"\n결과 저장: {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Flink Interval Join 로컬 검증")
    parser.add_argument("--input", default="sample_data.csv", help="입력 CSV 파일")
    parser.add_argument("--interval", type=int, default=3, help="Interval Join 윈도우(초)")
    args = parser.parse_args()

    main(args.input, args.interval)