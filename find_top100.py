"""
3거래소(Upbit/Binance/Bithumb) 모두 상장된 코인 중 시가총액 상위 100개 추출

CoinGecko의 무료 markets API로 시가총액 순위를 받아온 뒤,
3거래소 모두 상장된 심볼만 필터링하여 100개 산출.

실행:
    python find_top100.py
"""
import requests
import json

# ============================================================
# 1. 3거래소 상장 종목 조회
# ============================================================
def get_upbit_krw_symbols() -> set[str]:
    resp = requests.get("https://api.upbit.com/v1/market/all", timeout=10)
    resp.raise_for_status()
    return {
        m["market"].replace("KRW-", "")
        for m in resp.json()
        if m["market"].startswith("KRW-")
    }


def get_binance_usdt_symbols() -> set[str]:
    resp = requests.get("https://api.binance.com/api/v3/exchangeInfo", timeout=10)
    resp.raise_for_status()
    return {
        s["baseAsset"]
        for s in resp.json()["symbols"]
        if s["quoteAsset"] == "USDT" and s["status"] == "TRADING"
    }


def get_bithumb_krw_symbols() -> set[str]:
    resp = requests.get("https://api.bithumb.com/public/ticker/ALL_KRW", timeout=10)
    resp.raise_for_status()
    data = resp.json().get("data", {})
    return {s for s in data.keys() if s != "date"}


# ============================================================
# 2. CoinGecko에서 시가총액 순위 조회 (상위 250개 받아서 여유분 확보)
# ============================================================
def get_market_cap_ranking() -> list[dict]:
    """
    CoinGecko의 /coins/markets API로 시가총액 순위 받기.
    무료 API, 인증 불필요, 분당 30회 제한.
    """
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 250,
        "page": 1,
        "sparkline": "false",
    }
    resp = requests.get(url, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()


# ============================================================
# 3. 메인
# ============================================================
def main():
    print("=== 거래소 상장 조회 ===")
    upbit = get_upbit_krw_symbols()
    binance = get_binance_usdt_symbols()
    bithumb = get_bithumb_krw_symbols()
    print(f"Upbit KRW: {len(upbit)}개")
    print(f"Binance USDT: {len(binance)}개")
    print(f"Bithumb KRW: {len(bithumb)}개")

    all_three = upbit & binance & bithumb
    print(f"3거래소 모두 상장: {len(all_three)}개")

    print("\n=== CoinGecko 시가총액 순위 조회 (상위 250개) ===")
    coins = get_market_cap_ranking()
    print(f"받아온 코인: {len(coins)}개")

    # 시가총액 순서대로 + 3거래소 모두 상장된 것만 추리기
    selected = []
    for coin in coins:
        symbol = coin["symbol"].upper()
        if symbol in all_three:
            selected.append({
                "rank": coin["market_cap_rank"],
                "symbol": symbol,
                "name": coin["name"],
                "market_cap": coin["market_cap"],
            })
        if len(selected) >= 100:
            break

    print(f"\n=== 3거래소 모두 상장된 시총 상위 {len(selected)}개 ===")
    for i, c in enumerate(selected, 1):
        print(f"  {i:3d}. [{c['symbol']:10s}] {c['name']:30s} (cmc rank={c['rank']}, mcap=${c['market_cap']/1e9:.2f}B)")

    # ============================================================
    # 4. 결과 출력 (common.py / app.py 에 그대로 붙여넣을 형태)
    # ============================================================
    symbols = [c["symbol"] for c in selected]

    print("\n" + "=" * 60)
    print("common.py / app.py 에 붙여넣을 리스트:")
    print("=" * 60)
    # 10개씩 줄바꿈
    print("[")
    for i in range(0, len(symbols), 10):
        chunk = symbols[i:i+10]
        line = ", ".join(f'"{s}"' for s in chunk)
        print(f"    {line},")
    print("]")

    # 파일로도 저장
    with open("top100_symbols.json", "w") as f:
        json.dump({
            "symbols": symbols,
            "details": selected,
            "exchange_listings": {
                "upbit_only_count": len(upbit),
                "binance_only_count": len(binance),
                "bithumb_only_count": len(bithumb),
                "all_three_count": len(all_three),
            }
        }, f, indent=2, ensure_ascii=False)
    print(f"\n결과 저장: top100_symbols.json")


if __name__ == "__main__":
    main()