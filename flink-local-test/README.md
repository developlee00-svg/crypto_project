# Flink Interval Join 로컬 검증 (Day 6)

ADR Step 1: Flink에 배포하기 전에 로컬에서 조인 로직을 먼저 검증한다.

## 실행 순서

```bash
# 0. 의존성 설치
pip install -r requirements.txt

# 1. Kafka에서 샘플 데이터 수집 (60초)
python collect_samples.py --seconds 60

# 2. Interval Join + 아비트라지 계산 검증
python verify_join.py

# 옵션: 수집 시간/조인 윈도우 변경
python collect_samples.py --seconds 120
python verify_join.py --interval 5
```

## 검증 항목

| 항목 | 설명 |
|------|------|
| Interval Join | 같은 symbol, ±3초 이내 매칭 (merge_asof nearest) |
| 환율 적용 | Binance USDT × USD/KRW → KRW 변환 |
| 스프레드 계산 | (KRW거래소 - Binance_KRW) / Binance_KRW × 100 |
| 매매 방향 | 스프레드 부호로 buy/sell 거래소 결정 |

## 출력 파일

- `sample_data.csv` — Kafka에서 수집한 원시 데이터
- `arbitrage_results.csv` — 조인 + 아비트라지 계산 결과