# 암호화폐 거래소 간 아비트라지 탐지 파이프라인

Kafka 실시간 수집 + AWS Flink 스트림 처리 + Airflow 배치 + FastAPI + Streamlit 대시보드

## 빠른 시작

```bash
# 1. 인프라 기동
docker compose up -d

# 2. 상태 확인
docker compose ps

# 3. Kafka 토픽 확인
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# 4. MySQL 테이블 확인
docker exec -it mysql mysql -ucrypto -pcrypto1234 crypto_arbitrage -e "SHOW TABLES;"
```

## 접속 정보

| 서비스 | URL / 포트 |
|--------|-----------|
| Kafka | `localhost:9092` |
| MySQL | `localhost:3306` (crypto / crypto1234) |
| Airflow | `http://localhost:8080` (admin / admin) |

## 프로젝트 구조

```
├── docker-compose.yml
├── .env
├── mysql/init/          # DB 초기화 SQL
├── airflow/dags/        # Airflow DAG
├── producers/           # WebSocket → Kafka Producer (3개 거래소)
├── consumer/            # Kafka → Kinesis 브릿지
├── flink/               # AWS Flink SQL
├── fastapi/             # API 서버
└── streamlit/           # 대시보드
```