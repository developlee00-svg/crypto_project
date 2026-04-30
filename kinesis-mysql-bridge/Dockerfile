FROM python:3.11-slim

WORKDIR /app

# 시스템 의존성 (pymysql는 순수 Python이라 추가 패키지 불필요)
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bridge.py .

CMD ["python", "-u", "bridge.py"]