FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# 실행할 Producer는 docker-compose에서 command로 지정
CMD ["python", "-u", "producer_binance.py"]