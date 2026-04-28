import os, json, time, boto3
from kafka import KafkaConsumer

key = os.getenv('AWS_ACCESS_KEY_ID')
secret = os.getenv('AWS_SECRET_ACCESS_KEY')
region = os.getenv('AWS_REGION')
stream = os.getenv('KINESIS_STREAM_NAME')

c = boto3.client('kinesis', region_name=region, aws_access_key_id=key, aws_secret_access_key=secret)

consumer = KafkaConsumer(
    'crypto-prices-binance', 'crypto-prices-upbit', 'crypto-prices-bithumb',
    bootstrap_servers='kafka:29092',
    group_id='debug-bridge',
    auto_offset_reset='latest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    consumer_timeout_ms=5000,
)

batch = []
for msg in consumer:
    batch.append({'Data': json.dumps(msg.value).encode(), 'PartitionKey': msg.value.get('symbol','x')})
    if len(batch) >= 50:
        break

consumer.close()
print(f'Collected {len(batch)} records')

try:
    resp = c.put_records(StreamName=stream, Records=batch)
    print(f'FailedCount: {resp.get("FailedRecordCount")}')
    for i, r in enumerate(resp["Records"]):
        if "ErrorCode" in r:
            print(f'  Record {i}: {r["ErrorCode"]} - {r.get("ErrorMessage","")}')
    print('Done!')
except Exception as e:
    print(f'Exception: {e}')
