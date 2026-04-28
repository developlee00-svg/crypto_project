import os, json, boto3
from kafka import KafkaConsumer

key = os.getenv('AWS_ACCESS_KEY_ID')
secret = os.getenv('AWS_SECRET_ACCESS_KEY')
region = os.getenv('AWS_REGION')
c = boto3.client('kinesis', region_name=region, aws_access_key_id=key, aws_secret_access_key=secret)

consumer = KafkaConsumer(
    'crypto-prices-binance',
    bootstrap_servers='kafka:29092',
    auto_offset_reset='latest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    consumer_timeout_ms=5000,
)

batch = []
for msg in consumer:
    batch.append({'Data': json.dumps(msg.value).encode(), 'PartitionKey': msg.value.get('symbol','x')})
    if len(batch) >= 5:
        break

consumer.close()
print(f'Collected {len(batch)} records')
resp = c.put_records(StreamName='crypto-stream-input', Records=batch)
print(f'Failed: {resp.get("FailedRecordCount", -1)}')
print('Success!')
