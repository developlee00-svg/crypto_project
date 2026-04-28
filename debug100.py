import os, json, time, boto3
from kafka import KafkaConsumer

c = boto3.client('kinesis', region_name=os.getenv('AWS_REGION'))

consumer = KafkaConsumer(
    'crypto-prices-binance', 'crypto-prices-upbit', 'crypto-prices-bithumb',
    bootstrap_servers='kafka:29092',
    group_id='debug-bridge-2',
    auto_offset_reset='latest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    consumer_timeout_ms=10000,
)

batch = []
for msg in consumer:
    batch.append({'Data': json.dumps(msg.value).encode(), 'PartitionKey': msg.value.get('symbol','x')})
    if len(batch) >= 100:
        break

consumer.close()
print(f'Collected {len(batch)} records')

entries = batch
for attempt in range(4):
    try:
        resp = c.put_records(StreamName='crypto-stream-input', Records=entries)
        failed = resp.get('FailedRecordCount', 0)
        print(f'Attempt {attempt}: sent={len(entries)}, failed={failed}')
        if failed > 0:
            retry = []
            for i, r in enumerate(resp['Records']):
                if 'ErrorCode' in r:
                    print(f'  ErrorCode={r["ErrorCode"]} ErrorMsg={r.get("ErrorMessage","")}')
                    retry.append(entries[i])
            entries = retry
        else:
            break
    except Exception as e:
        print(f'Attempt {attempt} exception: {e}')
        break
