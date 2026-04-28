import os, json, boto3
key = os.getenv('AWS_ACCESS_KEY_ID')
secret = os.getenv('AWS_SECRET_ACCESS_KEY')
region = os.getenv('AWS_REGION')
print(f'KEY: {key[:5] if key else "NONE"}...')
print(f'SECRET: {secret[:5] if secret else "NONE"}...')
print(f'REGION: {region}')

c = boto3.client('kinesis', region_name=region, aws_access_key_id=key, aws_secret_access_key=secret)
resp = c.put_records(StreamName='crypto-stream-input', Records=[{'Data': b'test', 'PartitionKey': 'x'}])
print(f'Direct: OK (failed={resp.get("FailedRecordCount")})')

c2 = boto3.client(**{'service_name':'kinesis','region_name':region})
try:
    resp2 = c2.put_records(StreamName='crypto-stream-input', Records=[{'Data': b'test2', 'PartitionKey': 'x'}])
    print(f'No-creds: OK (failed={resp2.get("FailedRecordCount")})')
except Exception as e:
    print(f'No-creds: FAILED - {e}')
