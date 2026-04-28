import os, boto3
key = os.getenv('AWS_ACCESS_KEY_ID')
secret = os.getenv('AWS_SECRET_ACCESS_KEY')
region = os.getenv('AWS_REGION')
kwargs = {
    'service_name': 'kinesis',
    'region_name': region,
}
if key and secret:
    kwargs['aws_access_key_id'] = key
    kwargs['aws_secret_access_key'] = secret
c = boto3.client(**kwargs)
import json
resp = c.put_records(StreamName='crypto-stream-input', Records=[{'Data': json.dumps({'test':'kwargs'}).encode(), 'PartitionKey': 'test'}])
print(f'Failed: {resp.get("FailedRecordCount", -1)}')
print('Success!')
