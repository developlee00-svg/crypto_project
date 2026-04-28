#!/bin/bash
# ============================================================
# AWS 리소스 생성 스크립트
#
# 생성 리소스:
#   1. KDS: crypto-stream-input (입력)
#   2. KDS: crypto-stream-output (출력)
#   3. S3 버킷: crypto-raw-data (원시 데이터 보관)
#   4. Firehose: crypto-stream-output → S3
#   5. Managed Flink 애플리케이션
#
# 사전 조건:
#   - AWS CLI 설치 + 자격 증명 설정
#   - deploy.sh 실행 완료 (S3에 ZIP 업로드됨)
#
# 사용법:
#   chmod +x setup_aws.sh
#   ./setup_aws.sh
# ============================================================

set -euo pipefail

AWS_REGION="ap-northeast-2"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

echo "AWS 계정: ${ACCOUNT_ID}"
echo "리전: ${AWS_REGION}"
echo ""

# ============================================================
# 1. Kinesis Data Streams
# ============================================================
echo "=== 1. KDS 생성 ==="

# 입력 스트림 (온디맨드 모드 — 비용 최적화)
aws kinesis create-stream \
    --stream-name crypto-stream-input \
    --stream-mode-details StreamMode=ON_DEMAND \
    --region "${AWS_REGION}" 2>/dev/null \
    && echo "  → crypto-stream-input 생성" \
    || echo "  → crypto-stream-input 이미 존재"

# 출력 스트림
aws kinesis create-stream \
    --stream-name crypto-stream-output \
    --stream-mode-details StreamMode=ON_DEMAND \
    --region "${AWS_REGION}" 2>/dev/null \
    && echo "  → crypto-stream-output 생성" \
    || echo "  → crypto-stream-output 이미 존재"

echo "  KDS 활성화 대기 중..."
aws kinesis wait stream-exists --stream-name crypto-stream-input --region "${AWS_REGION}"
aws kinesis wait stream-exists --stream-name crypto-stream-output --region "${AWS_REGION}"
echo "  → KDS 활성화 완료"

# ============================================================
# 2. S3 버킷
# ============================================================
echo ""
echo "=== 2. S3 버킷 생성 ==="

S3_RAW_BUCKET="crypto-raw-data-${ACCOUNT_ID}"

aws s3 mb "s3://${S3_RAW_BUCKET}" --region "${AWS_REGION}" 2>/dev/null \
    && echo "  → ${S3_RAW_BUCKET} 생성" \
    || echo "  → ${S3_RAW_BUCKET} 이미 존재"

# ============================================================
# 3. Firehose (crypto-stream-output → S3)
# ============================================================
echo ""
echo "=== 3. Firehose 생성 ==="

# Firehose IAM 역할 생성 (이미 있으면 skip)
FIREHOSE_ROLE_NAME="firehose-crypto-s3-role"

cat > /tmp/firehose-trust-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "Service": "firehose.amazonaws.com" },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

aws iam create-role \
    --role-name "${FIREHOSE_ROLE_NAME}" \
    --assume-role-policy-document file:///tmp/firehose-trust-policy.json \
    2>/dev/null \
    && echo "  → IAM 역할 생성: ${FIREHOSE_ROLE_NAME}" \
    || echo "  → IAM 역할 이미 존재: ${FIREHOSE_ROLE_NAME}"

# Firehose 정책 (S3 쓰기 + KDS 읽기)
cat > /tmp/firehose-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:PutObjectAcl",
        "s3:GetBucketLocation",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::${S3_RAW_BUCKET}",
        "arn:aws:s3:::${S3_RAW_BUCKET}/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "kinesis:DescribeStream",
        "kinesis:GetShardIterator",
        "kinesis:GetRecords",
        "kinesis:ListShards"
      ],
      "Resource": "arn:aws:kinesis:${AWS_REGION}:${ACCOUNT_ID}:stream/crypto-stream-output"
    }
  ]
}
EOF

aws iam put-role-policy \
    --role-name "${FIREHOSE_ROLE_NAME}" \
    --policy-name "firehose-crypto-policy" \
    --policy-document file:///tmp/firehose-policy.json

echo "  IAM 역할 전파 대기 (10초)..."
sleep 10

FIREHOSE_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${FIREHOSE_ROLE_NAME}"

# Firehose 전송 스트림 생성 (KDS → S3)
aws firehose create-delivery-stream \
    --delivery-stream-name crypto-to-s3 \
    --delivery-stream-type KinesisStreamAsSource \
    --kinesis-stream-source-configuration \
        "KinesisStreamARN=arn:aws:kinesis:${AWS_REGION}:${ACCOUNT_ID}:stream/crypto-stream-output,RoleARN=${FIREHOSE_ROLE_ARN}" \
    --extended-s3-destination-configuration \
        "RoleARN=${FIREHOSE_ROLE_ARN},BucketARN=arn:aws:s3:::${S3_RAW_BUCKET},Prefix=crypto-raw/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/,ErrorOutputPrefix=crypto-errors/,BufferingHints={SizeInMBs=5,IntervalInSeconds=60},CompressionFormat=GZIP" \
    --region "${AWS_REGION}" 2>/dev/null \
    && echo "  → Firehose 'crypto-to-s3' 생성" \
    || echo "  → Firehose 'crypto-to-s3' 이미 존재"

# ============================================================
# 4. 리소스 요약
# ============================================================
echo ""
echo "============================================"
echo "AWS 리소스 생성 완료"
echo "============================================"
echo ""
echo "KDS 입력:   crypto-stream-input"
echo "KDS 출력:   crypto-stream-output"
echo "S3 버킷:    ${S3_RAW_BUCKET}"
echo "Firehose:   crypto-to-s3 (output → S3)"
echo ""
echo "다음 단계:"
echo "  1. AWS 콘솔에서 Managed Flink 애플리케이션 수동 생성"
echo "     (CLI 생성보다 콘솔이 런타임 프로퍼티 설정이 편함)"
echo "  2. 런타임: Apache Flink 1.15"
echo "  3. 코드: s3://crypto-flink-app/crypto-arbitrage.zip"
echo "  4. VPC 설정: MySQL 접근 가능한 VPC/서브넷 지정"
echo "  5. IAM: KDS read/write + S3 read + MySQL 접근 권한"
echo ""
echo "=== 정리 (테스트 후) ==="
echo "  aws kinesis delete-stream --stream-name crypto-stream-input"
echo "  aws kinesis delete-stream --stream-name crypto-stream-output"
echo "  aws firehose delete-delivery-stream --delivery-stream-name crypto-to-s3"
echo "  aws s3 rb s3://${S3_RAW_BUCKET} --force"