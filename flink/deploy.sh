#!/bin/bash
# ============================================================
# Flink 아비트라지 앱 배포 스크립트
#
# 순서:
#   1. Maven으로 Uber JAR 빌드 (Kinesis + JDBC + MySQL 커넥터)
#   2. main.py + JAR → ZIP 패키징
#   3. S3 업로드
#
# 사전 조건:
#   - JDK 11, Maven, AWS CLI 설치
#   - AWS 자격 증명 설정 완료
#
# 사용법:
#   chmod +x deploy.sh
#   ./deploy.sh                                    # 기본 버킷
#   ./deploy.sh my-flink-bucket ap-northeast-2     # 커스텀 버킷
# ============================================================

set -euo pipefail

S3_BUCKET="${1:-crypto-flink-app}"
AWS_REGION="${2:-ap-northeast-2}"
APP_NAME="crypto-arbitrage"
ZIP_FILE="${APP_NAME}.zip"

echo "=== Step 1: Maven 의존성 빌드 ==="
mvn clean package -q
echo "  → target/pyflink-dependencies.jar 생성 완료"

echo ""
echo "=== Step 2: ZIP 패키징 ==="
# 기존 ZIP 제거
rm -f "${ZIP_FILE}"

# ZIP 구조:
#   crypto-arbitrage.zip
#   ├── main.py
#   └── lib/
#       └── pyflink-dependencies.jar
mkdir -p lib
cp target/pyflink-dependencies.jar lib/

zip -r "${ZIP_FILE}" main.py lib/
echo "  → ${ZIP_FILE} 생성 완료"

# 정리
rm -rf lib

echo ""
echo "=== Step 3: S3 업로드 ==="
aws s3 cp "${ZIP_FILE}" "s3://${S3_BUCKET}/${ZIP_FILE}" --region "${AWS_REGION}"
echo "  → s3://${S3_BUCKET}/${ZIP_FILE} 업로드 완료"

echo ""
echo "=== 배포 패키지 준비 완료 ==="
echo ""
echo "다음 단계: AWS 콘솔에서 Managed Flink 애플리케이션 생성"
echo "  - 런타임: Apache Flink 1.20"
echo "  - 코드 위치: s3://${S3_BUCKET}/${ZIP_FILE}"
echo "  - 런타임 프로퍼티 설정 필요 (아래 참고)"
echo ""
echo "런타임 프로퍼티 (PropertyGroupId: 'kinesis'):"
echo "  input.stream   = crypto-stream-input"
echo "  output.stream  = crypto-stream-output"
echo "  region         = ${AWS_REGION}"
echo ""
echo "런타임 프로퍼티 (PropertyGroupId: 'mysql'):"
echo "  url      = jdbc:mysql://<RDS_ENDPOINT>:3306/crypto_arbitrage"
echo "  username = <USERNAME>"
echo "  password = <PASSWORD>"