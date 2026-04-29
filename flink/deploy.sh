#!/bin/bash
# ============================================================
# Flink 아비트라지 앱 배포 스크립트 (v5)
# - venv는 Step 3 (boto3 다운로드)에만 한정
# - S3 업로드는 수동
# ============================================================

set -euo pipefail

APP_NAME="crypto-arbitrage"
ZIP_FILE="${APP_NAME}.zip"
BUILD_VENV=".build-venv"

echo "=== Step 1: Maven 의존성 빌드 ==="
mvn clean package -q
echo "  → target/pyflink-dependencies.jar 생성 완료"

echo ""
echo "=== Step 2: 빌드 전용 venv 준비 ==="
if [ ! -d "${BUILD_VENV}" ]; then
    python -m venv "${BUILD_VENV}"
    echo "  → ${BUILD_VENV} 생성"
fi
echo "  → venv 준비 완료"

echo ""
echo "=== Step 3: boto3 의존성 다운로드 (Linux x86_64 호환) ==="
rm -rf deps
mkdir -p deps

# venv를 서브셸 안에서만 사용 → activate/deactivate가 외부 PATH에 영향 주지 않음
(
    if [ -f "${BUILD_VENV}/Scripts/activate" ]; then
        source "${BUILD_VENV}/Scripts/activate"
    elif [ -f "${BUILD_VENV}/bin/activate" ]; then
        source "${BUILD_VENV}/bin/activate"
    else
        echo "  ✗ venv activate 스크립트를 찾을 수 없음"
        exit 1
    fi

    pip install \
      --platform manylinux2014_x86_64 \
      --target=deps \
      --implementation cp \
      --python-version 3.11 \
      --only-binary=:all: \
      --upgrade \
      boto3 \
      --quiet
)

if [ ! -d "deps/boto3" ]; then
    echo "  ✗ deps/boto3 디렉토리가 생성되지 않음"
    exit 1
fi
echo "  → deps/ 패키지 수: $(ls deps | wc -l)"

echo ""
echo "=== Step 4: ZIP 패키징 (Python zipfile) ==="
rm -f "${ZIP_FILE}"

mkdir -p lib
cp target/pyflink-dependencies.jar lib/

python <<EOF
import os
import zipfile

ZIP_FILE = "${ZIP_FILE}"
EXCLUDE_PATTERNS = ('.dist-info', '__pycache__', '.pyc')

def should_skip(path):
    return any(pat in path for pat in EXCLUDE_PATTERNS)

with zipfile.ZipFile(ZIP_FILE, 'w', zipfile.ZIP_DEFLATED) as zf:
    zf.write('main.py', 'main.py')

    for root, dirs, files in os.walk('lib'):
        for f in files:
            full = os.path.join(root, f)
            arc = full.replace(os.sep, '/')
            zf.write(full, arc)

    deps_root = 'deps'
    for root, dirs, files in os.walk(deps_root):
        dirs[:] = [d for d in dirs if not should_skip(d)]
        for f in files:
            if should_skip(f):
                continue
            full = os.path.join(root, f)
            rel = os.path.relpath(full, deps_root)
            arc = rel.replace(os.sep, '/')
            zf.write(full, arc)

size_mb = os.path.getsize(ZIP_FILE) / (1024 * 1024)
with zipfile.ZipFile(ZIP_FILE, 'r') as zf:
    count = len(zf.namelist())
print(f"  → {ZIP_FILE} 생성 완료 ({size_mb:.1f} MB, {count} files)")
EOF

rm -rf lib deps

echo ""
echo "=== 빌드 완료 ==="
echo ""
echo "다음 단계 (수동):"
echo "  1. aws s3 cp ${ZIP_FILE} s3://<BUCKET>/crypto-flink-app/${ZIP_FILE} --region ap-northeast-2"
echo "  2. AWS 콘솔 → Flink Application Stop → Run"