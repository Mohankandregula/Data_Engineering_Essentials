#!/usr/bin/env bash
# Build bcp_package.zip and verify it works against the Glue 5.0 container.
#
# Usage:
#   ./build_and_upload.sh                          # build + local verification only
#   ./build_and_upload.sh s3://bucket/prefix/      # build + verify + upload to S3

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
OUTPUT_DIR="${SCRIPT_DIR}/output"

echo "=== step 1: build bcp package on amazonlinux:2023 ==="
docker build -f "${SCRIPT_DIR}/Dockerfile.bcp_builder" -t bcp-builder "${SCRIPT_DIR}"

mkdir -p "${OUTPUT_DIR}"
docker run --rm -v "${OUTPUT_DIR}:/output" bcp-builder

if [ ! -f "${OUTPUT_DIR}/bcp_package.zip" ]; then
    echo "ERROR: bcp_package.zip not found in ${OUTPUT_DIR}"
    exit 1
fi

echo ""
echo "=== step 2: check glibc versions ==="
BUILD_GLIBC=$(docker run --rm bcp-builder bash -c "ldd --version | head -1" 2>/dev/null || echo "?")
echo "build image: ${BUILD_GLIBC}"

# pull the glue image if we don't have it already
GLUE_IMAGE="public.ecr.aws/glue/aws-glue-libs:5"
if ! docker image inspect "${GLUE_IMAGE}" >/dev/null 2>&1; then
    echo "pulling ${GLUE_IMAGE} (this takes a while the first time)..."
    docker pull "${GLUE_IMAGE}"
fi

GLUE_GLIBC=$(docker run --rm --entrypoint bash "${GLUE_IMAGE}" -c "ldd --version | head -1" 2>/dev/null || echo "?")
echo "glue 5.0:    ${GLUE_GLIBC}"

echo ""
echo "=== step 3: smoke test — run bcp inside glue container ==="
docker run --rm \
    --entrypoint bash \
    -v "${OUTPUT_DIR}:/tmp/zip" \
    "${GLUE_IMAGE}" \
    -c "
        cd /tmp && \
        unzip -q zip/bcp_package.zip -d bcp && \
        chmod +x /tmp/bcp/bin/bcp && \
        export LD_LIBRARY_PATH=/tmp/bcp/lib && \
        export ODBCSYSINI=/tmp/bcp && \
        /tmp/bcp/bin/bcp -v
    "

echo ""
echo "=== zip contents ==="
unzip -l "${OUTPUT_DIR}/bcp_package.zip"

echo ""
echo "bcp_package.zip is ready at: ${OUTPUT_DIR}/bcp_package.zip"

# optional: upload to S3 if a destination was provided
if [ "${1:-}" != "" ]; then
    echo ""
    echo "=== uploading to S3 ==="
    aws s3 cp "${OUTPUT_DIR}/bcp_package.zip" "${1}bcp_package.zip"
    echo "uploaded to ${1}bcp_package.zip"
fi
