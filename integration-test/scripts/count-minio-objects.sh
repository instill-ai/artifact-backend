#!/bin/bash
# MinIO Object Counter for Integration Tests
# Usage: ./count-minio-objects.sh <bucket> <prefix>

set -e

BUCKET="$1"
PREFIX="$2"

# Set HOME to /tmp so mc can save its config
export HOME=/tmp

# Support both Docker service name (default) and localhost for CI
MINIO_HOST="${MINIO_HOST:-minio}"
MINIO_PORT="${MINIO_PORT:-9000}"

# Configure MinIO alias (idempotent)
mc alias set myminio http://${MINIO_HOST}:${MINIO_PORT} minioadmin minioadmin --api s3v4 > /dev/null 2>&1 || true

# Count objects with the given prefix
# Use 2>/dev/null to suppress errors if prefix doesn't exist
COUNT=$(mc ls --recursive "myminio/${BUCKET}/${PREFIX}" 2>/dev/null | wc -l | tr -d ' ')

echo "${COUNT}"
