#!/bin/bash
# MinIO Object Counter for Integration Tests
# Usage: ./count-minio-objects.sh <bucket> <prefix>

set -e

BUCKET="$1"
PREFIX="$2"

# Set HOME to /tmp so mc can save its config
export HOME=/tmp

# Configure MinIO alias (idempotent)
mc alias set myminio http://minio:9000 minioadmin minioadmin --api s3v4 > /dev/null 2>&1 || true

# Count objects with the given prefix
# Use 2>/dev/null to suppress errors if prefix doesn't exist
COUNT=$(mc ls --recursive "myminio/${BUCKET}/${PREFIX}" 2>/dev/null | wc -l | tr -d ' ')

echo "${COUNT}"
