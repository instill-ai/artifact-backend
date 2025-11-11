#!/bin/bash
# MinIO Object Counter and Validator for Integration Tests
#
# Usage:
#   ./count-minio-objects.sh <bucket> <prefix> [minio_host] [minio_port] [mode] [file_extension]
#
# Modes:
#   count    - Count objects with prefix (default)
#   verify   - Verify converted file exists with specific extension
#   list     - List all objects with prefix (for debugging)
#
# Examples:
#   ./count-minio-objects.sh artifact-backend kb-123/file-456/chunk
#   ./count-minio-objects.sh artifact-backend kb-123/file-456/converted-file minio 9000 verify png
#   ./count-minio-objects.sh artifact-backend kb-123/file-456/converted-file minio 9000 list

set -e

BUCKET="$1"
PREFIX="$2"

# Set HOME to /tmp so mc can save its config
export HOME=/tmp

# Support both Docker service name (default) and localhost for CI
# Arguments take precedence over environment variables
MINIO_HOST="${3:-${MINIO_HOST:-minio}}"
MINIO_PORT="${4:-${MINIO_PORT:-9000}}"
MODE="${5:-count}"
FILE_EXTENSION="${6:-}"

# Configure MinIO alias (idempotent)
mc alias set myminio http://${MINIO_HOST}:${MINIO_PORT} minioadmin minioadmin --api s3v4 > /dev/null 2>&1 || true

case "$MODE" in
  count)
    # Count objects with the given prefix
    # Use 2>/dev/null to suppress errors if prefix doesn't exist
    COUNT=$(mc ls --recursive "myminio/${BUCKET}/${PREFIX}" 2>/dev/null | wc -l | tr -d ' ')
    echo "${COUNT}"
    ;;

  verify)
    # Verify that a converted file with specific extension exists
    if [ -z "$FILE_EXTENSION" ]; then
      echo "ERROR: file_extension required for verify mode" >&2
      exit 1
    fi

    # List all files with the prefix and check for the extension
    FILES=$(mc ls --recursive "myminio/${BUCKET}/${PREFIX}" 2>/dev/null || true)

    if [ -z "$FILES" ]; then
      echo "0"
      exit 0
    fi

    # Count files matching the extension
    COUNT=$(echo "$FILES" | grep -c "\.${FILE_EXTENSION}$" || echo "0")
    echo "${COUNT}"
    ;;

  list)
    # List all objects with full details for debugging
    mc ls --recursive "myminio/${BUCKET}/${PREFIX}" 2>/dev/null || echo "No objects found"
    ;;

  *)
    echo "ERROR: Unknown mode: $MODE" >&2
    echo "Valid modes: count, verify, list" >&2
    exit 1
    ;;
esac
