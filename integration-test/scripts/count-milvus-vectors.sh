#!/bin/bash
# Milvus Vector Counter for Integration Tests
# Usage: ./count-milvus-vectors.sh <collection_name> <file_uid> [milvus_host] [milvus_port]

COLLECTION="$1"
FILE_UID="$2"

# Support both Docker service name (default) and localhost for CI
# Arguments take precedence over environment variables
MILVUS_HOST="${3:-${MILVUS_HOST:-milvus}}"
MILVUS_PORT="${4:-${MILVUS_PORT:-19530}}"

# Python code to query Milvus
read -r -d '' PYTHON_CODE << 'PYTHON_EOF'
import sys
from pymilvus import connections, Collection

try:
    collection_name = sys.argv[1]
    file_uid = sys.argv[2]
    milvus_host = sys.argv[3]
    milvus_port = sys.argv[4]

    connections.connect("default", host=milvus_host, port=milvus_port)
    collection = Collection(collection_name)
    collection.load()
    results = collection.query(
        expr=f'file_uid == "{file_uid}"',
        output_fields=["file_uid"],
        limit=16384
    )
    print(len(results))
except Exception as e:
    print(f"Error: {e}", file=sys.stderr)
    sys.exit(1)
PYTHON_EOF

# When running locally (milvus_host=localhost), we need to run pymilvus inside
# a Docker container that has it installed. When running in Docker network
# (milvus_host=milvus), we can run directly.
if [ "${MILVUS_HOST}" = "localhost" ]; then
    # Running locally - use docker exec to run in artifact-backend which has pymilvus
    # Note: From inside the container, we connect to 'milvus' (container name), not 'localhost'
    COUNT=$(docker exec artifact-backend python3 -c "${PYTHON_CODE}" "${COLLECTION}" "${FILE_UID}" "milvus" "${MILVUS_PORT}" 2>&1)
else
    # Running in Docker network - run directly
    COUNT=$(python3 -c "${PYTHON_CODE}" "${COLLECTION}" "${FILE_UID}" "${MILVUS_HOST}" "${MILVUS_PORT}" 2>&1)
fi

# Check if output is a number, otherwise return 0 (collection may not exist)
if [[ "${COUNT}" =~ ^[0-9]+$ ]]; then
    echo "${COUNT}"
else
    # Log error for debugging but return 0 (not found = 0 vectors)
    echo "Error querying Milvus: ${COUNT}" >&2
    echo "0"
fi
