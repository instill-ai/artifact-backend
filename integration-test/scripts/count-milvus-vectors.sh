#!/bin/bash
# Milvus Vector Counter for Integration Tests
# Usage: ./count-milvus-vectors.sh <collection_name> <file_uid> [milvus_host] [milvus_port]

COLLECTION="$1"
FILE_UID="$2"

# Support both Docker service name (default) and localhost for CI
# Arguments take precedence over environment variables
MILVUS_HOST="${3:-${MILVUS_HOST:-milvus}}"
MILVUS_PORT="${4:-${MILVUS_PORT:-19530}}"

# Use pymilvus directly - simpler and more reliable than milvus_cli for scripting
COUNT=$(python3 - "${COLLECTION}" "${FILE_UID}" "${MILVUS_HOST}" "${MILVUS_PORT}" <<'PYTHON_EOF'
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
)

echo "${COUNT:-0}"
