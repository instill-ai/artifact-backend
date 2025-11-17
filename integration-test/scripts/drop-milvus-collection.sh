#!/bin/bash
# Drop Milvus Collection for Integration Tests
# Usage: ./drop-milvus-collection.sh <collection_name> [milvus_host] [milvus_port]

COLLECTION="$1"

# Support both Docker service name (default) and localhost for CI
# Arguments take precedence over environment variables
MILVUS_HOST="${2:-${MILVUS_HOST:-milvus}}"
MILVUS_PORT="${3:-${MILVUS_PORT:-19530}}"

# Use pymilvus to drop collection
python3 - "${COLLECTION}" "${MILVUS_HOST}" "${MILVUS_PORT}" <<'PYTHON_EOF'
import sys
from pymilvus import connections, utility

try:
    collection_name = sys.argv[1]
    milvus_host = sys.argv[2]
    milvus_port = sys.argv[3]

    connections.connect("default", host=milvus_host, port=milvus_port)

    # Check if collection exists
    if utility.has_collection(collection_name):
        utility.drop_collection(collection_name)
        print(f"SUCCESS: Dropped collection {collection_name}")
    else:
        print(f"INFO: Collection {collection_name} does not exist")

    sys.exit(0)
except Exception as e:
    print(f"ERROR: {e}", file=sys.stderr)
    sys.exit(1)
PYTHON_EOF
