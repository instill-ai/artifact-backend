#!/bin/bash
# Milvus Vector Counter for Integration Tests
# Usage: ./count-milvus-vectors.sh <collection_name> <file_uid>

COLLECTION="$1"
FILE_UID="$2"

# Use pymilvus directly - simpler and more reliable than milvus_cli for scripting
COUNT=$(python3 - "${COLLECTION}" "${FILE_UID}" <<'PYTHON_EOF'
import sys
from pymilvus import connections, Collection

try:
    collection_name = sys.argv[1]
    file_uid = sys.argv[2]

    connections.connect("default", host="milvus", port="19530")
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
