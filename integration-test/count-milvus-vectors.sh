#!/bin/bash
# Milvus Vector Counter for Integration Tests
# Usage: ./count-milvus-vectors.sh <collection_name> <file_uid>

COLLECTION="$1"
FILE_UID="$2"

# Use pymilvus directly - simpler and more reliable than milvus_cli for scripting
COUNT=$(python3 <<PYTHON_EOF
from pymilvus import connections, Collection

try:
    connections.connect("default", host="milvus", port="19530")
    collection = Collection("${COLLECTION}")
    collection.load()
    results = collection.query(
        expr='file_uid == "${FILE_UID}"',
        output_fields=["file_uid"],
        limit=16384
    )
    print(len(results))
except:
    print(0)
PYTHON_EOF
)

echo "${COUNT:-0}"
