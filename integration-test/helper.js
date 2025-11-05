// ============================================================================
// Safe Database Query Helpers
// ============================================================================

import * as constant from './const.js';

/**
 * Safe database query wrapper with proper error handling.
 *
 * Prevents silent failures where SQL errors cause queries to return undefined/null,
 * which can lead to tests passing when they should fail.
 *
 * @param {string} query - SQL query string
 * @param {...any} params - Query parameters (passed individually, not as array)
 * @returns {Array} - Query results array, or empty array on error
 * @throws {Error} - Throws error if query fails (caller should handle)
 */
export function safeQuery(query, ...params) {
  try {
    const result = constant.db.query(query, ...params);
    if (result === undefined || result === null) {
      throw new Error("Query returned undefined/null - possible SQL error");
    }
    return result;
  } catch (e) {
    console.error(`[DB ERROR] Query failed: ${e}`);
    console.error(`[DB ERROR] Query: ${query}`);
    console.error(`[DB ERROR] Params: ${JSON.stringify(params)}`);
    throw e; // Re-throw so caller knows the query failed
  }
}

/**
 * Safe database execute wrapper for UPDATE/DELETE/INSERT statements.
 *
 * NOTE: k6 SQL driver only has query() method, not execute().
 * For UPDATE/DELETE/INSERT, query() returns an empty result set but still executes.
 * We can't get affected row count, so we return 0 on success.
 *
 * @param {string} statement - SQL statement
 * @param {...any} params - Statement parameters
 * @returns {number} - Always returns 0 (k6 SQL driver doesn't provide affected row count)
 * @throws {Error} - Throws error if statement fails
 */
export function safeExecute(statement, ...params) {
  try {
    // k6 SQL driver only has query(), use it for UPDATE/DELETE/INSERT too
    const result = constant.db.query(statement, ...params);
    // query() succeeds but returns empty array for UPDATE/DELETE/INSERT
    // If no exception, assume success
    return 0;
  } catch (e) {
    console.error(`[DB ERROR] Execute failed: ${e}`);
    console.error(`[DB ERROR] Statement: ${statement}`);
    console.error(`[DB ERROR] Params: ${JSON.stringify(params)}`);
    throw e;
  }
}

export function deepEqual(x, y) {
  const ok = Object.keys,
    tx = typeof x,
    ty = typeof y;
  return x && y && tx === "object" && tx === ty
    ? ok(x).length === ok(y).length &&
    ok(x).every((key) => deepEqual(x[key], y[key]))
    : x === y;
}

export function isUUID(uuid) {
  const regexExp =
    /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
  return regexExp.test(uuid);
}

export function isValidOwner(owner, expectedOwner) {
  if (owner === null || owner === undefined) return false;
  if (owner.user === null || owner.user === undefined) return false;
  if (owner.user.id !== "admin") return false;
  return deepEqual(owner.user.profile, expectedOwner.profile)
}

export function validateKnowledgeBase(knowledgeBase, isPrivate) {
  if (!("uid" in knowledgeBase)) {
    console.log("Knowledge base has no uid field");
    return false;
  }

  if (!("id" in knowledgeBase)) {
    console.log("Knowledge base has no id field");
    return false;
  }

  if (!("name" in knowledgeBase)) {
    console.log("Knowledge base has no name field");
    return false;
  }

  if (!("description" in knowledgeBase)) {
    console.log("Knowledge base has no description field");
    return false;
  }

  if (!("createTime" in knowledgeBase)) {
    console.log("Knowledge base has no create_time field");
    return false;
  }

  if (!("updateTime" in knowledgeBase)) {
    console.log("Knowledge base has no update_time field");
    return false;
  }

  return true;
}

export function validateFile(file, isPrivate) {
  if (!("uid" in file)) {
    console.log("File has no uid field");
    return false;
  }

  if (!("id" in file)) {
    console.log("File has no id field");
    return false;
  }

  if (!("name" in file)) {
    console.log("File has no name field (resource path)");
    return false;
  }

  if (!("filename" in file)) {
    console.log("File has no filename field");
    return false;
  }

  if (!("type" in file)) {
    console.log("File has no type field");
    return false;
  }

  if (!("processStatus" in file)) {
    console.log("File has no process_status field");
    return false;
  }

  if (!("createTime" in file)) {
    console.log("File has no create_time field");
    return false;
  }

  if (!("updateTime" in file)) {
    console.log("File has no update_time field");
    return false;
  }

  return true;
}

export function validateKnowledgeBaseGRPC(knowledgeBase, isPrivate) {
  if (!("uid" in knowledgeBase)) {
    console.log("Knowledge base has no uid field");
    return false;
  }

  if (!("id" in knowledgeBase)) {
    console.log("Knowledge base has no id field");
    return false;
  }

  if (!("name" in knowledgeBase)) {
    console.log("Knowledge base has no name field");
    return false;
  }

  if (!("description" in knowledgeBase)) {
    console.log("Knowledge base has no description field");
    return false;
  }

  if (!("createTime" in knowledgeBase)) {
    console.log("Knowledge base has no createTime field");
    return false;
  }

  if (!("updateTime" in knowledgeBase)) {
    console.log("Knowledge base has no updateTime field");
    return false;
  }

  return true;
}

export function validateFileGRPC(file, isPrivate) {
  if (!("uid" in file)) {
    console.log("File has no uid field");
    return false;
  }

  if (!("id" in file)) {
    console.log("File has no id field");
    return false;
  }

  if (!("name" in file)) {
    console.log("File has no name field (resource path)");
    return false;
  }

  if (!("filename" in file)) {
    console.log("File has no filename field");
    return false;
  }

  if (!("type" in file)) {
    console.log("File has no type field");
    return false;
  }

  if (!("processStatus" in file)) {
    console.log("File has no processStatus field");
    return false;
  }

  if (!("createTime" in file)) {
    console.log("File has no createTime field");
    return false;
  }

  if (!("updateTime" in file)) {
    console.log("File has no updateTime field");
    return false;
  }

  return true;
}

export function validateChunk(chunk, isPrivate) {
  if (!("uid" in chunk)) {
    console.log("Chunk has no uid field");
    return false;
  }

  if (!("id" in chunk)) {
    console.log("Chunk has no id field");
    return false;
  }

  if (!("name" in chunk)) {
    console.log("Chunk has no name field (resource path)");
    return false;
  }

  if (!("originalFileId" in chunk)) {
    console.log("Chunk has no originalFileId field");
    return false;
  }

  if (!("tokens" in chunk)) {
    console.log("Chunk has no tokens field");
    return false;
  }

  if (!("createTime" in chunk)) {
    console.log("Chunk has no createTime field");
    return false;
  }

  if (!("type" in chunk)) {
    console.log("Chunk has no type field");
    return false;
  }

  return true;
}

export function validateChunkGRPC(chunk, isPrivate) {
  if (!("uid" in chunk)) {
    console.log("Chunk has no uid field");
    return false;
  }

  if (!("id" in chunk)) {
    console.log("Chunk has no id field");
    return false;
  }

  if (!("name" in chunk)) {
    console.log("Chunk has no name field (resource path)");
    return false;
  }

  if (!("originalFileId" in chunk)) {
    console.log("Chunk has no originalFileId field");
    return false;
  }

  if (!("tokens" in chunk)) {
    console.log("Chunk has no tokens field");
    return false;
  }

  if (!("createTime" in chunk)) {
    console.log("Chunk has no createTime field");
    return false;
  }

  if (!("type" in chunk)) {
    console.log("Chunk has no type field");
    return false;
  }

  return true;
}

// ============================================================================
// Storage Verification Helpers for Reprocessing Tests
// ============================================================================

import { sleep } from 'k6';
import exec from 'k6/x/exec';
import * as helper from './helper.js';

/**
 * Count MinIO objects directly using MinIO CLI (mc)
 * Provides direct verification of blob storage
 *
 * @param {string} kbUID - Knowledge base UUID
 * @param {string} fileUID - File UUID
 * @param {string} objectType - Type of MinIO object ('converted-file' or 'chunk')
 * @returns {number} Count of objects (0 = no objects, -1 = error)
 */
export function countMinioObjects(kbUID, fileUID, objectType) {
  try {
    // CRITICAL: Query database for actual MinIO destination prefix
    // After KB swaps, MinIO paths might not match current KB UID (destination field is preserved)
    let prefix;

    if (objectType === 'chunk') {
      // Query chunk table for actual destination
      const chunkResult = safeQuery(`
        SELECT content_dest FROM chunk
        WHERE file_uid = $1
        LIMIT 1
      `, fileUID);

      if (chunkResult && chunkResult.length > 0 && chunkResult[0].content_dest) {
        // Extract prefix from destination (e.g., "kb-xxx/file-yyy/chunk/chunk_0.json" -> "kb-xxx/file-yyy/chunk/")
        const dest = chunkResult[0].content_dest;
        const lastSlashIndex = dest.lastIndexOf('/');
        prefix = dest.substring(0, lastSlashIndex + 1);
      } else {
        // Fallback to constructed prefix if no chunks found
        prefix = `kb-${kbUID}/file-${fileUID}/${objectType}/`;
      }
    } else if (objectType === 'converted-file') {
      // Query converted_file table for actual destination
      const convertedResult = safeQuery(`
        SELECT destination FROM converted_file
        WHERE file_uid = $1
        LIMIT 1
      `, fileUID);

      if (convertedResult && convertedResult.length > 0 && convertedResult[0].destination) {
        // Extract prefix from destination
        const dest = convertedResult[0].destination;
        const lastSlashIndex = dest.lastIndexOf('/');
        prefix = dest.substring(0, lastSlashIndex + 1);
      } else {
        // Fallback to constructed prefix
        prefix = `kb-${kbUID}/file-${fileUID}/${objectType}/`;
      }
    } else {
      // Unknown object type, use constructed prefix
      prefix = `kb-${kbUID}/file-${fileUID}/${objectType}/`;
    }

    // Execute the shell script to count MinIO objects
    const result = exec.command('sh', [
      `${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/scripts/count-minio-objects.sh`,
      constant.minioConfig.bucket,
      prefix
    ]);

    // Parse the output (should be a number)
    const count = parseInt(result.trim());
    return isNaN(count) ? 0 : count;
  } catch (e) {
    console.error(`Failed to count MinIO objects for file ${fileUID}: ${e}`);
    return -1; // Return -1 to indicate error (not 0, which means "no objects")
  }
}

/**
 * Count embeddings in the database for a specific file
 * Verifies vector data consistency in Postgres (Milvus vectors are referenced here)
 *
 * @param {string} fileUid - File UUID
 * @returns {number} Count of embedding records
 */
export function countEmbeddings(fileUid) {
  try {
    const results = safeQuery('SELECT uid FROM embedding WHERE file_uid = $1', fileUid);
    // results is an array of row objects
    return results ? results.length : 0;
  } catch (e) {
    console.error(`Failed to count embeddings for file ${fileUid}: ${e}`);
    return 0;
  }
}

/**
 * Count vectors in Milvus collection for a specific file
 * Verifies actual vector data in Milvus (not just database references)
 *
 * CRITICAL: This function queries the database for active_collection_uid first
 * to ensure we count vectors from the correct collection during KB updates.
 *
 * @param {string} kbUID - Knowledge base UUID
 * @param {string} fileUID - File UUID
 * @returns {number} Count of vectors (0 = no vectors, -1 = error)
 */
export function countMilvusVectors(kbUID, fileUID) {
  try {
    // CRITICAL FIX: Query active_collection_uid from database
    // During updates, a KB's active_collection_uid may point to a different collection than its own UID
    const kbResult = safeQuery(`
      SELECT active_collection_uid
      FROM knowledge_base
      WHERE uid = $1
    `, kbUID);

    if (!kbResult || kbResult.length === 0) {
      console.error(`KB ${kbUID} not found in database`);
      return -1;
    }

    // Convert active_collection_uid from Buffer (PostgreSQL UUID) to string
    let activeCollectionUID = kbResult[0].active_collection_uid;
    if (Array.isArray(activeCollectionUID)) {
      activeCollectionUID = String.fromCharCode(...activeCollectionUID);
    }

    if (!activeCollectionUID) {
      console.error(`KB ${kbUID} has null active_collection_uid`);
      return -1;
    }

    // Convert active_collection_uid to Milvus collection name format: kb_{uuid_with_underscores}
    const collectionName = `kb_${activeCollectionUID.replace(/-/g, '_')}`;

    // Execute the shell script to count Milvus vectors using milvus_cli
    const result = exec.command('sh', [
      `${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/scripts/count-milvus-vectors.sh`,
      collectionName,
      fileUID
    ]);

    // DEBUG: Log the raw output from the script
    if (result.indexOf('Error') >= 0 || result.indexOf('not exist') >= 0) {
      console.log(`DEBUG countMilvusVectors: KB=${kbUID}, activeCollection=${activeCollectionUID}, collection=${collectionName}, file=${fileUID}, result="${result.trim()}"`);
    }

    // Parse the output (should be a number)
    const count = parseInt(result.trim());
    return isNaN(count) ? 0 : count;
  } catch (e) {
    console.error(`Failed to count Milvus vectors for KB ${kbUID}, file ${fileUID}: ${e}`);
    return -1; // Return -1 to indicate error (not 0, which means "no vectors")
  }
}

// ============================================================================
// Polling Helpers for Eventual Consistency
// ============================================================================

/**
 * Poll MinIO until objects appear or timeout
 * Handles eventual consistency of S3/MinIO storage
 *
 * @param {string} kbUID - Knowledge base UUID
 * @param {string} fileUID - File UUID
 * @param {string} objectType - Type of object ('converted-file' or 'chunk')
 * @param {number} maxWaitSeconds - Maximum seconds to wait (default: 10)
 * @returns {number} Final count of objects
 */
export function pollMinIOObjects(kbUID, fileUID, objectType, maxWaitSeconds = 3) {
  for (let i = 0; i < maxWaitSeconds; i++) {
    const count = countMinioObjects(kbUID, fileUID, objectType);
    if (count > 0) {
      return count;
    }
    if (i < maxWaitSeconds - 1) {
      sleep(1);
    }
  }
  // Final attempt after waiting
  return countMinioObjects(kbUID, fileUID, objectType);
}

/**
 * Poll database embeddings until they appear or timeout
 * Handles transaction commit delays
 *
 * @param {string} fileUid - File UUID
 * @param {number} maxWaitSeconds - Maximum seconds to wait (default: 10)
 * @returns {number} Final count of embeddings
 */
export function pollEmbeddings(fileUid, maxWaitSeconds = 3) {
  for (let i = 0; i < maxWaitSeconds; i++) {
    const count = countEmbeddings(fileUid);
    if (count > 0) {
      return count;
    }
    if (i < maxWaitSeconds - 1) {
      sleep(1);
    }
  }
  // Final attempt after waiting
  return countEmbeddings(fileUid);
}

/**
 * Poll Milvus vectors until they appear or timeout
 * Handles Milvus indexing and eventual consistency
 *
 * @param {string} kbUID - Knowledge base UUID
 * @param {string} fileUID - File UUID
 * @param {number} maxWaitSeconds - Maximum seconds to wait (default: 10)
 * @returns {number} Final count of vectors
 */
export function pollMilvusVectors(kbUID, fileUID, maxWaitSeconds = 3) {
  for (let i = 0; i < maxWaitSeconds; i++) {
    const count = countMilvusVectors(kbUID, fileUID);
    if (count > 0) {
      return count;
    }
    if (i < maxWaitSeconds - 1) {
      sleep(1);
    }
  }
  // Final attempt after waiting
  return countMilvusVectors(kbUID, fileUID);
}

/**
 * Poll MinIO objects until they are cleaned up (count becomes zero)
 * Used to verify cleanup workflow completion after knowledge base deletion
 *
 * @param {string} kbUID - Knowledge base UUID
 * @param {string} fileUID - File UUID
 * @param {string} objectType - Type of object ('converted-file' or 'chunk')
 * @param {number} maxWaitSeconds - Maximum seconds to wait (default: 30)
 * @returns {number} Final count of objects (should be 0 if cleanup succeeded)
 */
export function pollMinIOCleanup(kbUID, fileUID, objectType, maxWaitSeconds = 30) {
  for (let i = 0; i < maxWaitSeconds; i++) {
    const count = countMinioObjects(kbUID, fileUID, objectType);
    if (count === 0) {
      return 0;
    }
    if (i < maxWaitSeconds - 1) {
      sleep(1);
    }
  }
  // Final attempt after waiting
  return countMinioObjects(kbUID, fileUID, objectType);
}

/**
 * Poll embeddings until they are cleaned up (count becomes zero)
 * Used to verify cleanup workflow completion after knowledge base deletion
 *
 * @param {string} fileUid - File UUID
 * @param {number} maxWaitSeconds - Maximum seconds to wait (default: 30)
 * @returns {number} Final count of embeddings (should be 0 if cleanup succeeded)
 */
export function pollEmbeddingsCleanup(fileUid, maxWaitSeconds = 30) {
  for (let i = 0; i < maxWaitSeconds; i++) {
    const count = countEmbeddings(fileUid);
    if (count === 0) {
      return 0;
    }
    if (i < maxWaitSeconds - 1) {
      sleep(1);
    }
  }
  // Final attempt after waiting
  return countEmbeddings(fileUid);
}

/**
 * Poll Milvus vectors until they are cleaned up (count becomes zero)
 * Used to verify cleanup workflow completion after knowledge base deletion
 *
 * @param {string} kbUID - Knowledge base UUID
 * @param {string} fileUID - File UUID
 * @param {number} maxWaitSeconds - Maximum seconds to wait (default: 30)
 * @returns {number} Final count of vectors (should be 0 if cleanup succeeded)
 */
export function pollMilvusVectorsCleanup(kbUID, fileUID, maxWaitSeconds = 30) {
  for (let i = 0; i < maxWaitSeconds; i++) {
    const count = countMilvusVectors(kbUID, fileUID);
    if (count === 0) {
      return 0;
    }
    if (i < maxWaitSeconds - 1) {
      sleep(1);
    }
  }
  // Final attempt after waiting
  return countMilvusVectors(kbUID, fileUID);
}

/**
 * Get file metadata from API endpoint
 * Returns page count and chunk count from the file response
 *
 * @param {string} namespaceId - Namespace ID
 * @param {string} knowledgeBaseId - Knowledge Base ID
 * @param {string} fileUid - File UUID
 * @param {object} headers - Request headers
 * @returns {object} { pages: number, chunks: number, status: string } or null on error
 */
export function getFileMetadata(namespaceId, knowledgeBaseId, fileUid, headers) {
  try {
    const apiUrl = `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${namespaceId}/knowledge-bases/${knowledgeBaseId}/files/${fileUid}`;
    const res = http.request("GET", apiUrl, null, headers);

    if (res.status !== 200) {
      console.log(`getFileMetadata: API returned status ${res.status} for file ${fileUid}`);
      return null;
    }

    const body = res.json();
    if (!body || !body.file) {
      console.log(`getFileMetadata: Invalid response body for file ${fileUid}`);
      return null;
    }

    const file = body.file;

    // Get chunk count from total_chunks field (camelCase in JSON)
    const chunks = file.totalChunks || file.total_chunks || 0;

    // Get page count from length.coordinates array
    // For documents, length.unit will be "UNIT_PAGE" and coordinates contains page numbers
    let pages = 0;
    if (file.length && Array.isArray(file.length.coordinates)) {
      pages = file.length.coordinates.length;
    }

    // Include processing status for debugging
    const processStatus = file.processStatus || file.process_status || "UNKNOWN";

    return { pages, chunks, processStatus };
  } catch (e) {
    console.error(`Failed to get file metadata for ${fileUid}: ${e}`);
    return null;
  }
}

/**
 * Poll file metadata (pages and chunks) from API until they appear or timeout
 * Handles transaction commit delays and API eventual consistency
 *
 * @param {string} namespaceId - Namespace ID
 * @param {string} knowledgeBaseId - Knowledge Base ID
 * @param {string} fileUid - File UUID
 * @param {object} headers - Request headers
 * @param {number} maxWaitSeconds - Maximum seconds to wait (default: 10)
 * @returns {object} { pages: number, chunks: number }
 */
export function pollFileMetadata(namespaceId, knowledgeBaseId, fileUid, headers, maxWaitSeconds = 10) {
  let bestResult = { pages: 0, chunks: 0, processStatus: 'UNKNOWN' };
  let attemptCount = 0;

  for (let i = 0; i < maxWaitSeconds; i++) {
    attemptCount++;
    const metadata = getFileMetadata(namespaceId, knowledgeBaseId, fileUid, headers);

    if (metadata) {
      // Track the best result we've seen (in case one appears before the other)
      if (metadata.pages > bestResult.pages || metadata.chunks > bestResult.chunks) {
        bestResult = metadata;
        console.log(`pollFileMetadata attempt ${attemptCount}: pages=${metadata.pages}, chunks=${metadata.chunks}, status=${metadata.processStatus}`);
      }

      // Return when both are available
      if (metadata.pages > 0 && metadata.chunks > 0) {
        console.log(`pollFileMetadata SUCCESS after ${attemptCount} attempts: pages=${metadata.pages}, chunks=${metadata.chunks}`);
        return metadata;
      }
    }

    if (i < maxWaitSeconds - 1) {
      sleep(1);
    }
  }

  // Final attempt after waiting
  attemptCount++;
  const finalMetadata = getFileMetadata(namespaceId, knowledgeBaseId, fileUid, headers);
  if (finalMetadata && (finalMetadata.pages > bestResult.pages || finalMetadata.chunks > bestResult.chunks)) {
    console.log(`pollFileMetadata FINAL attempt ${attemptCount}: pages=${finalMetadata.pages}, chunks=${finalMetadata.chunks}, status=${finalMetadata.processStatus}`);
    return finalMetadata;
  }

  console.log(`pollFileMetadata TIMEOUT after ${attemptCount} attempts: returning best result pages=${bestResult.pages}, chunks=${bestResult.chunks}`);
  return bestResult;
}

// ============================================================================
// API Polling Helpers for Eventual Consistency
// ============================================================================

import http from "k6/http";

/**
 * Poll chunks API until chunks appear or timeout
 * Handles API-level eventual consistency (transaction delays, caching, etc.)
 *
 * @param {string} apiUrl - Full URL to list chunks endpoint
 * @param {object} headers - Request headers (authentication, etc.)
 * @param {number} maxWaitSeconds - Maximum seconds to wait (default: 15)
 * @returns {number} Final count of chunks returned by API
 */
export function pollChunksAPI(apiUrl, headers, maxWaitSeconds = 15) {
  for (let i = 0; i < maxWaitSeconds; i++) {
    const res = http.request("GET", apiUrl, null, headers);

    if (res.status === 200) {
      try {
        const json = res.json();
        const chunks = Array.isArray(json.chunks) ? json.chunks : [];
        if (chunks.length > 0) {
          return chunks.length;
        }
      } catch (e) {
        // Ignore parse errors, continue polling
      }
    }

    if (i < maxWaitSeconds - 1) {
      sleep(1);
    }
  }

  // Final attempt after waiting
  const finalRes = http.request("GET", apiUrl, null, headers);
  if (finalRes.status === 200) {
    try {
      const json = finalRes.json();
      const chunks = Array.isArray(json.chunks) ? json.chunks : [];
      return chunks.length;
    } catch (e) {
      return 0;
    }
  }

  return 0;
}

// ============================================================================
// RAG Update Framework Helpers
// ============================================================================

/**
 * Poll database for knowledge base by name pattern
 * Used to find staging/rollback KBs during update workflow
 *
 * @param {string} namePattern - SQL LIKE pattern (e.g., '%staging', '%rollback')
 * @returns {Array} Array of knowledge base rows from database
 */
export function findKnowledgeBaseByPattern(namePattern) {
  try {
    const query = `SELECT * FROM knowledge_base WHERE id LIKE $1`;
    const results = safeQuery(query, namePattern);
    return results ? results.map(row => normalizeDBRow(row)) : [];
  } catch (e) {
    console.error(`Failed to find knowledge base by pattern ${namePattern}: ${e}`);
    return [];
  }
}

/**
 * Poll for update completion by monitoring knowledge base update status
 * Waits until knowledge base reaches "completed" status or timeout
 *
 * @param {object} client - gRPC client
 * @param {object} data - Test data with metadata
 * @param {string} knowledgeBaseUid - Knowledge Base UID to monitor
 * @param {number} maxWaitSeconds - Maximum seconds to wait (default: 300 = 5 minutes)
 * @returns {boolean} True if update completed, false if timeout
 */
export function pollUpdateCompletion(client, data, knowledgeBaseUid, maxWaitSeconds = 900) {
  console.log(`[POLL START] Polling for knowledgeBaseUid=${knowledgeBaseUid}, maxWait=${maxWaitSeconds}s`);

  let notFoundCount = 0;
  const MAX_NOT_FOUND_WAIT = 60; // Wait up to 60s for KB to appear in status list (workflow startup time)

  for (let i = 0; i < maxWaitSeconds; i++) {
    const res = client.invoke(
      "artifact.artifact.v1alpha.ArtifactPrivateService/GetKnowledgeBaseUpdateStatusAdmin",
      {},
      data.metadata
    );

    // k6 gRPC returns res.status in various forms, check if response has valid message
    if (res.message && res.message.details) { // Check for successful response with data
      const statuses = res.message.details || [];
      const kbStatus = statuses.find(s => s.knowledgeBaseUid === knowledgeBaseUid);

      // Debug logging on first attempt and periodically
      if (i === 0 || (i > 0 && i % 30 === 0)) {
        console.log(`Poll attempt ${i + 1}: Looking for knowledgeBaseUid=${knowledgeBaseUid}, found ${statuses.length} statuses`);
        if (statuses.length > 0 && !kbStatus) {
          console.log(`Available UIDs: ${statuses.map(s => s.knowledgeBaseUid).join(', ')}`);
        }
      }

      if (kbStatus) {
        // KB found in status list - reset not-found counter
        notFoundCount = 0;

        // Check enum status using numeric values (k6 gRPC returns enums as numbers)
        // KNOWLEDGE_BASE_UPDATE_STATUS_COMPLETED = 6
        if (kbStatus.status === 6 || kbStatus.status === "KNOWLEDGE_BASE_UPDATE_STATUS_COMPLETED") {
          console.log(`✓ Update completed successfully for knowledge base ${knowledgeBaseUid}`);
          return true;
        }
        // KNOWLEDGE_BASE_UPDATE_STATUS_FAILED = 7
        if (kbStatus.status === 7 || kbStatus.status === "KNOWLEDGE_BASE_UPDATE_STATUS_FAILED") {
          console.error(`✗ Update FAILED for knowledge base ${knowledgeBaseUid}`);
          console.error(`   Error message: ${kbStatus.errorMessage || 'No error message provided'}`);
          console.error(`   Workflow ID: ${kbStatus.workflowId || 'N/A'}`);
          return false;
        }
        // KNOWLEDGE_BASE_UPDATE_STATUS_ABORTED = 8
        if (kbStatus.status === 8 || kbStatus.status === "KNOWLEDGE_BASE_UPDATE_STATUS_ABORTED") {
          console.error(`✗ Update ABORTED for knowledge base ${knowledgeBaseUid}`);
          return false;
        }
        // Log current status periodically
        if (i === 0 || (i > 0 && i % 30 === 0)) {
          console.log(`   Current status: ${kbStatus.status} (${typeof kbStatus.status})`);
        }
      } else {
        // KB not found in status list yet - might be race condition with workflow startup
        notFoundCount++;

        if (notFoundCount <= MAX_NOT_FOUND_WAIT) {
          // Still within grace period - workflow might be starting up
          if (notFoundCount === 1 || notFoundCount % 10 === 0) {
            console.log(`   KB not in status list yet (${notFoundCount}s) - waiting for workflow to start...`);
          }
        } else {
          // Exceeded grace period - workflow likely failed to start or KB was deleted
          console.error(`   KB not found in status list after ${MAX_NOT_FOUND_WAIT}s - workflow may have failed to start`);
          return false;
        }
      }
    } else {
      // Log gRPC error for debugging
      if (i === 0 || i % 60 === 0) { // Log every minute to avoid spam
        console.error(`GetKnowledgeBaseUpdateStatus error (attempt ${i + 1}): status=${res.status}, error=${res.error ? JSON.stringify(res.error) : 'none'}`);
      }
    }

    if (i < maxWaitSeconds - 1) {
      sleep(1);
    }
  }

  console.error(`Timeout waiting for update completion of knowledge base ${knowledgeBaseUid}`);
  return false;
}

/**
 * Verify rollback KB exists in database
 * Checks for KB with -rollback suffix and correct attributes
 *
 * @param {string} knowledgeBaseId - Original knowledge base ID (without suffix)
 * @param {string} ownerUid - Owner UID
 * @returns {Array} Array of rollback KB rows (should be 1 or 0)
 */
/**
 * Convert PostgreSQL byte array to string
 * PostgreSQL text fields sometimes come back as byte arrays from the driver
 */
function byteArrayToString(value) {
  if (Array.isArray(value) && value.length > 0 && typeof value[0] === 'number') {
    return String.fromCharCode(...value);
  }
  return value;
}

/**
 * Normalize database row by converting byte arrays to strings
 */
function normalizeDBRow(row) {
  if (!row) return row;
  const normalized = {};
  for (const [key, value] of Object.entries(row)) {
    normalized[key] = byteArrayToString(value);
  }
  return normalized;
}

export function verifyRollbackKB(knowledgeBaseId, ownerUid) {
  const rollbackKbId = `${knowledgeBaseId}-rollback`;
  try {
    const query = `
      SELECT id, uid, staging, update_status, rollback_retention_until, tags, active_collection_uid
      FROM knowledge_base
      WHERE id = $1 AND owner = $2
    `;
    const results = safeQuery(query, rollbackKbId, ownerUid);
    return results ? results.map(row => normalizeDBRow(row)) : [];
  } catch (e) {
    console.error(`Failed to verify rollback KB for ${knowledgeBaseId}: ${e}`);
    return [];
  }
}

/**
 * Verify staging KB exists in database
 * Checks for KB with -staging suffix during update workflow
 *
 * @param {string} knowledgeBaseId - Original knowledge base ID (without suffix)
 * @param {string} ownerUid - Owner UID
 * @returns {Array} Array of staging KB rows (should be 1 or 0)
 */
export function verifyStagingKB(knowledgeBaseId, ownerUid) {
  const stagingKbId = `${knowledgeBaseId}-staging`;
  try {
    const query = `
      SELECT uid, id, staging, update_status, active_collection_uid
      FROM knowledge_base
      WHERE id = $1 AND owner = $2
    `;
    const results = safeQuery(query, stagingKbId, ownerUid);
    return results ? results.map(row => normalizeDBRow(row)) : [];
  } catch (e) {
    console.error(`Failed to verify staging KB for ${knowledgeBaseId}: ${e}`);
    return [];
  }
}

/**
 * Get knowledge base by knowledge_base_id and owner
 * Helper to retrieve knowledge base details from database
 *
 * @param {string} knowledgeBaseId - Knowledge Base ID
 * @param {string} ownerUid - Owner UID
 * @returns {Array} Array of knowledge base rows (should be 1 or 0)
 */
export function getKnowledgeBaseByIdAndOwner(knowledgeBaseId, ownerUid) {
  try {
    const query = `
      SELECT * FROM knowledge_base
      WHERE id = $1 AND owner = $2
    `;
    const results = safeQuery(query, knowledgeBaseId, ownerUid);
    return results ? results.map(row => normalizeDBRow(row)) : [];
  } catch (e) {
    console.error(`Failed to get knowledge base ${knowledgeBaseId}: ${e}`);
    return [];
  }
}

/**
 * Poll for staging KB to be cleaned up (soft-deleted)
 * Used after abort operations to wait for async cleanup to complete
 *
 * @param {string} stagingKBID - Staging KB ID to check
 * @param {string} ownerUid - Owner UID
 * @param {number} maxWaitSeconds - Maximum time to wait (default 10s)
 * @returns {boolean} True if staging KB is cleaned up or doesn't exist
 */
export function pollStagingKBCleanup(stagingKBID, ownerUid, maxWaitSeconds = 10) {
  console.log(`[POLL] Waiting for staging KB cleanup: ${stagingKBID}, maxWait=${maxWaitSeconds}s`);

  for (let i = 0; i < maxWaitSeconds; i++) {
    const stagingKB = getKnowledgeBaseByIdAndOwner(stagingKBID, ownerUid);

    // Log on first attempt and every 3 seconds
    if (i === 0 || i % 3 === 0) {
      if (stagingKB && stagingKB.length > 0) {
        console.log(`[POLL] Attempt ${i + 1}: Staging KB found, delete_time=${stagingKB[0].delete_time}, update_status=${stagingKB[0].update_status}`);
      } else {
        console.log(`[POLL] Attempt ${i + 1}: Staging KB not found (cleaned up)`);
      }
    }

    // Check if staging KB is cleaned up
    if (!stagingKB || stagingKB.length === 0 || stagingKB[0].delete_time !== null) {
      console.log(`[POLL] Staging KB cleanup confirmed after ${i + 1}s`);
      return true;
    }

    if (i < maxWaitSeconds - 1) {
      sleep(1);
    }
  }

  console.log(`[POLL] Timeout: Staging KB not cleaned up after ${maxWaitSeconds}s`);
  return false;
}

/**
 * Poll for rollback KB cleanup completion (soft-delete + resource purge)
 * Waits for the scheduled cleanup workflow to:
 * 1. Soft-delete the KB (set delete_time)
 * 2. Purge all resources (files, converted_files, chunks, embeddings)
 *
 * @param {string} rollbackKBID - Rollback KB ID
 * @param {string} rollbackKBUID - Rollback KB UID
 * @param {string} ownerUid - Owner UID
 * @param {number} maxWaitSeconds - Maximum seconds to wait (default: 30)
 * @returns {boolean} True if cleanup completed, false if timeout
 */
export function pollRollbackKBCleanup(rollbackKBID, rollbackKBUID, ownerUid, maxWaitSeconds = 30) {
  console.log(`[POLL] Waiting for rollback KB cleanup: ${rollbackKBID} (UID: ${rollbackKBUID}), maxWait=${maxWaitSeconds}s`);

  // Note: chunk and embedding tables don't have kb_uid (they use source_uid/source_table)
  // and don't have delete_time (they're hard deleted). We only check converted_file which has kb_uid.
  const convertedFilesQuery = `SELECT COUNT(*) as count FROM converted_file WHERE kb_uid = $1`;

  for (let i = 0; i < maxWaitSeconds; i++) {
    const rollbackKB = getKnowledgeBaseByIdAndOwner(rollbackKBID, ownerUid);
    const filesCount = countFilesInKnowledgeBase(rollbackKBUID);

    const convertedFilesResult = safeQuery(convertedFilesQuery, rollbackKBUID);
    const convertedFilesCount = convertedFilesResult && convertedFilesResult.length > 0 ? parseInt(convertedFilesResult[0].count) : 0;

    // Log on first attempt and every 5 seconds
    if (i === 0 || i % 5 === 0) {
      if (rollbackKB && rollbackKB.length > 0) {
        console.log(`[POLL] Attempt ${i + 1}: Rollback KB found, delete_time=${rollbackKB[0].delete_time}, files=${filesCount}, converted=${convertedFilesCount}`);
      } else {
        console.log(`[POLL] Attempt ${i + 1}: Rollback KB not found (fully deleted)`);
      }
    }

    // Check if rollback KB is soft-deleted AND key resources are purged
    // If files and converted_files are gone, chunks/embeddings are also gone (cleanup is atomic)
    const isSoftDeleted = !rollbackKB || rollbackKB.length === 0 || rollbackKB[0].delete_time !== null;
    const resourcesPurged = filesCount === 0 && convertedFilesCount === 0;

    if (isSoftDeleted && resourcesPurged) {
      console.log(`[POLL] Rollback KB cleanup confirmed after ${i + 1}s`);
      return true;
    }

    if (i < maxWaitSeconds - 1) {
      sleep(1);
    }
  }

  console.log(`[POLL] Timeout: Rollback KB not cleaned up after ${maxWaitSeconds}s`);
  return false;
}

/**
 * Verify resource kb_uid references after atomic swap
 * CRITICAL: This prevents regression of the kb_uid bug where resources
 * pointed to old KB UIDs after swap, breaking queries
 *
 * @param {string} newProdKBUID - New production KB UID (staging KB's UID)
 * @param {string} rollbackKBUID - Rollback KB UID (original KB's UID)
 * @returns {object} Result with counts and correctness flags
 */
export function verifyResourceKBUIDs(newProdKBUID, rollbackKBUID) {
  const result = {
    fileCount: 0,
    chunkCount: 0,
    embeddingCount: 0,
    convertedFileCount: 0,
    filesCorrect: false,
    chunksCorrect: false,
    embeddingsCorrect: false,
    convertedFilesCorrect: false,
  };

  try {
    // Check file
    const fileQuery = `SELECT COUNT(*) as count FROM file WHERE kb_uid = $1`;
    const fileResults = safeQuery(fileQuery, newProdKBUID);
    result.fileCount = fileResults && fileResults.length > 0 ? parseInt(fileResults[0].count) : 0;
    result.filesCorrect = result.fileCount > 0;

    // Check chunk
    const chunkQuery = `SELECT COUNT(*) as count FROM chunk WHERE kb_uid = $1`;
    const chunkResults = safeQuery(chunkQuery, newProdKBUID);
    result.chunkCount = chunkResults && chunkResults.length > 0 ? parseInt(chunkResults[0].count) : 0;
    result.chunksCorrect = result.chunkCount > 0;

    // Check embedding (optional - may not exist yet)
    const embeddingQuery = `SELECT COUNT(*) as count FROM embedding WHERE kb_uid = $1`;
    const embeddingResults = safeQuery(embeddingQuery, newProdKBUID);
    result.embeddingCount = embeddingResults && embeddingResults.length > 0 ? parseInt(embeddingResults[0].count) : 0;
    result.embeddingsCorrect = true; // Always true, embeddings are optional

    // Check converted_file
    const convertedQuery = `SELECT COUNT(*) as count FROM converted_file WHERE kb_uid = $1`;
    const convertedResults = safeQuery(convertedQuery, newProdKBUID);
    result.convertedFileCount = convertedResults && convertedResults.length > 0 ? parseInt(convertedResults[0].count) : 0;
    result.convertedFilesCorrect = result.convertedFileCount > 0;

  } catch (e) {
    console.error(`Failed to verify resource KB UIDs: ${e}`);
  }

  return result;
}

/**
 * Count files in a knowledge base by KB UID
 *
 * @param {string} knowledgeBaseUid - Knowledge Base UID
 * @returns {number} Number of files in knowledge base
 */
export function countFilesInKnowledgeBase(knowledgeBaseUid) {
  try {
    const query = `SELECT COUNT(*) as count FROM file WHERE kb_uid = $1 AND delete_time IS NULL`;
    const results = safeQuery(query, knowledgeBaseUid);
    return results && results.length > 0 ? parseInt(results[0].count) : 0;
  } catch (e) {
    console.error(`Failed to count files in knowledge base ${knowledgeBaseUid}: ${e}`);
    return 0;
  }
}

/**
 * Get all knowledge bases matching a pattern (uses SQL LIKE)
 *
 * @param {string} pattern - SQL LIKE pattern (e.g., "kb%")
 * @param {string} ownerUid - Owner UID
 * @returns {Array} Array of knowledge base objects
 */
export function getKnowledgeBasesByPattern(pattern, ownerUid) {
  try {
    // Note: 'name' column removed from DB - using 'id' for pattern matching
    const query = `
      SELECT uid, id, staging, update_status, delete_time, rollback_retention_until
      FROM knowledge_base
      WHERE owner = $1 AND id LIKE $2
      ORDER BY create_time ASC
    `;
    return safeQuery(query, ownerUid, pattern);
  } catch (e) {
    console.error(`Failed to get knowledge bases by pattern ${pattern}: ${e}`);
    return null;
  }
}

/**
 * Poll for staging KB to appear during Phase 1 (Prepare)
 *
 * @param {string} knowledgeBaseId - Original knowledge base ID (without suffix)
 * @param {string} ownerUid - Owner UID
 * @param {number} maxWaitSeconds - Maximum seconds to wait (default: 30)
 * @returns {boolean} True if staging KB found, false otherwise
 */
export function pollForStagingKB(knowledgeBaseId, ownerUid, maxWaitSeconds = 30) {
  for (let i = 0; i < maxWaitSeconds; i++) {
    const stagingKBs = verifyStagingKB(knowledgeBaseId, ownerUid);
    if (stagingKBs && stagingKBs.length > 0) {
      return true;
    }
    if (i < maxWaitSeconds - 1) {
      sleep(1);
    }
  }
  return false;
}

/**
 * Wait for all ongoing updates to complete before starting a new one
 * This prevents "Update already in progress" errors
 */
export function waitForAllUpdatesComplete(client, data, maxWaitSeconds = 60) {
  console.log(`[WAIT] Waiting for all ongoing updates to complete (max ${maxWaitSeconds}s)...`);

  // OPTIMIZATION: Poll more frequently for faster detection (every 0.5s)
  const pollIntervalSeconds = 0.5;
  const maxIterations = maxWaitSeconds / pollIntervalSeconds;

  for (let i = 0; i < maxIterations; i++) {
    const res = client.invoke(
      "artifact.artifact.v1alpha.ArtifactPrivateService/GetKnowledgeBaseUpdateStatusAdmin",
      {},
      data.metadata
    );

    if (res && res.message && res.message.details) {
      // Check for ANY active update status, not just "updating"
      const activeStatuses = ["updating", "swapping", "validating", "syncing"];
      const active = res.message.details.filter(s => activeStatuses.includes(s.status));

      if (active.length === 0) {
        const elapsedSeconds = (i * pollIntervalSeconds).toFixed(1);
        console.log(`[WAIT] All updates complete after ${elapsedSeconds}s`);
        return true;
      }

      // Log every 10 seconds
      if (i % 20 === 0 && i > 0) {
        const elapsedSeconds = (i * pollIntervalSeconds).toFixed(1);
        console.log(`[WAIT] Still waiting... ${active.length} knowledge bases active (${elapsedSeconds}s elapsed)`);
      }
    }

    sleep(pollIntervalSeconds);
  }

  console.error(`[WAIT] Timeout: Updates still in progress after ${maxWaitSeconds}s`);
  return false;
}

// ============================================================================
// Content Generation Helpers
// ============================================================================

/**
 * Generate realistic article-like content for LLM processing tests
 * Creates markdown-formatted articles with varied vocabulary and structure
 *
 * @param {number} targetLength - Target length in characters
 * @returns {string} Generated article content
 */
export function generateArticle(targetLength) {
  const topics = [
    "machine learning", "data science", "cloud computing", "artificial intelligence",
    "software engineering", "database systems", "web development", "mobile applications"
  ];
  const sentences = [
    "The rapid advancement in {topic} has revolutionized how businesses operate in the modern era.",
    "Researchers continue to explore innovative approaches to {topic} that push the boundaries of what's possible.",
    "Understanding {topic} requires a comprehensive grasp of both theoretical foundations and practical applications.",
    "Industry leaders emphasize the importance of {topic} in driving digital transformation initiatives.",
    "Recent breakthroughs in {topic} demonstrate the potential for solving complex real-world problems.",
    "Organizations are increasingly investing in {topic} to maintain competitive advantages in the market.",
    "The evolution of {topic} has created new opportunities for professionals across various industries.",
    "Experts predict that {topic} will continue to shape the future of technology and innovation."
  ];

  let article = "# Article on Technology and Innovation\n\n";
  let currentLength = article.length;
  let paragraphCount = 0;

  while (currentLength < targetLength) {
    const topic = topics[Math.floor(Math.random() * topics.length)];
    const sentenceCount = 3 + Math.floor(Math.random() * 3); // 3-5 sentences per paragraph
    let paragraph = "";

    for (let i = 0; i < sentenceCount && currentLength < targetLength; i++) {
      const sentence = sentences[Math.floor(Math.random() * sentences.length)].replace("{topic}", topic);
      paragraph += sentence + " ";
      currentLength += sentence.length + 1;
    }

    article += paragraph.trim() + "\n\n";
    paragraphCount++;

    // Add section headers every few paragraphs
    if (paragraphCount % 5 === 0 && currentLength < targetLength) {
      const sectionTitle = `## Section ${Math.floor(paragraphCount / 5)}: ${topic.charAt(0).toUpperCase() + topic.slice(1)}\n\n`;
      article += sectionTitle;
      currentLength += sectionTitle.length;
    }
  }

  return article.substring(0, targetLength);
}

/**
 * Wait for ALL file processing to complete (database-level check).
 *
 * Uses direct database queries to check for any files in processing states.
 * This is faster than API polling and doesn't require authentication.
 *
 * WHEN TO USE:
 * - Teardown/cleanup: Ensure no workflows are running before deleting knowledge bases
 * - Race condition prevention: Avoid "collection does not exist" errors
 *
 * VS waitForFileProcessingComplete:
 * - This checks DB directly (faster, no auth needed)
 * - waitForFileProcessingComplete checks via API (proper test isolation, includes permissions)
 *
 * VS waitForMultipleFilesProcessingComplete:
 * - This checks ALL files matching a pattern (cleanup use case)
 * - waitForMultipleFilesProcessingComplete checks a specific list (test assertions)
 *
 * @param {number} maxWaitSeconds - Maximum time to wait (default: 120s)
 * @param {string|null} dbIDPrefix - Knowledge Base ID prefix to filter (e.g., "test-ab12-"), null = global
 * @returns {boolean} - True if all processing complete, false if timeout
 */
export function waitForAllFileProcessingComplete(maxWaitSeconds = 120, dbIDPrefix = null) {
  const scope = dbIDPrefix ? `this test (${dbIDPrefix})` : "GLOBAL";
  console.log(`${scope}: Waiting for file processing to complete...`);
  let waited = 0;

  while (waited < maxWaitSeconds) {
    // CRITICAL: Filter by dbIDPrefix if provided to only wait for THIS test's files
    // This prevents tests from blocking on each other's file processing
    const prefixFilter = dbIDPrefix
      ? `AND kb.id LIKE '${dbIDPrefix}%'`
      : '';

    const processingQuery = `
      SELECT COUNT(*) as count
      FROM file kbf
      JOIN knowledge_base kb ON kbf.kb_uid = kb.uid
      WHERE kbf.process_status IN (
          'FILE_PROCESS_STATUS_NOTSTARTED',
          'FILE_PROCESS_STATUS_PROCESSING',
          'FILE_PROCESS_STATUS_CHUNKING',
          'FILE_PROCESS_STATUS_EMBEDDING'
        )
        AND kbf.delete_time IS NULL
        AND kb.delete_time IS NULL
        ${prefixFilter}
    `;

    const result = safeQuery(processingQuery);
    const processingCount = result && result.length > 0 ? parseInt(result[0].count) : 0;

    if (processingCount === 0) {
      console.log(`${scope}: All file processing complete (waited ${waited}s)`);
      return true;
    }

    // Log every 5 seconds to avoid spam
    if (waited % 5 === 0) {
      console.log(`${scope}: ${processingCount} files still processing (waited ${waited}s/${maxWaitSeconds}s)`);
    }

    sleep(1);
    waited++;
  }

  console.warn(`${scope}: Timeout waiting for file processing after ${maxWaitSeconds}s`);
  return false;
}

/**
 * Wait for a single file to complete processing via API polling.
 *
 * Uses REST API to poll file status, providing proper test isolation and
 * respecting API-level permissions/authentication.
 *
 * WHEN TO USE:
 * - Test assertions: Verify a specific file completed successfully
 * - Single file uploads: Wait for one file to finish before proceeding
 *
 * VS waitForAllFileProcessingComplete:
 * - This checks via API (proper test isolation, includes permissions)
 * - waitForAllFileProcessingComplete checks DB directly (faster, cleanup use case)
 *
 * VS waitForMultipleFilesProcessingComplete:
 * - This polls one file at a time (simpler, good for single uploads)
 * - waitForMultipleFilesProcessingComplete batches multiple files (more efficient for bulk)
 *
 * Polls the Get File API endpoint to check processStatus until:
 * - FILE_PROCESS_STATUS_COMPLETED: Returns { completed: true, status: "COMPLETED" }
 * - FILE_PROCESS_STATUS_FAILED: Returns { completed: false, status: "FAILED", error: "..." }
 * - Timeout: Returns { completed: false, status: "TIMEOUT" }
 *
 * @param {string} namespaceId - The namespace ID
 * @param {string} knowledgeBaseId - The knowledge base ID
 * @param {string} fileUid - The file UID to poll
 * @param {object} headers - HTTP headers including Authorization
 * @param {number} maxWaitSeconds - Maximum seconds to wait (default: 300)
 * @param {number} notStartedThreshold - Seconds to wait before failing if file stuck in NOTSTARTED (default: 60)
 * @returns {object} - { completed: boolean, status: string, error?: string }
 */
export function waitForFileProcessingComplete(namespaceId, knowledgeBaseId, fileUid, headers, maxWaitSeconds = 300, notStartedThreshold = 60) {
  console.log(`Waiting for file ${fileUid} to complete processing (max ${maxWaitSeconds}s)...`);

  let consecutiveNotStarted = 0;
  let consecutiveErrors = 0;
  const NOTSTARTED_THRESHOLD = notStartedThreshold; // If file stays NOTSTARTED for this long, workflow likely never started
  const MAX_CONSECUTIVE_ERRORS = 5; // Tolerate up to 5 consecutive API errors before giving up

  // Adaptive polling: Start with faster polls, then back off
  // This reduces load on resource-constrained systems while maintaining responsiveness
  let pollInterval = 1; // Start with 1 second
  let elapsed = 0;

  while (elapsed < maxWaitSeconds) {
    // Check status FIRST, then sleep (don't waste the first interval)
    const statusRes = http.request(
      "GET",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${namespaceId}/knowledge-bases/${knowledgeBaseId}/files/${fileUid}`,
      null,
      headers
    );

    // Check for HTTP errors first (e.g., 404 Not Found means knowledge base or file was deleted)
    if (statusRes.status === 404) {
      console.error(`✗ File or knowledge base not found (404) - knowledge base/file may have been deleted`);
      return { completed: false, status: "NOT_FOUND", error: "Knowledge base or file not found" };
    } else if (statusRes.status >= 500) {
      // 5xx errors might be transient on resource-constrained systems - tolerate a few
      consecutiveErrors++;
      console.warn(`⚠ API error ${statusRes.status} while checking file status (${consecutiveErrors}/${MAX_CONSECUTIVE_ERRORS})`);

      if (consecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
        console.error(`✗ Too many consecutive API errors (${MAX_CONSECUTIVE_ERRORS})`);
        return { completed: false, status: "API_ERROR", error: `HTTP ${statusRes.status} - ${MAX_CONSECUTIVE_ERRORS} consecutive failures` };
      }

      // Back off more aggressively on errors
      sleep(Math.min(pollInterval * 2, 5));
      elapsed += Math.min(pollInterval * 2, 5);
      continue;
    } else if (statusRes.status >= 400) {
      console.error(`✗ Client API error ${statusRes.status} while checking file status`);
      return { completed: false, status: "API_ERROR", error: `HTTP ${statusRes.status}` };
    }

    // Reset error counter on successful response
    consecutiveErrors = 0;

    try {
      const body = statusRes.json();
      const status = body.file ? (body.file.processStatus || body.file.process_status) : "";

      if (status === "FILE_PROCESS_STATUS_COMPLETED") {
        console.log(`✓ File processing completed after ${elapsed}s`);
        return { completed: true, status: "COMPLETED" };
      } else if (status === "FILE_PROCESS_STATUS_FAILED") {
        const errorMsg = body.file && body.file.processOutcome ? body.file.processOutcome : "Unknown error";
        console.log(`✗ File processing failed after ${elapsed}s: ${errorMsg}`);
        return { completed: false, status: "FAILED", error: errorMsg };
      } else if (status === "FILE_PROCESS_STATUS_NOTSTARTED") {
        // FAST-FAIL: If file stays NOTSTARTED for too long, workflow likely never triggered
        // This happens when Temporal worker is down or auto-trigger fails
        consecutiveNotStarted += pollInterval;
        if (consecutiveNotStarted >= NOTSTARTED_THRESHOLD) {
          console.error(`✗ File stuck in NOTSTARTED for ${NOTSTARTED_THRESHOLD}s - workflow likely never triggered (worker down?)`);
          return { completed: false, status: "WORKFLOW_NOT_STARTED", error: `File stuck in NOTSTARTED for ${NOTSTARTED_THRESHOLD}s - ProcessFileWorkflow likely never triggered. Check if Temporal worker is running.` };
        }
      } else {
        // File is processing (PROCESSING, CHUNKING, EMBEDDING, etc.) - reset counter
        consecutiveNotStarted = 0;
      }

      // Log progress every 30 seconds to avoid spam
      if (elapsed > 0 && elapsed % 30 === 0) {
        console.log(`Still waiting for file processing... (${elapsed}s/${maxWaitSeconds}s, status: ${status})`);
      }
    } catch (e) {
      // Log parse errors but continue polling (might be transient)
      consecutiveErrors++;
      if (elapsed === 0 || elapsed % 60 === 0) {
        console.warn(`Parse error checking file status (${consecutiveErrors}/${MAX_CONSECUTIVE_ERRORS}): ${e.message}`);
      }

      if (consecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
        console.error(`✗ Too many consecutive parse errors (${MAX_CONSECUTIVE_ERRORS})`);
        return { completed: false, status: "PARSE_ERROR", error: `${MAX_CONSECUTIVE_ERRORS} consecutive parse errors` };
      }
    }

    // Adaptive polling interval: increase gradually to reduce load on slow systems
    // 0-60s: 1s interval (responsive for quick files)
    // 60-180s: 2s interval (file is taking a while)
    // 180s+: 3s interval (long-running file, reduce API load)
    if (elapsed < 60) {
      pollInterval = 1;
    } else if (elapsed < 180) {
      pollInterval = 2;
    } else {
      pollInterval = 3;
    }

    sleep(pollInterval);
    elapsed += pollInterval;
  }

  console.warn(`⚠ File processing timeout after ${maxWaitSeconds}s`);
  return { completed: false, status: "TIMEOUT" };
}

/**
 * Wait for multiple files to complete processing via API polling (batched).
 *
 * Efficiently polls multiple files in parallel using k6's http.batch().
 * More efficient than calling waitForFileProcessingComplete() in a loop.
 *
 * WHEN TO USE:
 * - Batch uploads: Wait for multiple files uploaded together
 * - Test setup: Ensure all test data is ready before running assertions
 *
 * VS waitForFileProcessingComplete:
 * - This batches multiple API calls (more efficient for 2+ files)
 * - waitForFileProcessingComplete polls one file (simpler for single file)
 *
 * VS waitForAllFileProcessingComplete:
 * - This checks a specific list via API (test assertions on known files)
 * - waitForAllFileProcessingComplete checks DB for ANY matching files (cleanup)
 *
 * Efficiently polls multiple files in parallel, checking all files in each iteration.
 * Returns as soon as all files complete or any file fails.
 *
 * @param {string} namespaceId - The namespace ID
 * @param {string} knowledgeBaseId - The knowledge base ID
 * @param {Array<string>} fileUids - Array of file UIDs to wait for
 * @param {object} headers - HTTP headers including Authorization
 * @param {number} maxWaitSeconds - Maximum seconds to wait (default: 600)
 * @returns {object} - { completed: boolean, status: string, processedCount: number, error?: string }
 */
export function waitForMultipleFilesProcessingComplete(namespaceId, knowledgeBaseId, fileUids, headers, maxWaitSeconds = 600) {
  console.log(`Waiting for ${fileUids.length} files to complete processing (max ${maxWaitSeconds}s)...`);

  const pending = new Set(fileUids);
  const notStartedCounters = {}; // Track consecutive NOTSTARTED for each file
  let processedCount = 0;
  const NOTSTARTED_THRESHOLD = 60; // seconds

  for (let i = 0; i < maxWaitSeconds * 2; i++) { // *2 because we sleep 0.5s
    sleep(0.5);

    // Batch check all pending files
    const checks = Array.from(pending).map(fileUid =>
      http.request(
        "GET",
        `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${namespaceId}/knowledge-bases/${knowledgeBaseId}/files/${fileUid}`,
        null,
        headers
      )
    );

    const pendingArray = Array.from(pending);
    for (let j = 0; j < checks.length; j++) {
      const fileUid = pendingArray[j];
      const response = checks[j];

      try {
        const body = response.json();
        const status = body.file ? (body.file.processStatus || body.file.process_status) : "";

        if (status === "FILE_PROCESS_STATUS_COMPLETED") {
          pending.delete(fileUid);
          delete notStartedCounters[fileUid];
          processedCount++;
        } else if (status === "FILE_PROCESS_STATUS_FAILED") {
          const errorMsg = body.file && body.file.processOutcome ? body.file.processOutcome : "Unknown error";
          console.log(`✗ File ${fileUid} processing failed: ${errorMsg}`);
          return {
            completed: false,
            status: "FAILED",
            processedCount,
            error: `File ${fileUid}: ${errorMsg}`
          };
        } else if (status === "FILE_PROCESS_STATUS_NOTSTARTED") {
          // FAST-FAIL: Track how long file has been stuck in NOTSTARTED
          notStartedCounters[fileUid] = (notStartedCounters[fileUid] || 0) + 0.5;
          if (notStartedCounters[fileUid] >= NOTSTARTED_THRESHOLD) {
            console.error(`✗ File ${fileUid} stuck in NOTSTARTED for ${NOTSTARTED_THRESHOLD}s - workflow likely never triggered`);
            return {
              completed: false,
              status: "WORKFLOW_NOT_STARTED",
              processedCount,
              error: `File ${fileUid} stuck in NOTSTARTED for ${NOTSTARTED_THRESHOLD}s - ProcessFileWorkflow likely never triggered. Check if Temporal worker is running.`
            };
          }
        } else {
          // File is processing (PROCESSING, CHUNKING, EMBEDDING, etc.) - reset counter
          delete notStartedCounters[fileUid];
        }
      } catch (e) {
        // Continue polling on parse errors
      }
    }

    // All files completed
    if (pending.size === 0) {
      console.log(`✓ All ${fileUids.length} files completed processing after ${(i * 0.5).toFixed(1)}s`);
      return { completed: true, status: "COMPLETED", processedCount };
    }

    // Log progress every 30 seconds to avoid spam
    if (i > 0 && (i * 0.5) % 30 === 0) {
      console.log(`Still waiting... ${processedCount}/${fileUids.length} completed (${(i * 0.5).toFixed(0)}s/${maxWaitSeconds}s)`);
    }
  }

  console.warn(`⚠ Multiple files processing timeout after ${maxWaitSeconds}s (${processedCount}/${fileUids.length} completed)`);
  return { completed: false, status: "TIMEOUT", processedCount };
}

/**
 * Upload a file with automatic retry logic to handle transient failures.
 *
 * When tests run in parallel, file uploads can intermittently fail with:
 * - HTTP 500 errors (temporary resource exhaustion)
 * - Response returns UID: null (race condition in resource allocation)
 *
 * This function implements exponential backoff retry to handle these transient failures.
 *
 * @param {string} url - The upload endpoint URL
 * @param {object} payload - The file upload payload (name, type, content)
 * @param {object} headers - HTTP headers including Authorization
 * @param {number} maxRetries - Maximum number of retry attempts (default: 3)
 * @returns {object|null} - HTTP response object, or null if all retries failed
 */
export function uploadFileWithRetry(url, payload, headers, maxRetries = 3) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    const response = http.request("POST", url, JSON.stringify(payload), headers);

    try {
      // Check if response is successful and has a valid file UID
      const body = response.json();
      const file = body.file || body;
      const fileUid = file ? file.uid : null;

      if (response.status === 200 && fileUid) {
        if (attempt > 1) {
          console.log(`✓ File upload succeeded on attempt ${attempt}/${maxRetries}`);
        }
        return response;
      }

      // Log the failure reason
      const errorMsg = body.message || body.error || 'Unknown error';
      console.log(`⚠ Upload attempt ${attempt}/${maxRetries} failed: status=${response.status}, fileUid=${fileUid}, error=${errorMsg}`);

    } catch (e) {
      console.log(`⚠ Upload attempt ${attempt}/${maxRetries} exception: ${e}`);
    }

    // Exponential backoff before retry (0.5s, 1s, 2s, 4s, ...)
    if (attempt < maxRetries) {
      const backoff = Math.pow(2, attempt - 1) * 0.5;
      console.log(`⏳ Retrying in ${backoff.toFixed(1)}s...`);
      sleep(backoff);
    }
  }

  console.log(`✗ File upload failed after ${maxRetries} attempts`);
  return null;
}

/**
 * Add random delay to stagger parallel test execution and reduce resource contention.
 *
 * When multiple tests run in parallel, they compete for:
 * - Database connection pool
 * - MinIO connections
 * - Temporal worker capacity
 * - API gateway rate limits
 *
 * Adding a random delay at test start spreads out resource usage and reduces
 * the probability of simultaneous requests causing temporary exhaustion.
 *
 * @param {number} maxDelaySeconds - Maximum delay in seconds (default: 2)
 */
export function staggerTestExecution(maxDelaySeconds = 2) {
  const delay = Math.random() * maxDelaySeconds;
  console.log(`⏸ Test stagger: ${delay.toFixed(2)}s delay to reduce parallel contention`);
  sleep(delay);
}

/**
 * Poll until a KB appears in the database with the expected state.
 * Used to deterministically wait for KB creation after workflow operations.
 *
 * @param {string} knowledgeBaseId - The knowledge base ID to check
 * @param {string} ownerUid - The owner UID
 * @param {number} maxWaitSeconds - Maximum time to wait (default: 30)
 * @param {boolean} expectDeleted - If true, wait for delete_time to be set (default: false)
 * @returns {object|null} - The KB object if found, null if timeout
 */
export function pollForKBState(knowledgeBaseId, ownerUid, maxWaitSeconds = 30, expectDeleted = false) {
  const startTime = Date.now();

  while ((Date.now() - startTime) / 1000 < maxWaitSeconds) {
    const kbs = getKnowledgeBaseByIdAndOwner(knowledgeBaseId, ownerUid);

    if (!kbs || kbs.length === 0) {
      if (expectDeleted) {
        // KB fully deleted (not even soft-deleted) - this is OK if expecting deletion
        return null;
      }
      sleep(0.5);
      continue;
    }

    const kb = kbs[0];

    if (expectDeleted) {
      // Waiting for soft-deletion (delete_time set)
      if (kb.delete_time !== null) {
        return kb;
      }
    } else {
      // Waiting for KB to exist and be active
      if (kb.delete_time === null) {
        return kb;
      }
    }

    sleep(0.5);
  }

  return null;
}

/**
 * Poll until staging KB is soft-deleted after an update completes.
 * The update workflow should clean up the staging KB as part of swap/completion.
 *
 * @param {string} knowledgeBaseId - The base knowledge base ID (staging KB is {knowledgeBaseId}-staging)
 * @param {string} ownerUid - The owner UID
 * @param {number} maxWaitSeconds - Maximum time to wait (default: 10)
 * @returns {boolean} - True if staging KB is deleted, false if timeout
 */
export function pollForStagingKBCleanup(knowledgeBaseId, ownerUid, maxWaitSeconds = 10) {
  const stagingKBID = `${knowledgeBaseId}-staging`;
  const result = pollForKBState(stagingKBID, ownerUid, maxWaitSeconds, true);

  // Result is null if KB is fully deleted, or has delete_time set if soft-deleted
  // Both are acceptable states for "cleaned up"
  return result === null || (result && result.delete_time !== null);
}

/**
 * Poll until rollback KB exists after an update completes.
 * The update workflow creates the rollback KB during the swap phase.
 *
 * @param {string} knowledgeBaseId - The base knowledge base ID (rollback KB is {knowledgeBaseId}-rollback)
 * @param {string} ownerUid - The owner UID
 * @param {number} maxWaitSeconds - Maximum time to wait (default: 10)
 * @returns {object|null} - The rollback KB object if found, null if timeout
 */
export function pollForRollbackKBCreation(knowledgeBaseId, ownerUid, maxWaitSeconds = 10) {
  const rollbackKBID = `${knowledgeBaseId}-rollback`;
  return pollForKBState(rollbackKBID, ownerUid, maxWaitSeconds, false);
}

// ============================================================================
// Global Test Cleanup Helpers
// ============================================================================

/**
 * Clean up all previous test knowledge bases before starting a new test run.
 *
 * SAFETY: This function should ONLY be called in setup() BEFORE generating
 * the current test's unique prefix. At that point, ALL test-% knowledge bases are
 * from previous test runs and safe to delete.
 *
 * This prevents zombie files/knowledge-bases from previous failed test runs from
 * blocking the worker queue and causing new tests to hang.
 *
 * Process:
 * 1. Mark any stuck files (NOTSTARTED/PROCESSING/CHUNKING/EMBEDDING) as FAILED
 * 2. Delete all test-% knowledge bases via API (respects business logic)
 *
 * USAGE PATTERN (recommended order):
 * ```javascript
 * export function setup() {
 *   // 1. Stagger execution (optional, for parallel test runs)
 *   helper.staggerTestExecution(2);
 *
 *   // 2. Authenticate FIRST (required for API calls)
 *   const loginResp = http.request("POST", `${mgmtHost}/auth/login`, ...);
 *   const header = { headers: { Authorization: `Bearer ${loginResp.json().accessToken}` } };
 *   const userResp = http.request("GET", `${mgmtHost}/user`, {}, header);
 *
 *   // 3. CLEAN UP ALL test-% knowledge bases from previous runs
 *   //    CRITICAL: Do this BEFORE generating this run's prefix!
 *   helper.cleanupPreviousTestKnowledgeBases(userResp.json().user.id, header);
 *
 *   // 4. NOW generate this run's unique prefix (after cleanup)
 *   const dbIDPrefix = constant.generateDBIDPrefix(); // e.g., "test-ab12-"
 *
 *   // 5. Create knowledge bases using the unique prefix
 *   //    e.g., `${dbIDPrefix}my-kb-1`, `${dbIDPrefix}my-kb-2`
 *   //    These will be the ONLY test-% knowledge bases in the system
 *
 *   return { header, userResp, dbIDPrefix };
 * }
 * ```
 *
 * WHY THIS IS SAFE FOR PARALLEL TESTS:
 * - Each test file generates a unique prefix (e.g., test-ab12-, test-cd34-)
 * - Cleanup runs BEFORE prefix generation, so ALL test-% knowledge bases are old
 * - After cleanup, each test creates its own isolated set of knowledge bases
 * - Tests cannot interfere with each other's knowledge bases
 *
 * @param {string} namespaceId - The namespace ID for API calls
 * @param {object} headers - HTTP headers including Authorization
 * @returns {object} - { knowledgeBasesDeleted: number, filesMarkedFailed: number, errors: Array }
 */
export function cleanupPreviousTestKnowledgeBases(namespaceId, headers) {
  console.log("=== GLOBAL CLEANUP: Removing previous test knowledge bases ===");

  const result = {
    knowledgeBasesDeleted: 0,
    filesMarkedFailed: 0,
    errors: []
  };

  try {
    // Step 1: Mark any stuck files in test knowledge bases as FAILED
    // This prevents worker queue from being blocked by zombie files
    console.log("Step 1: Marking zombie files as FAILED...");
    const updateFilesQuery = `
      UPDATE file
      SET process_status = 'FILE_PROCESS_STATUS_FAILED'
      WHERE kb_uid IN (
        SELECT uid FROM knowledge_base
        WHERE id LIKE 'test-%' AND staging = false
      )
      AND process_status IN (
        'FILE_PROCESS_STATUS_NOTSTARTED',
        'FILE_PROCESS_STATUS_PROCESSING',
        'FILE_PROCESS_STATUS_CHUNKING',
        'FILE_PROCESS_STATUS_EMBEDDING'
      )
      AND delete_time IS NULL
    `;

    try {
      const updateResult = safeExecute(updateFilesQuery);
      result.filesMarkedFailed = updateResult || 0;
      if (result.filesMarkedFailed > 0) {
        console.log(`✓ Marked ${result.filesMarkedFailed} zombie files as FAILED`);
      }
    } catch (e) {
      console.warn(`Failed to mark zombie files (non-fatal): ${e}`);
      result.errors.push(`Mark files: ${e.message || e}`);
    }

    // Step 2: Get all production test knowledge bases (excluding staging/rollback)
    console.log("Step 2: Querying for test knowledge bases...");
    const testKBsQuery = `
      SELECT id, create_time
      FROM knowledge_base
      WHERE id LIKE 'test-%'
        AND staging = false
        AND delete_time IS NULL
      ORDER BY create_time ASC
    `;

    const testKBs = safeQuery(testKBsQuery);

    if (!testKBs || testKBs.length === 0) {
      console.log("✓ No previous test knowledge bases found - system is clean");
      return result;
    }

    console.log(`Found ${testKBs.length} test knowledge bases to delete`);

    // Step 3: Delete each knowledge base via API (respects business logic, cascades properly)
    for (const kb of testKBs) {
      const knowledgeBaseId = byteArrayToString(kb.id);

      try {
        const deleteRes = http.request(
          "DELETE",
          `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${namespaceId}/knowledge-bases/${knowledgeBaseId}`,
          null,
          headers
        );

        if (deleteRes.status === 204 || deleteRes.status === 200) {
          result.knowledge_basesDeleted++;
          console.log(`  ✓ Deleted: ${knowledgeBaseId}`);
        } else if (deleteRes.status === 404) {
          // Already deleted (race condition or concurrent cleanup)
          result.knowledge_basesDeleted++;
          console.log(`  ✓ Already deleted: ${knowledgeBaseId}`);
        } else {
          console.warn(`  ✗ Failed to delete ${knowledgeBaseId}: HTTP ${deleteRes.status}`);
          result.errors.push(`Delete ${knowledgeBaseId}: HTTP ${deleteRes.status}`);
        }
      } catch (e) {
        console.warn(`  ✗ Exception deleting ${knowledgeBaseId}: ${e}`);
        result.errors.push(`Delete ${knowledgeBaseId}: ${e.message || e}`);
      }
    }

    console.log(`=== CLEANUP COMPLETE: ${result.knowledge_basesDeleted}/${testKBs.length} knowledge bases deleted ===`);

    if (result.errors.length > 0) {
      console.warn(`Cleanup had ${result.errors.length} non-fatal errors`);
    }

  } catch (e) {
    console.error(`Global cleanup failed: ${e}`);
    result.errors.push(`Global error: ${e.message || e}`);
  }

  return result;
}
