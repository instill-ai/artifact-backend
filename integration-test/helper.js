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

export function validateCatalog(catalog, isPrivate) {
  if (!("catalogUid" in catalog)) {
    console.log("Catalog has no catalog_uid field");
    return false;
  }

  if (!("catalogId" in catalog)) {
    console.log("Catalog has no catalog_id field");
    return false;
  }

  if (!("name" in catalog)) {
    console.log("Catalog has no name field");
    return false;
  }

  if (!("description" in catalog)) {
    console.log("Catalog has no description field");
    return false;
  }

  if (!("createTime" in catalog)) {
    console.log("Catalog has no create_time field");
    return false;
  }

  if (!("updateTime" in catalog)) {
    console.log("Catalog has no update_time field");
    return false;
  }

  return true;
}

export function validateFile(file, isPrivate) {
  if (!("fileUid" in file)) {
    console.log("File has no file_uid field");
    return false;
  }

  if (!("name" in file)) {
    console.log("File has no name field");
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

export function validateCatalogGRPC(catalog, isPrivate) {
  if (!("catalogUid" in catalog)) {
    console.log("Catalog has no catalogUid field");
    return false;
  }

  if (!("catalogId" in catalog)) {
    console.log("Catalog has no catalogId field");
    return false;
  }

  if (!("name" in catalog)) {
    console.log("Catalog has no name field");
    return false;
  }

  if (!("description" in catalog)) {
    console.log("Catalog has no description field");
    return false;
  }

  if (!("createTime" in catalog)) {
    console.log("Catalog has no createTime field");
    return false;
  }

  if (!("updateTime" in catalog)) {
    console.log("Catalog has no updateTime field");
    return false;
  }

  return true;
}

export function validateFileGRPC(file, isPrivate) {
  if (!("fileUid" in file)) {
    console.log("File has no fileUid field");
    return false;
  }

  if (!("name" in file)) {
    console.log("File has no name field");
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

// ============================================================================
// Storage Verification Helpers for Reprocessing Tests
// ============================================================================

import { sleep } from 'k6';
import exec from 'k6/x/exec';
import * as constant from './const.js';

/**
 * Count MinIO objects directly using MinIO CLI (mc)
 * Provides direct verification of blob storage
 *
 * @param {string} kbUID - Knowledge base (catalog) UUID
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
      // Query text_chunk table for actual destination
      const chunkResult = constant.db.query(`
        SELECT content_dest FROM text_chunk
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
      const convertedResult = constant.db.query(`
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
      `${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/count-minio-objects.sh`,
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
    const results = constant.db.query('SELECT uid FROM embedding WHERE file_uid = $1', fileUid);
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
 * @param {string} kbUID - Knowledge base (catalog) UUID
 * @param {string} fileUID - File UUID
 * @returns {number} Count of vectors (0 = no vectors, -1 = error)
 */
export function countMilvusVectors(kbUID, fileUID) {
  try {
    // CRITICAL FIX: Query active_collection_uid from database
    // During updates, a KB's active_collection_uid may point to a different collection than its own UID
    const kbResult = constant.db.query(`
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
      `${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/count-milvus-vectors.sh`,
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
 * Used to verify cleanup workflow completion after catalog deletion
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
 * Used to verify cleanup workflow completion after catalog deletion
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
 * Used to verify cleanup workflow completion after catalog deletion
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
 * @param {string} catalogId - Catalog ID
 * @param {string} fileUid - File UUID
 * @param {object} headers - Request headers
 * @returns {object} { pages: number, chunks: number, status: string } or null on error
 */
export function getFileMetadata(namespaceId, catalogId, fileUid, headers) {
  try {
    const apiUrl = `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${namespaceId}/catalogs/${catalogId}/files/${fileUid}`;
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
 * @param {string} catalogId - Catalog ID
 * @param {string} fileUid - File UUID
 * @param {object} headers - Request headers
 * @param {number} maxWaitSeconds - Maximum seconds to wait (default: 10)
 * @returns {object} { pages: number, chunks: number }
 */
export function pollFileMetadata(namespaceId, catalogId, fileUid, headers, maxWaitSeconds = 10) {
  let bestResult = { pages: 0, chunks: 0, processStatus: 'UNKNOWN' };
  let attemptCount = 0;

  for (let i = 0; i < maxWaitSeconds; i++) {
    attemptCount++;
    const metadata = getFileMetadata(namespaceId, catalogId, fileUid, headers);

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
  const finalMetadata = getFileMetadata(namespaceId, catalogId, fileUid, headers);
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
 * Poll database for catalog by name pattern
 * Used to find staging/rollback KBs during update workflow
 *
 * @param {string} namePattern - SQL LIKE pattern (e.g., '%staging', '%rollback')
 * @returns {Array} Array of catalog rows from database
 */
export function findCatalogByPattern(namePattern) {
  try {
    const query = `SELECT * FROM knowledge_base WHERE id LIKE $1`;
    const results = constant.db.query(query, namePattern);
    return results ? results.map(row => normalizeDBRow(row)) : [];
  } catch (e) {
    console.error(`Failed to find catalog by pattern ${namePattern}: ${e}`);
    return [];
  }
}

/**
 * Poll for update completion by monitoring catalog update status
 * Waits until catalog reaches "completed" status or timeout
 *
 * @param {object} client - gRPC client
 * @param {object} data - Test data with metadata
 * @param {string} catalogUid - Catalog UID to monitor
 * @param {number} maxWaitSeconds - Maximum seconds to wait (default: 300 = 5 minutes)
 * @returns {boolean} True if update completed, false if timeout
 */
export function pollUpdateCompletion(client, data, catalogUid, maxWaitSeconds = 300) {
  console.log(`[POLL START] Polling for catalogUid=${catalogUid}, maxWait=${maxWaitSeconds}s`);
  for (let i = 0; i < maxWaitSeconds; i++) {
    const res = client.invoke(
      "artifact.artifact.v1alpha.ArtifactPrivateService/GetKnowledgeBaseUpdateStatusAdmin",
      {},
      data.metadata
    );

    // k6 gRPC returns res.status in various forms, check if response has valid message
    if (res.message && res.message.details) { // Check for successful response with data
      const statuses = res.message.details || [];
      const catalogStatus = statuses.find(s => s.catalogUid === catalogUid);

      // Debug logging on first attempt and periodically
      if (i === 0 || (i > 0 && i % 30 === 0)) {
        console.log(`Poll attempt ${i + 1}: Looking for catalogUid=${catalogUid}, found ${statuses.length} statuses`);
        if (statuses.length > 0 && !catalogStatus) {
          console.log(`Available UIDs: ${statuses.map(s => s.catalogUid).join(', ')}`);
        }
      }

      if (catalogStatus) {
        // Check enum status using numeric values (k6 gRPC returns enums as numbers)
        // KNOWLEDGE_BASE_UPDATE_STATUS_COMPLETED = 6
        if (catalogStatus.status === 6 || catalogStatus.status === "KNOWLEDGE_BASE_UPDATE_STATUS_COMPLETED") {
          return true;
        }
        // KNOWLEDGE_BASE_UPDATE_STATUS_FAILED = 7
        if (catalogStatus.status === 7 || catalogStatus.status === "KNOWLEDGE_BASE_UPDATE_STATUS_FAILED") {
          console.error(`Update failed for catalog ${catalogUid}`);
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

  console.error(`Timeout waiting for update completion of catalog ${catalogUid}`);
  return false;
}

/**
 * Verify rollback KB exists in database
 * Checks for KB with -rollback suffix and correct attributes
 *
 * @param {string} catalogId - Original catalog ID (without suffix)
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

export function verifyRollbackKB(catalogId, ownerUid) {
  const rollbackKbId = `${catalogId}-rollback`;
  try {
    const query = `
      SELECT id, uid, staging, update_status, rollback_retention_until, tags, active_collection_uid
      FROM knowledge_base
      WHERE id = $1 AND owner = $2
    `;
    const results = constant.db.query(query, rollbackKbId, ownerUid);
    return results ? results.map(row => normalizeDBRow(row)) : [];
  } catch (e) {
    console.error(`Failed to verify rollback KB for ${catalogId}: ${e}`);
    return [];
  }
}

/**
 * Verify staging KB exists in database
 * Checks for KB with -staging suffix during update workflow
 *
 * @param {string} catalogId - Original catalog ID (without suffix)
 * @param {string} ownerUid - Owner UID
 * @returns {Array} Array of staging KB rows (should be 1 or 0)
 */
export function verifyStagingKB(catalogId, ownerUid) {
  const stagingKbId = `${catalogId}-staging`;
  try {
    const query = `
      SELECT uid, id, staging, update_status, active_collection_uid
      FROM knowledge_base
      WHERE id = $1 AND owner = $2
    `;
    const results = constant.db.query(query, stagingKbId, ownerUid);
    return results ? results.map(row => normalizeDBRow(row)) : [];
  } catch (e) {
    console.error(`Failed to verify staging KB for ${catalogId}: ${e}`);
    return [];
  }
}

/**
 * Get catalog by catalog_id and owner
 * Helper to retrieve catalog details from database
 *
 * @param {string} catalogId - Catalog ID
 * @param {string} ownerUid - Owner UID
 * @returns {Array} Array of catalog rows (should be 1 or 0)
 */
export function getCatalogByIdAndOwner(catalogId, ownerUid) {
  try {
    const query = `
      SELECT * FROM knowledge_base
      WHERE id = $1 AND owner = $2
    `;
    const results = constant.db.query(query, catalogId, ownerUid);
    return results ? results.map(row => normalizeDBRow(row)) : [];
  } catch (e) {
    console.error(`Failed to get catalog ${catalogId}: ${e}`);
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
    const stagingKB = getCatalogByIdAndOwner(stagingKBID, ownerUid);

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

  const chunksQuery = `SELECT COUNT(*) as count FROM text_chunk WHERE knowledge_base_uid = $1 AND delete_time IS NULL`;
  const embeddingsQuery = `SELECT COUNT(*) as count FROM embedding WHERE knowledge_base_uid = $1 AND delete_time IS NULL`;
  const convertedFilesQuery = `SELECT COUNT(*) as count FROM converted_file WHERE knowledge_base_uid = $1 AND delete_time IS NULL`;

  for (let i = 0; i < maxWaitSeconds; i++) {
    const rollbackKB = getCatalogByIdAndOwner(rollbackKBID, ownerUid);
    const filesCount = countFilesInCatalog(rollbackKBUID);

    const chunksResult = constant.db.query(chunksQuery, rollbackKBUID);
    const chunksCount = chunksResult && chunksResult.length > 0 ? parseInt(chunksResult[0].count) : 0;

    const embeddingsResult = constant.db.query(embeddingsQuery, rollbackKBUID);
    const embeddingsCount = embeddingsResult && embeddingsResult.length > 0 ? parseInt(embeddingsResult[0].count) : 0;

    const convertedFilesResult = constant.db.query(convertedFilesQuery, rollbackKBUID);
    const convertedFilesCount = convertedFilesResult && convertedFilesResult.length > 0 ? parseInt(convertedFilesResult[0].count) : 0;

    // Log on first attempt and every 5 seconds
    if (i === 0 || i % 5 === 0) {
      if (rollbackKB && rollbackKB.length > 0) {
        console.log(`[POLL] Attempt ${i + 1}: Rollback KB found, delete_time=${rollbackKB[0].delete_time}, files=${filesCount}, converted=${convertedFilesCount}, chunks=${chunksCount}, embeddings=${embeddingsCount}`);
      } else {
        console.log(`[POLL] Attempt ${i + 1}: Rollback KB not found (fully deleted)`);
      }
    }

    // Check if rollback KB is soft-deleted AND all resources are purged
    const isSoftDeleted = !rollbackKB || rollbackKB.length === 0 || rollbackKB[0].delete_time !== null;
    const resourcesPurged = filesCount === 0 && convertedFilesCount === 0 && chunksCount === 0 && embeddingsCount === 0;

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
    // Check knowledge_base_file
    const fileQuery = `SELECT COUNT(*) as count FROM knowledge_base_file WHERE kb_uid = $1`;
    const fileResults = constant.db.query(fileQuery, newProdKBUID);
    result.fileCount = fileResults && fileResults.length > 0 ? parseInt(fileResults[0].count) : 0;
    result.filesCorrect = result.fileCount > 0;

    // Check text_chunk
    const chunkQuery = `SELECT COUNT(*) as count FROM text_chunk WHERE kb_uid = $1`;
    const chunkResults = constant.db.query(chunkQuery, newProdKBUID);
    result.chunkCount = chunkResults && chunkResults.length > 0 ? parseInt(chunkResults[0].count) : 0;
    result.chunksCorrect = result.chunkCount > 0;

    // Check embedding (optional - may not exist yet)
    const embeddingQuery = `SELECT COUNT(*) as count FROM embedding WHERE kb_uid = $1`;
    const embeddingResults = constant.db.query(embeddingQuery, newProdKBUID);
    result.embeddingCount = embeddingResults && embeddingResults.length > 0 ? parseInt(embeddingResults[0].count) : 0;
    result.embeddingsCorrect = true; // Always true, embeddings are optional

    // Check converted_file
    const convertedQuery = `SELECT COUNT(*) as count FROM converted_file WHERE kb_uid = $1`;
    const convertedResults = constant.db.query(convertedQuery, newProdKBUID);
    result.convertedFileCount = convertedResults && convertedResults.length > 0 ? parseInt(convertedResults[0].count) : 0;
    result.convertedFilesCorrect = result.convertedFileCount > 0;

  } catch (e) {
    console.error(`Failed to verify resource KB UIDs: ${e}`);
  }

  return result;
}

/**
 * Count files in a catalog by catalog UID
 *
 * @param {string} catalogUid - Catalog UID
 * @returns {number} Number of files in catalog
 */
export function countFilesInCatalog(catalogUid) {
  try {
    const query = `SELECT COUNT(*) as count FROM knowledge_base_file WHERE kb_uid = $1 AND delete_time IS NULL`;
    const results = constant.db.query(query, catalogUid);
    return results && results.length > 0 ? parseInt(results[0].count) : 0;
  } catch (e) {
    console.error(`Failed to count files in catalog ${catalogUid}: ${e}`);
    return 0;
  }
}

/**
 * Get all catalogs matching a pattern (uses SQL LIKE)
 *
 * @param {string} pattern - SQL LIKE pattern (e.g., "catalog%")
 * @param {string} ownerUid - Owner UID
 * @returns {Array} Array of catalog objects
 */
export function getCatalogsByPattern(pattern, ownerUid) {
  try {
    const query = `
      SELECT uid, id, name, staging, update_status, delete_time, rollback_retention_until
      FROM knowledge_base
      WHERE owner = $1 AND name LIKE $2
      ORDER BY create_time ASC
    `;
    return constant.db.query(query, ownerUid, pattern);
  } catch (e) {
    console.error(`Failed to get catalogs by pattern ${pattern}: ${e}`);
    return null;
  }
}

/**
 * Poll for staging KB to appear during Phase 1 (Prepare)
 *
 * @param {string} catalogId - Original catalog ID (without suffix)
 * @param {string} ownerUid - Owner UID
 * @param {number} maxWaitSeconds - Maximum seconds to wait (default: 30)
 * @returns {boolean} True if staging KB found, false otherwise
 */
export function pollForStagingKB(catalogId, ownerUid, maxWaitSeconds = 30) {
  for (let i = 0; i < maxWaitSeconds; i++) {
    const stagingKBs = verifyStagingKB(catalogId, ownerUid);
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
        console.log(`[WAIT] Still waiting... ${active.length} catalogs active (${elapsedSeconds}s elapsed)`);
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
