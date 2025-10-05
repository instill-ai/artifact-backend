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
  // Construct the MinIO path prefix based on object type
  // Format: kb-{kbUID}/file-{fileUID}/{objectType}/
  const prefix = `kb-${kbUID}/file-${fileUID}/${objectType}/`;

  try {
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
    console.error(`Failed to count MinIO objects for ${prefix}: ${e}`);
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
    const results = constant.db.query('SELECT uid FROM embedding WHERE kb_file_uid = $1', fileUid);
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
 * @param {string} kbUID - Knowledge base (catalog) UUID
 * @param {string} fileUID - File UUID
 * @returns {number} Count of vectors (0 = no vectors, -1 = error)
 */
export function countMilvusVectors(kbUID, fileUID) {
  // Convert kbUID to Milvus collection name format: kb_{uuid_with_underscores}
  const collectionName = `kb_${kbUID.replace(/-/g, '_')}`;

  try {
    // Execute the shell script to count Milvus vectors using milvus_cli
    const result = exec.command('sh', [
      `${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/count-milvus-vectors.sh`,
      collectionName,
      fileUID
    ]);

    // Parse the output (should be a number)
    const count = parseInt(result.trim());
    return isNaN(count) ? 0 : count;
  } catch (e) {
    console.error(`Failed to count Milvus vectors for file ${fileUID} in collection ${collectionName}: ${e}`);
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
export function pollMinioObjects(kbUID, fileUID, objectType, maxWaitSeconds = 10) {
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
export function pollEmbeddings(fileUid, maxWaitSeconds = 10) {
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
export function pollMilvusVectors(kbUID, fileUID, maxWaitSeconds = 10) {
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
 * Poll all storage systems until data appears or timeout
 * Comprehensive check for MinIO, Postgres, and Milvus consistency
 *
 * @param {string} kbUID - Knowledge base UUID
 * @param {string} fileUID - File UUID
 * @param {boolean} expectConverted - Whether to check for converted files (false for text files)
 * @param {number} maxWaitSeconds - Maximum seconds to wait per storage system (default: 10)
 * @returns {object} Counts from all storage systems { converted, chunks, embeddings, vectors }
 */
export function pollAllStorageSystems(kbUID, fileUID, expectConverted = true, maxWaitSeconds = 10) {
  const results = {
    converted: 0,
    chunks: 0,
    embeddings: 0,
    vectors: 0,
  };

  // Poll MinIO for converted files (if expected)
  if (expectConverted) {
    results.converted = pollMinioObjects(kbUID, fileUID, 'converted-file', maxWaitSeconds);
  }

  // Poll MinIO for chunks (always expected)
  results.chunks = pollMinioObjects(kbUID, fileUID, 'chunk', maxWaitSeconds);

  // Poll database embeddings
  results.embeddings = pollEmbeddings(fileUID, maxWaitSeconds);

  // Poll Milvus vectors
  results.vectors = pollMilvusVectors(kbUID, fileUID, maxWaitSeconds);

  return results;
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
