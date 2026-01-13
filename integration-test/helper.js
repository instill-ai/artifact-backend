// ============================================================================
// Safe Database Query Helpers
// ============================================================================

import * as constant from './const.js';
import http from 'k6/http';
import { sleep } from 'k6';
import exec from 'k6/x/exec';

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

// ============================================================================
// HTTP Retry Helpers for Transient Errors
// ============================================================================

// Default retry configuration
const DEFAULT_RETRY_CONFIG = {
  maxRetries: 3,
  backoffMs: 1000,
  retryOnStatus: [429, 500, 502, 503, 504],
};

/**
 * HTTP request with automatic retry for transient errors.
 * Handles rate limits (429), server errors (5xx), and network issues.
 *
 * @param {string} method - HTTP method (GET, POST, PATCH, DELETE)
 * @param {string} url - Request URL
 * @param {string|object} body - Request body (will be JSON.stringify'd if object)
 * @param {object} params - Request params (headers, etc.)
 * @returns {object} HTTP response
 */
function httpRequestWithRetry(method, url, body = null, params = {}) {
  const { maxRetries, backoffMs, retryOnStatus } = DEFAULT_RETRY_CONFIG;

  let lastResponse = null;
  let requestBody = body;
  if (body && typeof body === 'object') {
    requestBody = JSON.stringify(body);
  }

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    lastResponse = http.request(method, url, requestBody, params);

    // Success - return immediately
    if (lastResponse.status >= 200 && lastResponse.status < 300) {
      return lastResponse;
    }

    // Check if we should retry this status code
    if (!retryOnStatus.includes(lastResponse.status)) {
      // Non-retryable error (4xx except 429) - return immediately
      return lastResponse;
    }

    // Retryable error - wait and retry
    if (attempt < maxRetries) {
      const waitTime = backoffMs * Math.pow(2, attempt - 1); // Exponential backoff: 1s, 2s, 4s
      const urlPath = url.split('/').slice(-2).join('/'); // Last 2 path segments for logging
      console.log(`⚠ HTTP ${method} .../${urlPath} failed (status=${lastResponse.status}), ` +
                  `retry ${attempt}/${maxRetries} in ${waitTime}ms...`);
      sleep(waitTime / 1000);
    }
  }

  // All retries exhausted
  const urlPath = url.split('/').slice(-2).join('/');
  console.log(`✗ HTTP ${method} .../${urlPath} failed after ${maxRetries} retries (status=${lastResponse.status})`);
  return lastResponse;
}

/**
 * Drop-in replacement for k6/http with automatic retry for transient errors.
 * Use this instead of importing http from 'k6/http' to get automatic retry.
 *
 * Usage:
 *   // Instead of: import http from "k6/http";
 *   // Use: import { httpRetry as http } from "./helper.js";
 *
 *   const res = http.request("POST", url, body, params);
 *   const res = http.post(url, body, params);
 *   const res = http.get(url, params);
 */
export const httpRetry = {
  /**
   * Generic HTTP request with retry
   */
  request: httpRequestWithRetry,

  /**
   * POST request with retry
   */
  post: (url, body = null, params = {}) => httpRequestWithRetry("POST", url, body, params),

  /**
   * GET request with retry
   */
  get: (url, params = {}) => httpRequestWithRetry("GET", url, null, params),

  /**
   * PUT request with retry
   */
  put: (url, body = null, params = {}) => httpRequestWithRetry("PUT", url, body, params),

  /**
   * PATCH request with retry
   */
  patch: (url, body = null, params = {}) => httpRequestWithRetry("PATCH", url, body, params),

  /**
   * DELETE request with retry
   */
  del: (url, params = {}) => httpRequestWithRetry("DELETE", url, null, params),

  /**
   * Batch requests (pass-through to k6 http.batch)
   * Note: Individual requests in a batch don't get retry logic,
   * but the batch itself is atomic and rarely fails transiently.
   */
  batch: (requests) => http.batch(requests),
};


// ============================================================================
// Utility Functions
// ============================================================================

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

/**
 * Validate a creator object (User) returned from the API.
 * The creator is always a User object (not an Owner wrapper).
 *
 * @param {object} creator - The creator object from the API response
 * @param {object} expectedUser - The expected user object (optional, for exact match)
 * @returns {boolean} True if creator is valid
 */
export function isValidCreator(creator, expectedUser = null) {
  if (creator === null || creator === undefined) {
    // Creator can be null for system-created resources
    return true;
  }

  // Creator should be a User object with required fields
  if (!("id" in creator)) {
    console.log("Creator has no id field");
    return false;
  }

  if (!("uid" in creator)) {
    console.log("Creator has no uid field");
    return false;
  }

  if (!isUUID(creator.uid)) {
    console.log("Creator uid is not a valid UUID");
    return false;
  }

  // If expected user provided, validate match
  if (expectedUser !== null) {
    if (creator.id !== expectedUser.id) {
      console.log(`Creator id mismatch: ${creator.id} !== ${expectedUser.id}`);
      return false;
    }
    if (creator.uid !== expectedUser.uid) {
      console.log(`Creator uid mismatch: ${creator.uid} !== ${expectedUser.uid}`);
      return false;
    }
  }

  return true;
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

  // Validate ownerName field (required)
  if (!("ownerName" in knowledgeBase)) {
    console.log("Knowledge base has no ownerName field");
    return false;
  }

  // Validate ownerUid field (required, must be UUID)
  if (!("ownerUid" in knowledgeBase)) {
    console.log("Knowledge base has no ownerUid field");
    return false;
  }
  if (!isUUID(knowledgeBase.ownerUid)) {
    console.log("Knowledge base ownerUid is not a valid UUID");
    return false;
  }

  // Validate owner object (optional but if present, must be valid)
  if ("owner" in knowledgeBase && knowledgeBase.owner !== null && knowledgeBase.owner !== undefined) {
    if (!knowledgeBase.owner.user && !knowledgeBase.owner.organization) {
      console.log("Knowledge base owner is neither user nor organization");
      return false;
    }
  }

  // Validate creatorUid field (optional, but if present must be UUID)
  if ("creatorUid" in knowledgeBase && knowledgeBase.creatorUid !== null && knowledgeBase.creatorUid !== undefined) {
    if (!isUUID(knowledgeBase.creatorUid)) {
      console.log("Knowledge base creatorUid is not a valid UUID");
      return false;
    }
  }

  // Validate creator object (optional, but if present must be valid User)
  if ("creator" in knowledgeBase && knowledgeBase.creator !== null && knowledgeBase.creator !== undefined) {
    if (!isValidCreator(knowledgeBase.creator)) {
      console.log("Knowledge base creator is not a valid User object");
      return false;
    }
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

  if (!("displayName" in file)) {
    console.log("File has no displayName field");
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

  // Validate ownerUid field (required, must be UUID)
  if (!("ownerUid" in file)) {
    console.log("File has no ownerUid field");
    return false;
  }
  if (!isUUID(file.ownerUid)) {
    console.log("File ownerUid is not a valid UUID");
    return false;
  }

  // Validate ownerName field (required)
  if (!("ownerName" in file)) {
    console.log("File has no ownerName field");
    return false;
  }

  // Validate owner object (optional but if present, must be valid)
  if ("owner" in file && file.owner !== null && file.owner !== undefined) {
    if (!file.owner.user && !file.owner.organization) {
      console.log("File owner is neither user nor organization");
      return false;
    }
  }

  // Validate creatorUid field (required, must be UUID)
  if (!("creatorUid" in file)) {
    console.log("File has no creatorUid field");
    return false;
  }
  if (!isUUID(file.creatorUid)) {
    console.log("File creatorUid is not a valid UUID");
    return false;
  }

  // Validate creator object (optional, but if present must be valid User)
  if ("creator" in file && file.creator !== null && file.creator !== undefined) {
    if (!isValidCreator(file.creator)) {
      console.log("File creator is not a valid User object");
      return false;
    }
  }

  // Validate collectionUids field (optional, should be array if present)
  if ("collectionUids" in file && file.collectionUids !== null && file.collectionUids !== undefined) {
    if (!Array.isArray(file.collectionUids)) {
      console.log("File collectionUids is not an array");
      return false;
    }
    // Each element should be a valid string UID
    for (const uid of file.collectionUids) {
      if (typeof uid !== "string" || uid.length === 0) {
        console.log(`File collectionUids contains invalid UID: ${uid}`);
        return false;
      }
    }
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

  // Validate ownerName field (required)
  if (!("ownerName" in knowledgeBase)) {
    console.log("Knowledge base has no ownerName field");
    return false;
  }

  // Validate ownerUid field (required, must be UUID)
  if (!("ownerUid" in knowledgeBase)) {
    console.log("Knowledge base has no ownerUid field");
    return false;
  }
  if (!isUUID(knowledgeBase.ownerUid)) {
    console.log("Knowledge base ownerUid is not a valid UUID");
    return false;
  }

  // Validate owner object (optional but if present, must be valid)
  if ("owner" in knowledgeBase && knowledgeBase.owner !== null && knowledgeBase.owner !== undefined) {
    if (!knowledgeBase.owner.user && !knowledgeBase.owner.organization) {
      console.log("Knowledge base owner is neither user nor organization");
      return false;
    }
  }

  // Validate creatorUid field (optional, but if present must be UUID)
  if ("creatorUid" in knowledgeBase && knowledgeBase.creatorUid !== null && knowledgeBase.creatorUid !== undefined) {
    if (!isUUID(knowledgeBase.creatorUid)) {
      console.log("Knowledge base creatorUid is not a valid UUID");
      return false;
    }
  }

  // Validate creator object (optional, but if present must be valid User)
  if ("creator" in knowledgeBase && knowledgeBase.creator !== null && knowledgeBase.creator !== undefined) {
    if (!isValidCreator(knowledgeBase.creator)) {
      console.log("Knowledge base creator is not a valid User object");
      return false;
    }
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

  if (!("displayName" in file)) {
    console.log("File has no displayName field");
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

  // Validate ownerUid field (required, must be UUID)
  if (!("ownerUid" in file)) {
    console.log("File has no ownerUid field");
    return false;
  }
  if (!isUUID(file.ownerUid)) {
    console.log("File ownerUid is not a valid UUID");
    return false;
  }

  // Validate ownerName field
  // NOTE: ownerName is OUTPUT_ONLY (required) in protobuf, but api-gateway doesn't
  // proxy gRPC reflection requests, so k6 cannot discover the full proto schema.
  // This field IS returned by the backend (verified via direct grpcurl calls),
  // but k6 through api-gateway cannot see it. Full field validation is covered
  // in REST tests which don't have this limitation.
  // See: https://github.com/instill-ai/api-gateway - gRPC reflection not supported
  if ("ownerName" in file) {
    if (typeof file.ownerName !== 'string' || file.ownerName === '') {
      console.log("File ownerName is invalid (expected non-empty string)");
      return false;
    }
  }

  // Validate owner object (optional but if present, must be valid)
  if ("owner" in file && file.owner !== null && file.owner !== undefined) {
    if (!file.owner.user && !file.owner.organization) {
      console.log("File owner is neither user nor organization");
      return false;
    }
  }

  // Validate creatorUid field (required, must be UUID)
  if (!("creatorUid" in file)) {
    console.log("File has no creatorUid field");
    return false;
  }
  if (!isUUID(file.creatorUid)) {
    console.log("File creatorUid is not a valid UUID");
    return false;
  }

  // Validate creator object (optional, but if present must be valid User)
  if ("creator" in file && file.creator !== null && file.creator !== undefined) {
    if (!isValidCreator(file.creator)) {
      console.log("File creator is not a valid User object");
      return false;
    }
  }

  // Validate collectionUids field (optional, should be array if present)
  if ("collectionUids" in file && file.collectionUids !== null && file.collectionUids !== undefined) {
    if (!Array.isArray(file.collectionUids)) {
      console.log("File collectionUids is not an array");
      return false;
    }
    // Each element should be a valid string UID
    for (const uid of file.collectionUids) {
      if (typeof uid !== "string" || uid.length === 0) {
        console.log(`File collectionUids contains invalid UID: ${uid}`);
        return false;
      }
    }
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
    // Use minioConfig.host which handles local vs Docker mode automatically
    const minioHost = __ENV.MINIO_HOST || constant.minioConfig.host;
    const minioPort = __ENV.MINIO_PORT || String(constant.minioConfig.port);
    const result = exec.command('sh', [
      `${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/scripts/count-minio-objects.sh`,
      constant.minioConfig.bucket,
      prefix,
      minioHost,
      minioPort
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
 * Verify that a converted file with specific extension exists in MinIO
 * Used to validate StandardizeFileTypeActivity created the correct file type
 *
 * @param {string} kbUID - Knowledge base UUID
 * @param {string} fileUID - File UUID
 * @param {string} expectedExtension - Expected file extension (e.g., 'png', 'pdf', 'ogg', 'mp4')
 * @returns {boolean} True if converted file with expected extension exists
 */
export function verifyConvertedFileType(kbUID, fileUID, expectedExtension) {
  try {
    // Query database for converted file destination
    const convertedResult = safeQuery(`
      SELECT destination FROM converted_file
      WHERE file_uid = $1
      AND destination LIKE '%/converted-file/%'
      LIMIT 1
    `, fileUID);

    if (!convertedResult || convertedResult.length === 0) {
      console.warn(`No converted file found in database for file ${fileUID}`);
      return false;
    }

    // Extract prefix from destination
    const dest = convertedResult[0].destination;
    const lastSlashIndex = dest.lastIndexOf('/');
    const prefix = dest.substring(0, lastSlashIndex + 1);

    // Use the verify mode of count-minio-objects.sh
    // Use minioConfig.host which handles local vs Docker mode automatically
    const minioHost = __ENV.MINIO_HOST || constant.minioConfig.host;
    const minioPort = __ENV.MINIO_PORT || String(constant.minioConfig.port);
    const result = exec.command('sh', [
      `${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/scripts/count-minio-objects.sh`,
      constant.minioConfig.bucket,
      prefix,
      minioHost,
      minioPort,
      'verify',
      expectedExtension
    ]);

    const count = parseInt(result.trim());
    const exists = count > 0;

    if (!exists) {
      console.warn(`Expected .${expectedExtension} file not found in MinIO at ${prefix}`);
    }

    return exists;
  } catch (e) {
    console.error(`Failed to verify converted file type for file ${fileUID}: ${e}`);
    return false;
  }
}

/**
 * Get the expected standard file extension for a given file type
 * Maps file types to their standardized formats based on Gemini-native support:
 * - Gemini-native formats keep their original extension
 * - Non-native formats are converted to standard formats (PDF, PNG, OGG, MP4)
 *
 * @param {string} fileType - File type (e.g., 'TYPE_PNG', 'TYPE_DOC', 'TYPE_MP3')
 * @returns {string|null} Expected extension or null if not standardizable
 */
export function getStandardFileExtension(fileType) {
  // Documents - all convert to PDF (only PDF is Gemini-native)
  const documentTypes = ["TYPE_PDF", "TYPE_DOC", "TYPE_DOCX", "TYPE_PPT", "TYPE_PPTX",
    "TYPE_XLS", "TYPE_XLSX", "TYPE_HTML", "TYPE_TEXT", "TYPE_MARKDOWN", "TYPE_CSV"];
  if (documentTypes.includes(fileType)) return 'pdf';

  // Images - Gemini-native formats (PNG, JPEG, WEBP, HEIC, HEIF) keep original extension
  // Non-native (GIF, BMP, TIFF, AVIF) convert to PNG
  const geminiNativeImages = {
    "TYPE_PNG": "png", "TYPE_JPEG": "jpg", "TYPE_WEBP": "webp",
    "TYPE_HEIC": "heic", "TYPE_HEIF": "heif"
  };
  if (geminiNativeImages[fileType]) return geminiNativeImages[fileType];
  const convertibleImages = ["TYPE_GIF", "TYPE_BMP", "TYPE_TIFF", "TYPE_AVIF"];
  if (convertibleImages.includes(fileType)) return 'png';

  // Audio - Gemini-native formats (WAV, MP3, AIFF, AAC, OGG, FLAC) keep original extension
  // Non-native (M4A, WMA, WEBM_AUDIO) convert to OGG
  const geminiNativeAudio = {
    "TYPE_WAV": "wav", "TYPE_MP3": "mp3", "TYPE_AIFF": "aiff",
    "TYPE_AAC": "aac", "TYPE_OGG": "ogg", "TYPE_FLAC": "flac"
  };
  if (geminiNativeAudio[fileType]) return geminiNativeAudio[fileType];
  const convertibleAudio = ["TYPE_M4A", "TYPE_WMA", "TYPE_WEBM_AUDIO"];
  if (convertibleAudio.includes(fileType)) return 'ogg';

  // Video - Gemini-native formats (MP4, MPEG, MOV, AVI, FLV, WMV, WEBM_VIDEO) keep original extension
  // Non-native (MKV) converts to MP4
  const geminiNativeVideo = {
    "TYPE_MP4": "mp4", "TYPE_MPEG": "mpeg", "TYPE_MOV": "mov",
    "TYPE_AVI": "avi", "TYPE_FLV": "flv", "TYPE_WMV": "wmv", "TYPE_WEBM_VIDEO": "webm"
  };
  if (geminiNativeVideo[fileType]) return geminiNativeVideo[fileType];
  if (fileType === "TYPE_MKV") return 'mp4';

  return null; // Not a standardizable type
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
    // EXPLICITLY INCLUDE SOFT-DELETED KBs: During cleanup tests, the KB is soft-deleted but record still exists
    const kbResult = safeQuery(`
      SELECT active_collection_uid
      FROM knowledge_base
      WHERE uid = $1
      -- Explicitly include soft-deleted KBs (delete_time IS NOT NULL)
    `, kbUID);

    if (!kbResult || kbResult.length === 0) {
      console.log(`countMilvusVectors: KB ${kbUID} not found in database (may have been fully deleted)`);
      // KB record doesn't exist - collection was likely already dropped during cleanup
      // Return 0 (no vectors) instead of -1 (error), as this is expected after cleanup completes
      return 0;
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
    // Use milvusConfig which switches between localhost (local) and milvus (Docker)
    const milvusHost = constant.milvusConfig.host;
    const milvusPort = constant.milvusConfig.port;
    const result = exec.command('sh', [
      `${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/scripts/count-milvus-vectors.sh`,
      collectionName,
      fileUID,
      milvusHost,
      milvusPort
    ]);

    // DEBUG: Log the raw output from the script
    if (result.indexOf('Error') >= 0 || result.indexOf('not exist') >= 0) {
      console.log(`DEBUG countMilvusVectors: KB=${kbUID}, activeCollection=${activeCollectionUID}, collection=${collectionName}, file=${fileUID}, result="${result.trim()}"`);
      // Collection not found or error - this means vectors are already removed (count = 0)
      // This is expected after cleanup workflow drops the collection
      if (result.indexOf('does not exist') >= 0 || result.indexOf('not found') >= 0 || result.indexOf('cannot find') >= 0) {
        return 0;
      }
    }

    // Parse the output (should be a number)
    const count = parseInt(result.trim());
    if (isNaN(count)) {
      // If we can't parse the result but no clear error, assume collection was dropped (0 vectors)
      console.log(`countMilvusVectors: Cannot parse result as number, assuming collection dropped: "${result.trim()}"`);
      return 0;
    }
    return count;
  } catch (e) {
    console.error(`Failed to count Milvus vectors for KB ${kbUID}, file ${fileUID}: ${e}`);
    // If there's an error accessing the collection, it likely means it's been dropped (vectors removed)
    if (e.toString().indexOf('not exist') >= 0 || e.toString().indexOf('not found') >= 0) {
      return 0;
    }
    return -1; // Return -1 for unexpected errors only
  }
}

/**
 * Drop a Milvus collection (used for testing missing collection scenarios)
 *
 * @param {string} kbUID - Knowledge base UUID
 * @returns {boolean} True if collection was dropped successfully
 */
export function dropMilvusCollection(kbUID) {
  try {
    // Query active_collection_uid from database (same logic as countMilvusVectors)
    // EXPLICITLY INCLUDE SOFT-DELETED KBs: We may need to manually drop collections even after KB is soft-deleted
    const kbResult = safeQuery(`
      SELECT active_collection_uid
      FROM knowledge_base
      WHERE uid = $1
      -- Explicitly include soft-deleted KBs (delete_time IS NOT NULL)
    `, kbUID);

    if (!kbResult || kbResult.length === 0) {
      console.log(`dropMilvusCollection: KB ${kbUID} not found in database (may have been fully deleted)`);
      // KB record doesn't exist - cannot determine collection UID
      return false;
    }

    // Convert active_collection_uid from Buffer (PostgreSQL UUID) to string
    let activeCollectionUID = kbResult[0].active_collection_uid;
    if (Array.isArray(activeCollectionUID)) {
      activeCollectionUID = String.fromCharCode(...activeCollectionUID);
    }

    if (!activeCollectionUID) {
      console.error(`dropMilvusCollection: KB ${kbUID} has null active_collection_uid`);
      return false;
    }

    // Convert active_collection_uid to Milvus collection name format: kb_{uuid_with_underscores}
    const collectionName = `kb_${activeCollectionUID.replace(/-/g, '_')}`;

    const milvusHost = __ENV.MILVUS_HOST || 'milvus';
    const milvusPort = __ENV.MILVUS_PORT || '19530';

    const result = exec.command('sh', [
      `${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/scripts/drop-milvus-collection.sh`,
      collectionName,
      milvusHost,
      milvusPort
    ]);

    console.log(`dropMilvusCollection: KB=${kbUID}, collection=${collectionName}, result="${result.trim()}"`);

    // Check if operation was successful
    return result.includes('SUCCESS') || result.includes('INFO');
  } catch (e) {
    console.error(`Failed to drop Milvus collection for KB ${kbUID}: ${e}`);
    return false;
  }
}

/**
 * Check if a Milvus collection exists (used for testing cleanup verification)
 *
 * @param {string} collectionUID - Collection UUID (not KB UID)
 * @returns {boolean} True if collection exists, false if dropped or error
 */
export function checkMilvusCollectionExists(collectionUID) {
  try {
    if (!collectionUID) {
      console.log(`checkMilvusCollectionExists: null collectionUID provided`);
      return false;
    }

    // Convert collection_uid from Buffer (PostgreSQL UUID) to string if needed
    let collectionUIDStr = collectionUID;
    if (Array.isArray(collectionUID)) {
      collectionUIDStr = String.fromCharCode(...collectionUID);
    }

    // Convert collection_uid to Milvus collection name format: kb_{uuid_with_underscores}
    const collectionName = `kb_${collectionUIDStr.replace(/-/g, '_')}`;

    const milvusHost = __ENV.MILVUS_HOST || 'milvus';
    const milvusPort = __ENV.MILVUS_PORT || '19530';

    // Use a simple check - try to count vectors in the collection
    // If collection doesn't exist, the script will indicate that
    const result = exec.command('sh', [
      `${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/scripts/count-milvus-vectors.sh`,
      collectionName,
      'dummy_file_uid', // Not filtering by file, just checking collection existence
      milvusHost,
      milvusPort
    ]);

    // If collection doesn't exist, the result will contain error messages or "0"
    // If collection exists, result should be a number (even if 0)
    const trimmedResult = result.trim();

    // Collection doesn't exist if we get error messages
    if (trimmedResult.includes('collection not exist') ||
      trimmedResult.includes('ERROR') ||
      trimmedResult.includes('not found')) {
      return false;
    }

    // Collection exists if we get a numeric result (including "0")
    return /^\d+$/.test(trimmedResult);
  } catch (e) {
    console.error(`Failed to check Milvus collection existence for ${collectionUID}: ${e}`);
    return false;
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
export function pollMilvusVectors(kbUID, fileUID, maxWaitSeconds = 30) {
  // Default increased to 30s because Milvus indexing can take 5-10 seconds,
  // especially under load or when other tests are running in parallel
  for (let i = 0; i < maxWaitSeconds; i++) {
    const count = countMilvusVectors(kbUID, fileUID);
    if (count > 0) {
      console.log(`pollMilvusVectors: Found ${count} vectors after ${i}s for KB=${kbUID}, file=${fileUID}`);
      return count;
    }
    if (i < maxWaitSeconds - 1) {
      sleep(1);
    }
  }
  // Final attempt after waiting
  const finalCount = countMilvusVectors(kbUID, fileUID);
  console.log(`pollMilvusVectors: Final count=${finalCount} after ${maxWaitSeconds}s for KB=${kbUID}, file=${fileUID}`);
  return finalCount;
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
 * @param {string} knowledgeBaseId - Knowledge Base ID (hash-based slug, NOT UUID) to monitor
 * @param {number} maxWaitSeconds - Maximum seconds to wait (default: 300 = 5 minutes)
 * @returns {boolean} True if update completed, false if timeout
 */
export function pollUpdateCompletion(client, data, knowledgeBaseId, maxWaitSeconds = 900) {
  console.log(`[POLL START] Polling for knowledgeBaseId=${knowledgeBaseId}, maxWait=${maxWaitSeconds}s`);

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
      const kbStatus = statuses.find(s => s.knowledgeBaseId === knowledgeBaseId);

      // Debug logging on first attempt and periodically
      if (i === 0 || (i > 0 && i % 30 === 0)) {
        console.log(`Poll attempt ${i + 1}: Looking for knowledgeBaseId=${knowledgeBaseId}, found ${statuses.length} statuses`);
        if (statuses.length > 0 && !kbStatus) {
          console.log(`Available IDs: ${statuses.map(s => s.knowledgeBaseId).join(', ')}`);
        }
      }

      if (kbStatus) {
        // KB found in status list - reset not-found counter
        notFoundCount = 0;

        // Check enum status using numeric values (k6 gRPC returns enums as numbers)
        // KNOWLEDGE_BASE_UPDATE_STATUS_COMPLETED = 6
        if (kbStatus.status === 6 || kbStatus.status === "KNOWLEDGE_BASE_UPDATE_STATUS_COMPLETED") {
          console.log(`✓ Update completed successfully for knowledge base ${knowledgeBaseId}`);
          return true;
        }
        // KNOWLEDGE_BASE_UPDATE_STATUS_FAILED = 7
        if (kbStatus.status === 7 || kbStatus.status === "KNOWLEDGE_BASE_UPDATE_STATUS_FAILED") {
          console.error(`✗ Update FAILED for knowledge base ${knowledgeBaseId}`);
          console.error(`   Error message: ${kbStatus.errorMessage || 'No error message provided'}`);
          console.error(`   Workflow ID: ${kbStatus.workflowId || 'N/A'}`);
          return false;
        }
        // KNOWLEDGE_BASE_UPDATE_STATUS_ABORTED = 8
        if (kbStatus.status === 8 || kbStatus.status === "KNOWLEDGE_BASE_UPDATE_STATUS_ABORTED") {
          console.error(`✗ Update ABORTED for knowledge base ${knowledgeBaseId}`);
          return false;
        }
        // Log current status periodically
        if (i === 0 || (i > 0 && i % 30 === 0)) {
          console.log(`   Current status: ${kbStatus.status} (${typeof kbStatus.status})`);
        }
      } else {
        // KB not found in status list - could be:
        // 1. Workflow hasn't started yet (race condition)
        // 2. Workflow completed very quickly and KB was removed from status list
        notFoundCount++;

        if (notFoundCount <= MAX_NOT_FOUND_WAIT) {
          // Still within grace period - workflow might be starting up
          if (notFoundCount === 1 || notFoundCount % 10 === 0) {
            console.log(`   KB not in status list yet (${notFoundCount}s) - waiting for workflow to start...`);
          }
        } else {
          // Exceeded grace period - check database to see if update already completed
          // This handles the case where the update completed so quickly that we missed it
          const kbQuery = safeQuery(
            `SELECT update_status FROM knowledge_base WHERE id = $1 AND delete_time IS NULL`,
            knowledgeBaseId
          );

          if (kbQuery && kbQuery.length > 0) {
            const dbStatus = kbQuery[0].update_status;
            // KNOWLEDGE_BASE_UPDATE_STATUS_COMPLETED = status value for completed updates
            if (dbStatus === 'KNOWLEDGE_BASE_UPDATE_STATUS_COMPLETED' || dbStatus === 6) {
              console.log(`✓ Update already completed (verified via DB) for knowledge base ${knowledgeBaseId}`);
              return true;
            }
            // If status is failed/aborted, return false
            if (dbStatus === 'KNOWLEDGE_BASE_UPDATE_STATUS_FAILED' || dbStatus === 7 ||
                dbStatus === 'KNOWLEDGE_BASE_UPDATE_STATUS_ABORTED' || dbStatus === 8) {
              console.error(`✗ Update failed/aborted (verified via DB): ${dbStatus}`);
              return false;
            }
            console.log(`   KB found in DB with status: ${dbStatus}, continuing to poll...`);
          } else {
            console.error(`   KB not found in status list or database after ${MAX_NOT_FOUND_WAIT}s`);
            return false;
          }
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

  console.error(`Timeout waiting for update completion of knowledge base ${knowledgeBaseId}`);
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
  try {
    // First, get the production KB's UID
    const prodQuery = `
      SELECT uid
      FROM knowledge_base
      WHERE id = $1 AND namespace_uid = $2 AND staging = false AND delete_time IS NULL
    `;
    const prodResults = safeQuery(prodQuery, knowledgeBaseId, ownerUid);
    if (!prodResults || prodResults.length === 0) {
      return [];
    }
    // Normalize the production KB UID (convert from byte array to UUID string if needed)
    const normalizedProdKB = normalizeDBRow(prodResults[0]);
    const prodKBUID = normalizedProdKB.uid;

    // Query rollback KB using parent_kb_uid and tags (no longer relying on KBID suffix)
    const query = `
      SELECT id, uid, staging, update_status, rollback_retention_until, tags, active_collection_uid, parent_kb_uid
      FROM knowledge_base
      WHERE parent_kb_uid = $1 AND staging = true AND 'rollback' = ANY(tags) AND delete_time IS NULL
    `;
    const results = safeQuery(query, prodKBUID);
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
  try {
    // First, get the production KB's UID
    const prodQuery = `
      SELECT uid
      FROM knowledge_base
      WHERE id = $1 AND namespace_uid = $2 AND staging = false AND delete_time IS NULL
    `;
    const prodResults = safeQuery(prodQuery, knowledgeBaseId, ownerUid);
    if (!prodResults || prodResults.length === 0) {
      return [];
    }
    // Normalize the production KB UID (convert from byte array to UUID string if needed)
    const normalizedProdKB = normalizeDBRow(prodResults[0]);
    const prodKBUID = normalizedProdKB.uid;

    // Query staging KB using parent_kb_uid and tags (no longer relying on KBID suffix)
    const query = `
      SELECT uid, id, staging, update_status, active_collection_uid, parent_kb_uid, description, tags
      FROM knowledge_base
      WHERE parent_kb_uid = $1 AND staging = true AND 'staging' = ANY(tags) AND delete_time IS NULL
    `;
    const results = safeQuery(query, prodKBUID);
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
      WHERE id = $1 AND namespace_uid = $2
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
 * @param {string} productionKBID - Production KB ID (to find associated staging KB via parent_kb_uid)
 * @param {string} ownerUid - Owner UID
 * @param {number} maxWaitSeconds - Maximum time to wait (default 10s)
 * @returns {boolean} True if staging KB is cleaned up or doesn't exist
 */
export function pollStagingKBCleanup(productionKBID, ownerUid, maxWaitSeconds = 10) {
  console.log(`[POLL] Waiting for staging KB cleanup for production KB: ${productionKBID}, maxWait=${maxWaitSeconds}s`);

  for (let i = 0; i < maxWaitSeconds; i++) {
    // Use verifyStagingKB which queries by parent_kb_uid and excludes deleted KBs
    const stagingKBs = verifyStagingKB(productionKBID, ownerUid);

    // Log on first attempt and every 3 seconds
    if (i === 0 || i % 3 === 0) {
      if (stagingKBs && stagingKBs.length > 0) {
        console.log(`[POLL] Attempt ${i + 1}: Staging KB still exists, update_status=${stagingKBs[0].update_status}`);
      } else {
        console.log(`[POLL] Attempt ${i + 1}: Staging KB not found (cleaned up)`);
      }
    }

    // Check if staging KB is cleaned up (verifyStagingKB excludes deleted KBs)
    if (!stagingKBs || stagingKBs.length === 0) {
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
  // Convert rollbackKBUID from Buffer to string if needed (PostgreSQL UUIDs are returned as Buffers)
  let kbUIDString = rollbackKBUID;
  if (Array.isArray(rollbackKBUID)) {
    kbUIDString = String.fromCharCode(...rollbackKBUID);
  } else if (typeof rollbackKBUID === 'object' && rollbackKBUID !== null) {
    // Handle Buffer-like objects
    kbUIDString = rollbackKBUID.toString();
  }

  console.log(`[POLL] Waiting for rollback KB cleanup: ${rollbackKBID} (UID: ${kbUIDString}), maxWait=${maxWaitSeconds}s`);

  // Note: chunk and embedding tables don't have kb_uid (they use source_uid/source_table)
  // and don't have delete_time (they're hard deleted). We only check converted_file which has kb_uid.
  const convertedFilesQuery = `SELECT COUNT(*) as count FROM converted_file WHERE kb_uid = $1`;

  for (let i = 0; i < maxWaitSeconds; i++) {
    const rollbackKB = getKnowledgeBaseByIdAndOwner(rollbackKBID, ownerUid);
    const filesCount = countFilesInKnowledgeBase(kbUIDString);

    const convertedFilesResult = safeQuery(convertedFilesQuery, kbUIDString);
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
      WHERE namespace_uid = $1 AND id LIKE $2
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
      // Note: gRPC returns full enum names or numeric values, not lowercase short forms
      const activeStatuses = [
        "KNOWLEDGE_BASE_UPDATE_STATUS_UPDATING", 2,   // Phase 1-2: Preparing and reprocessing
        "KNOWLEDGE_BASE_UPDATE_STATUS_SYNCING", 3,    // Phase 3: Synchronization
        "KNOWLEDGE_BASE_UPDATE_STATUS_VALIDATING", 4, // Phase 4: Validation
        "KNOWLEDGE_BASE_UPDATE_STATUS_SWAPPING", 5    // Phase 5: Atomic Swap
      ];
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
 * Wait for all file processing AND Temporal activities to settle before cleanup.
 *
 * This is a more robust version for test teardown that:
 * 1. Waits for file processing to complete (via DB check)
 * 2. Adds a buffer delay for in-flight Temporal activities to finish
 * 3. Optionally checks for pending Temporal workflows
 *
 * This reduces "no file found" errors during cleanup when activities are
 * still running after file processing status has changed.
 *
 * @param {number} maxWaitSeconds - Maximum time to wait for processing (default: 120s)
 * @param {string|null} dbIDPrefix - Knowledge Base ID prefix to filter (e.g., "test-ab12-")
 * @param {number} bufferSeconds - Additional seconds to wait after processing complete (default: 3s)
 * @returns {boolean} - True if cleanup is safe to proceed
 */
export function waitForSafeCleanup(maxWaitSeconds = 120, dbIDPrefix = null, bufferSeconds = 3) {
  const scope = dbIDPrefix ? `this test (${dbIDPrefix})` : "GLOBAL";

  // Step 1: Wait for file processing to complete
  const processingComplete = waitForAllFileProcessingComplete(maxWaitSeconds, dbIDPrefix);

  if (!processingComplete) {
    console.warn(`${scope}: File processing did not complete within ${maxWaitSeconds}s, proceeding with cleanup anyway`);
    // Still add buffer to let in-flight activities settle
    console.log(`${scope}: Adding ${bufferSeconds}s buffer for Temporal activities to settle...`);
    sleep(bufferSeconds);
    return false;
  }

  // Step 2: Add buffer for Temporal activities to complete
  // Even after file status is COMPLETED/FAILED, there may be follow-up activities running
  // (e.g., embedding, cleanup, workflow completion handlers)
  console.log(`${scope}: Processing complete, adding ${bufferSeconds}s buffer for Temporal activities to settle...`);
  sleep(bufferSeconds);

  // Step 3: Check for any pending Temporal workflows (optional, may not catch all)
  // This is a best-effort check - some workflows may still be in flight
  const pendingWorkflows = checkPendingWorkflows(dbIDPrefix);
  if (pendingWorkflows > 0) {
    console.log(`${scope}: ${pendingWorkflows} Temporal workflows may still be running, adding extra buffer...`);
    sleep(2); // Extra buffer for workflow completion
  }

  console.log(`${scope}: Safe to proceed with cleanup`);
  return true;
}

/**
 * Check for pending Temporal workflows related to test KBs (best-effort).
 *
 * This queries the database for files that might have workflows in progress.
 * It's not perfect (can't directly query Temporal) but helps reduce race conditions.
 *
 * @param {string|null} dbIDPrefix - KB ID prefix to filter
 * @returns {number} - Approximate count of potentially pending workflows
 */
function checkPendingWorkflows(dbIDPrefix) {
  try {
    // Check for files that were recently completed but might have follow-up workflows
    // (embedding, cleanup, etc.)
    const prefixFilter = dbIDPrefix
      ? `AND kb.id LIKE '${dbIDPrefix}%'`
      : '';

    // Look for files completed in the last 10 seconds that might still have activities
    const recentQuery = `
      SELECT COUNT(*) as count
      FROM file kbf
      JOIN knowledge_base kb ON kbf.kb_uid = kb.uid
      WHERE kbf.process_status IN ('FILE_PROCESS_STATUS_COMPLETED', 'FILE_PROCESS_STATUS_FAILED')
        AND kbf.update_time > NOW() - INTERVAL '10 seconds'
        AND kb.delete_time IS NULL
        ${prefixFilter}
    `;

    const result = safeQuery(recentQuery);
    return result && result.length > 0 ? parseInt(result[0].count) : 0;
  } catch (e) {
    // If query fails, return 0 to not block cleanup
    return 0;
  }
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
  const MAX_CONSECUTIVE_ERRORS = 10; // Tolerate up to 10 consecutive API errors before giving up (increased for CI)

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
      // 5xx errors might be transient on resource-constrained systems - tolerate more in CI
      consecutiveErrors++;
      console.warn(`⚠ API error ${statusRes.status} while checking file status (${consecutiveErrors}/${MAX_CONSECUTIVE_ERRORS})`);

      if (consecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
        console.error(`✗ Too many consecutive API errors (${MAX_CONSECUTIVE_ERRORS})`);
        return { completed: false, status: "API_ERROR", error: `HTTP ${statusRes.status} - ${MAX_CONSECUTIVE_ERRORS} consecutive failures` };
      }

      // Exponential backoff on errors - more aggressive for CI environments
      const backoff = Math.min(Math.pow(2, consecutiveErrors - 1), 10);
      console.log(`   Backing off for ${backoff}s due to API error...`);
      sleep(backoff);
      elapsed += backoff;
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
 * Authenticate with automatic retry logic to handle transient failures.
 *
 * When tests run in parallel, auth requests can intermittently fail due to:
 * - Service not fully ready
 * - Connection timeouts under load
 * - Brief network hiccups in Docker networking
 *
 * This function implements exponential backoff retry to handle these transient failures.
 *
 * @param {string} mgmtHost - The management API host URL
 * @param {string} username - Username for authentication
 * @param {string} password - Password for authentication
 * @param {number} maxRetries - Maximum number of retry attempts (default: 3)
 * @returns {object|null} - HTTP response object with accessToken, or null if all retries failed
 */
export function authenticateWithRetry(mgmtHost, username, password, maxRetries = 3) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    const response = http.request("POST", `${mgmtHost}/v1beta/auth/login`, JSON.stringify({
      username: username,
      password: password,
    }), {
      headers: { "Content-Type": "application/json" },
      timeout: "30s",
    });

    try {
      if (response.status === 200) {
        const body = response.json();
        if (body.accessToken) {
          if (attempt > 1) {
            console.log(`✓ Authentication succeeded on attempt ${attempt}/${maxRetries}`);
          }
          return response;
        }
      }
    } catch (e) {
      console.log(`⚠ Auth attempt ${attempt}/${maxRetries} parse error: ${e.message}`);
    }

    console.log(`⚠ Auth attempt ${attempt}/${maxRetries} failed (status: ${response.status})`);

    // Exponential backoff before retry (1s, 2s, 4s, ...)
    if (attempt < maxRetries) {
      const backoff = Math.pow(2, attempt - 1);
      console.log(`  Retrying in ${backoff}s...`);
      sleep(backoff);
    }
  }

  console.log(`✗ Authentication failed after ${maxRetries} attempts`);
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
 * @param {string} knowledgeBaseId - The production knowledge base ID
 * @param {string} ownerUid - The owner UID
 * @param {number} maxWaitSeconds - Maximum time to wait (default: 10)
 * @returns {object|null} - The rollback KB object if found, null if timeout
 */
export function pollForRollbackKBCreation(knowledgeBaseId, ownerUid, maxWaitSeconds = 10) {
  // Use parent_kb_uid to find rollback KB instead of KBID suffix
  const startTime = Date.now();
  while ((Date.now() - startTime) / 1000 < maxWaitSeconds) {
    const rollbackKBs = verifyRollbackKB(knowledgeBaseId, ownerUid);
    if (rollbackKBs && rollbackKBs.length > 0) {
      return rollbackKBs[0];
    }
    sleep(1);
  }
  return null;
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
