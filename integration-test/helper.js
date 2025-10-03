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
    const results = constant.db.query(`SELECT uid FROM embedding WHERE kb_file_uid = '${fileUid}'`);
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
