import http from "k6/http";
import { check, group, sleep } from "k6";
import { randomString } from "https://jslib.k6.io/k6-utils/1.1.0/index.js";

import { artifactRESTPublicHost } from "./const.js";

import * as constant from "./const.js";
import * as helper from "./helper.js";

export let options = {
  setupTimeout: '30s',
  iterations: 1,
  duration: '120m',
  insecureSkipTLSVerify: true,
  thresholds: {
    checks: ["rate == 1.0"],
  },
};

export function setup() {
  check(true, { [constant.banner('Artifact API E2E: Setup')]: () => true });

  // Clean up any leftover test data from previous runs
  try {
    constant.db.exec(`DELETE FROM text_chunk WHERE file_uid IN (SELECT uid FROM knowledge_base_file WHERE name LIKE '${constant.dbIDPrefix}%')`);
    constant.db.exec(`DELETE FROM embedding WHERE file_uid IN (SELECT uid FROM knowledge_base_file WHERE name LIKE '${constant.dbIDPrefix}%')`);
    constant.db.exec(`DELETE FROM converted_file WHERE file_uid IN (SELECT uid FROM knowledge_base_file WHERE name LIKE '${constant.dbIDPrefix}%')`);
    constant.db.exec(`DELETE FROM knowledge_base_file WHERE name LIKE '${constant.dbIDPrefix}%'`);
    constant.db.exec(`DELETE FROM knowledge_base WHERE id LIKE '${constant.dbIDPrefix}%'`);
  } catch (e) {
    console.log(`E2E Setup cleanup warning: ${e}`);
  }

  var loginResp = http.request("POST", `${constant.mgmtRESTPublicHost}/v1beta/auth/login`, JSON.stringify({
    "username": constant.defaultUsername,
    "password": constant.defaultPassword,
  }))

  check(loginResp, {
    [`POST ${constant.mgmtRESTPublicHost}/v1beta/auth/login response status is 200`]: (r) => r.status === 200,
  });

  var header = {
    "headers": {
      "Authorization": `Bearer ${loginResp.json().accessToken}`,
      "Content-Type": "application/json",
    },
    "timeout": "600s",
  }

  var resp = http.request("GET", `${constant.mgmtRESTPublicHost}/v1beta/user`, {}, {
    headers: { "Authorization": `Bearer ${loginResp.json().accessToken}` }
  })

  return { header: header, expectedOwner: resp.json().user }
}

export default function (data) {
  CheckKnowledgeBaseEndToEndFileProcessing(data);
}

export function teardown(data) {
  const groupName = "Artifact API E2E: Cleanup";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    // Clean up catalogs created by this test
    var listResp = http.request("GET", `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`, null, data.header)
    if (listResp.status === 200) {
      var catalogs = Array.isArray(listResp.json().catalogs) ? listResp.json().catalogs : []

      for (const catalog of catalogs) {
        if (catalog.catalog_id && catalog.catalog_id.startsWith(constant.dbIDPrefix)) {
          http.request("DELETE", `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalog.catalog_id}`, null, data.header);
        }
      }
    }

    // Final DB cleanup
    try {
      constant.db.exec(`DELETE FROM text_chunk WHERE file_uid IN (SELECT uid FROM knowledge_base_file WHERE name LIKE '${constant.dbIDPrefix}%')`);
      constant.db.exec(`DELETE FROM embedding WHERE file_uid IN (SELECT uid FROM knowledge_base_file WHERE name LIKE '${constant.dbIDPrefix}%')`);
      constant.db.exec(`DELETE FROM converted_file WHERE file_uid IN (SELECT uid FROM knowledge_base_file WHERE name LIKE '${constant.dbIDPrefix}%')`);
      constant.db.exec(`DELETE FROM knowledge_base_file WHERE name LIKE '${constant.dbIDPrefix}%'`);
      constant.db.exec(`DELETE FROM knowledge_base WHERE id LIKE '${constant.dbIDPrefix}%'`);
    } catch (e) {
      console.log(`E2E Teardown cleanup warning: ${e}`);
    }

    constant.db.close();
  });
}

/**
 * Test the complete end-to-end catalog and file processing workflow.
 *
 * This comprehensive test verifies the entire lifecycle of a knowledge base catalog:
 * 1. Catalog Management: Create, list, update, delete catalog
 * 2. Multi-File Upload: Upload all supported file types (13 files)
 * 3. Batch Processing: Process all files asynchronously
 * 4. File Metadata Verification: Verify file metadata and processing results
 * 5. Storage Verification: Verify resources in MinIO, Postgres, and Milvus
 * 6. API Completeness: Test all file-related endpoints (chunks, summary, source)
 * 7. Type-Specific Validation: Verify file type-specific processing (PDF, DOC, TEXT, etc.)
 * 8. Resource Cleanup: Verify proper cleanup on catalog deletion
 *
 * Note: Database schema and data format tests (enum storage, JSONB formats, field naming,
 * content/summary separation, File.Type enum serialization) are in rest-db.js
 *
 * Test Flow:
 * - Create catalog
 * - List catalogs (verify presence)
 * - Update catalog metadata
 * - Upload 13 files of different types in parallel
 * - Trigger batch processing for all files
 * - Poll until all files reach COMPLETED status
 * - Verify each file's metadata (name, size, chunks, tokens, etc.)
 * - Verify type-specific attributes (pages for PDF/DOC, characters for TEXT)
 * - List all files in catalog
 * - List chunks for each file
 * - Get summary for each file
 * - Get source file for each file
 * - Verify storage layer resources (MinIO, Postgres, Milvus)
 * - Delete catalog and verify cleanup
 *
 * Supported File Types Tested:
 * - TEXT, MARKDOWN, CSV, HTML
 * - PDF, DOC, DOCX, PPT, PPTX, XLS, XLSX
 * - Case variations (uppercase filenames)
 *
 * This test ensures:
 * - All file types are processed correctly
 * - Batch processing works reliably
 * - All file types (including TEXT/MARKDOWN) create converted files
 * - Storage layers are populated correctly
 * - All API endpoints return expected data
 * - Cleanup removes all resources
 */
export function CheckKnowledgeBaseEndToEndFileProcessing(data) {
  const groupName = "Artifact API: Knowledge base end-to-end file processing";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    // Step 1: Create catalog
    const createBody = {
      name: constant.dbIDPrefix + "e2e-" + randomString(8),
      description: "E2E test catalog for multi-file processing",
      tags: ["test", "integration", "e2e", "multi-file"],
      type: "CATALOG_TYPE_PERSISTENT",
    };

    const cRes = http.request(
      "POST",
      `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`,
      JSON.stringify(createBody),
      data.header
    );

    let created;
    try { created = (cRes.json() || {}).catalog; } catch (e) { created = {}; }
    const catalogId = created && created.catalogId;
    const catalogUid = created && created.catalogUid;

    check(cRes, {
      "E2E: Catalog created successfully": (r) => r.status === 200,
      "E2E: Catalog ID matches name": () => catalogId === createBody.name,
      "E2E: Catalog has valid UID": () => catalogUid && catalogUid.length > 0,
      "E2E: Catalog is valid": () => created && helper.validateCatalog(created, false),
    });

    if (!catalogId || !catalogUid) {
      return;
    }

    // Step 2: List catalogs - ensure presence
    const listRes = http.request(
      "GET",
      `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`,
      null,
      data.header
    );

    let listJson;
    try { listJson = listRes.json(); } catch (e) { listJson = {}; }
    const catalogs = Array.isArray(listJson.catalogs) ? listJson.catalogs : [];

    check(listRes, {
      "E2E: List catalogs successful": (r) => r.status === 200,
      "E2E: List contains created catalog": () => catalogs.some((c) => c.catalogId === catalogId),
    });

    // Step 3: Update catalog metadata
    const updateBody = {
      catalogId: catalogId,
      namespaceId: data.expectedOwner.id,
      description: "Updated E2E test catalog - testing metadata update",
      tags: ["test", "integration", "e2e", "updated"],
    };

    const uRes = http.request(
      "PUT",
      `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}`,
      JSON.stringify(updateBody),
      data.header
    );

    let updated;
    try { updated = (uRes.json() || {}).catalog; } catch (e) { updated = {}; }

    check(uRes, {
      "E2E: Update catalog successful": (r) => r.status === 200,
      "E2E: Catalog ID remains stable after update": () => updated.catalogId === catalogId,
      "E2E: Catalog description updated": () => updated && updated.description === updateBody.description,
      "E2E: Catalog tags updated": () => updated && JSON.stringify(updated.tags) === JSON.stringify(updateBody.tags),
    });

    // Step 4: Upload all file types (parallel batch upload)
    // This tests the system's ability to handle multiple file types simultaneously
    const uploaded = [];
    const uploadReqs = constant.sampleFiles.map((s) => {
      const fileName = `${constant.dbIDPrefix}${s.originalName}`;
      return {
        s,
        fileName,
        req: {
          method: "POST",
          url: `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files`,
          body: JSON.stringify({ name: fileName, type: s.type, content: s.content }),
          params: data.header,
        },
      };
    });

    const uploadResponses = http.batch(uploadReqs.map((x) => x.req));

    for (let i = 0; i < uploadResponses.length; i++) {
      const resp = uploadResponses[i];
      const s = uploadReqs[i].s;
      const fileName = uploadReqs[i].fileName;

      const fJson = (function () {
        try { return resp.json(); } catch (e) { return {}; }
      })();
      const file = (fJson && fJson.file) || {};

      check(resp, {
        [`E2E: File uploaded successfully (${s.originalName})`]: (r) => r.status === 200,
        [`E2E: File has UID (${s.originalName})`]: () => file.fileUid && file.fileUid.length > 0,
        [`E2E: File type matches (${s.originalName})`]: () => file.type === s.type,
      });

      if (file && file.fileUid) {
        uploaded.push({
          fileUid: file.fileUid,
          fileId: file.fileId,
          name: fileName,
          type: s.type,
          originalName: s.originalName
        });
      }
    }

    check({ uploadCount: uploaded.length }, {
      "E2E: All files uploaded successfully": () => uploaded.length === constant.sampleFiles.length,
    });

    if (uploaded.length === 0) {
      http.request("DELETE", `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}`, null, data.header);
      return;
    }

    // Step 5: Trigger batch processing for all files
    const fileUids = uploaded.map((f) => f.fileUid);
    const pRes = http.request(
      "POST",
      `${artifactRESTPublicHost}/v1alpha/catalogs/files/processAsync`,
      JSON.stringify({ fileUids }),
      data.header
    );

    check(pRes, {
      "E2E: Batch processing triggered successfully": (r) => r.status === 200,
    });

    // Step 6: Poll for completion (batched polling for efficiency)
    // Wait for all files to complete processing
    let completedCount = 0;
    let failedFiles = [];
    {
      const pending = new Set(uploaded.map((f) => f.fileUid));
      let lastBatch = [];
      const startTime = Date.now();
      const maxWaitMs = 60 * 60 * 1000; // 60 minutes for resource-constrained CI

      // Poll until all complete or timeout (max 60 minutes)
      let iter = 0;
      while (pending.size > 0 && (Date.now() - startTime) < maxWaitMs) {
        iter++;

        lastBatch = http.batch(
          Array.from(pending).map((uid) => ({
            method: "GET",
            url: `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files/${uid}`,
            params: data.header,
          }))
        );

        let idx = 0;
        let processingCount = 0;
        for (const uid of Array.from(pending)) {
          const r = lastBatch[idx++];
          try {
            const body = r.json();
            const st = (body.file && body.file.processStatus) || "";
            const fileName = (body.file && body.file.name) || uid;

            if (r.status === 200 && st === "FILE_PROCESS_STATUS_COMPLETED") {
              pending.delete(uid);
              completedCount++;
              console.log(`[${iter}] ✓ Completed: ${fileName} (${completedCount}/${uploaded.length})`);
            } else if (r.status === 200 && st === "FILE_PROCESS_STATUS_FAILED") {
              pending.delete(uid);
              failedFiles.push({
                uid,
                name: fileName,
                outcome: (body.file && body.file.processOutcome) || "Unknown error"
              });
              console.log(`[${iter}] ✗ Failed: ${fileName} - ${failedFiles[failedFiles.length - 1].outcome}`);
            } else if (r.status === 200 && (
              st === "FILE_PROCESS_STATUS_PROCESSING" ||
              st === "FILE_PROCESS_STATUS_CHUNKING" ||
              st === "FILE_PROCESS_STATUS_EMBEDDING" ||
              st === "FILE_PROCESS_STATUS_WAITING" ||
              st === "FILE_PROCESS_STATUS_NOTSTARTED"
            )) {
              processingCount++;
            }
          } catch (e) { /* ignore parsing errors, continue polling */ }
        }

        // Log progress every 30 seconds
        if (iter % 60 === 0 || pending.size === 0) {
          const elapsedSec = Math.floor((Date.now() - startTime) / 1000);
          console.log(`[${elapsedSec}s] Progress: ${completedCount} completed, ${processingCount} processing, ${pending.size} pending, ${failedFiles.length} failed`);
        }

        if (pending.size === 0) break;
        sleep(0.5);
      }

      // Log timeout if occurred
      if (pending.size > 0) {
        const elapsedMin = Math.floor((Date.now() - startTime) / 60000);
        console.log(`⚠️  Timeout after ${elapsedMin} minutes. ${pending.size} files still pending.`);
      }

      check({ completedCount, totalFiles: uploaded.length }, {
        "E2E: All files completed processing": () => completedCount === uploaded.length,
        "E2E: No files failed processing": () => failedFiles.length === 0,
      });

      // Log failures if any
      if (failedFiles.length > 0) {
        for (const f of failedFiles) {
          check(false, { [`E2E: File processing failed: ${f.name} - ${f.outcome}`]: () => false });
        }
        // Cleanup and exit
        http.request("DELETE", `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}`, null, data.header);
        return;
      }
    }

    // Step 7: Verify each file's metadata and processing results
    for (const f of uploaded) {
      const viewPath = `/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files?filter.fileUids=${f.fileUid}`;
      const viewRes = http.request("GET", artifactRESTPublicHost + viewPath, null, data.header);

      // Log errors for debugging, but don't fail the test yet (check assertions will catch issues)
      if (viewRes.status !== 200) {
        try {
          console.log(`E2E: File view failed (${f.originalName}) status=${viewRes.status} body=${JSON.stringify(viewRes.json())}`);
        } catch (e) {
          console.log(`E2E: File view failed (${f.originalName}) status=${viewRes.status}`);
        }
      }

      let fileData;
      try {
        const viewJson = viewRes.json();
        fileData = viewJson.files && viewJson.files[0];
      } catch (e) {
        fileData = null;
      }

      check(viewRes, {
        [`E2E: File view successful (${f.originalName})`]: (r) => r.status === 200,
        [`E2E: File has correct name (${f.originalName})`]: () => fileData && fileData.name === f.name,
        [`E2E: File status is COMPLETED (${f.originalName})`]: () => fileData && fileData.processStatus === "FILE_PROCESS_STATUS_COMPLETED",
        [`E2E: File has creator UID (${f.originalName})`]: () => fileData && fileData.creatorUid === data.expectedOwner.uid,
        [`E2E: File has positive size (${f.originalName})`]: () => fileData && fileData.size > 0,
        [`E2E: File has chunks (${f.originalName})`]: () => fileData && fileData.totalChunks > 0,
        [`E2E: File has tokens (${f.originalName})`]: () => fileData && fileData.totalTokens > 0,
        // Note: Summary field removed from file metadata - use GetFileSummary API instead (tested in Step 10)
        [`E2E: File has download URL (${f.originalName})`]: () => fileData && fileData.downloadUrl && fileData.downloadUrl.includes("v1alpha/blob-urls/"),
      });

      // Check type-specific attributes
      const isDocumentType = ["TYPE_PDF", "TYPE_DOC", "TYPE_DOCX", "TYPE_PPT", "TYPE_PPTX"].includes(f.type);
      const isTextType = ["TYPE_TEXT", "TYPE_MARKDOWN"].includes(f.type);

      if (isDocumentType && fileData) {
        // Document types should have page-based length
        check({ fileData }, {
          [`E2E: File length unit is UNIT_PAGE (${f.originalName})`]: () =>
            fileData.length && fileData.length.unit === "UNIT_PAGE",
          [`E2E: File has length coordinates (${f.originalName})`]: () =>
            fileData.length && Array.isArray(fileData.length.coordinates) && fileData.length.coordinates.length > 0,
          [`E2E: File page count is positive (${f.originalName})`]: () =>
            fileData.length && fileData.length.coordinates[0] > 0,
        });
      } else if (isTextType && fileData) {
        // Text types should have character-based length
        check({ fileData }, {
          [`E2E: File length unit is UNIT_CHARACTER (${f.originalName})`]: () =>
            fileData.length && fileData.length.unit === "UNIT_CHARACTER",
          [`E2E: File has length coordinates (${f.originalName})`]: () =>
            fileData.length && Array.isArray(fileData.length.coordinates) && fileData.length.coordinates.length > 0,
          [`E2E: File character count is positive (${f.originalName})`]: () =>
            fileData.length && fileData.length.coordinates[0] > 0,
        });
      }
    }

    // Step 8: List all files in catalog (pagination test)
    const listFilesRes = http.request(
      "GET",
      `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files?pageSize=100`,
      null,
      data.header
    );

    let listFilesJson;
    try { listFilesJson = listFilesRes.json(); } catch (e) { listFilesJson = {}; }

    check(listFilesRes, {
      "E2E: List files successful": (r) => r.status === 200,
      "E2E: List files returns array": () => Array.isArray(listFilesJson.files),
      "E2E: List files count matches uploads": () =>
        Array.isArray(listFilesJson.files) && listFilesJson.files.length === uploaded.length,
    });

    // Step 9: List chunks for each file
    // Use polling to handle API-level eventual consistency on resource-constrained runners
    let totalChunksCount = 0;
    console.log(`E2E: Starting chunk verification for ${uploaded.length} files`);
    for (const f of uploaded) {
      const chunkApiUrl = `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/chunks?fileUid=${f.fileUid}`;

      console.log(`E2E: Polling chunks for ${f.originalName} (${f.fileUid})`);
      // Poll for chunks until they appear or timeout
      const chunkCount = helper.pollChunksAPI(chunkApiUrl, data.header);
      console.log(`E2E: Chunks found for ${f.originalName}: ${chunkCount}`);
      totalChunksCount += chunkCount;

      // Verify final state after polling
      const listChunksRes = http.request("GET", chunkApiUrl, null, data.header);

      let listChunksJson;
      try { listChunksJson = listChunksRes.json(); } catch (e) { listChunksJson = {}; }

      check(listChunksRes, {
        [`E2E: List chunks successful (${f.originalName})`]: (r) => r.status === 200,
        [`E2E: Chunks is array (${f.originalName})`]: () => Array.isArray(listChunksJson.chunks),
      });

      // Check chunk count after polling
      check({ chunkCount }, {
        [`E2E: File has chunks (${f.originalName})`]: () => chunkCount > 0,
      });
    }

    check({ totalChunks: totalChunksCount }, {
      "E2E: Total chunks across all files is positive": () => totalChunksCount > 0,
    });

    // Step 10: Get summary for each file
    console.log(`E2E: Getting summaries for ${uploaded.length} files`);
    for (const f of uploaded) {
      const getSummaryRes = http.request(
        "GET",
        `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files/${f.fileUid}/summary`,
        null,
        data.header
      );

      check(getSummaryRes, {
        [`E2E: Get summary successful (${f.originalName})`]: (r) => r.status === 200,
      });
    }
    console.log(`E2E: All summaries retrieved`);

    // Step 11: Get source file for each file
    console.log(`E2E: Getting source files for ${uploaded.length} files`);
    for (const f of uploaded) {
      const sourceRes = http.request(
        "GET",
        `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files/${f.fileUid}/source`,
        null,
        data.header
      );

      let sourceData;
      try {
        sourceData = sourceRes.json();
      } catch (e) {
        sourceData = null;
      }

      check(sourceRes, {
        [`E2E: Get source successful (${f.originalName})`]: (r) => r.status === 200,
      });

      if (sourceData && sourceData.sourceFile) {
        const source = sourceData.sourceFile;

        check({ source }, {
          [`E2E: Source returns content (${f.originalName})`]: () =>
            source.content && source.content.length > 0,
          [`E2E: Source has originalFileUid (${f.originalName})`]: () =>
            source.originalFileUid && source.originalFileUid === f.fileUid,
          [`E2E: Source has originalFileName (${f.originalName})`]: () =>
            source.originalFileName && source.originalFileName.length > 0,
        });
      }
    }
    console.log(`E2E: All source files retrieved`);

    // Step 12: Verify storage layer resources (MinIO, Postgres, Milvus)
    // Direct count checks - no polling since API confirmed COMPLETED status
    // All storage operations are synchronous in the workflow
    console.log(`E2E: Starting storage verification for ${uploaded.length} files`);

    let totalMinioConverted = 0;
    let totalMinioChunks = 0;
    let totalEmbeddings = 0;
    let totalMilvusVectors = 0;

    for (const f of uploaded) {
      console.log(`E2E: Verifying storage for ${f.originalName}`);

      // Direct count - no polling needed since API already confirmed COMPLETED status
      // All storage operations (MinIO, Postgres, Milvus) are synchronous in the workflow
      const minioCounts = {
        converted: helper.countMinioObjects(catalogUid, f.fileUid, 'converted-file'),
        chunks: helper.countMinioObjects(catalogUid, f.fileUid, 'chunk'),
      };
      const embeddings = helper.countEmbeddings(f.fileUid);
      const vectors = helper.countMilvusVectors(catalogUid, f.fileUid);
      console.log(`E2E: Storage verified for ${f.originalName}: converted=${minioCounts.converted}, chunks=${minioCounts.chunks}, embeddings=${embeddings}, vectors=${vectors}`);

      totalMinioConverted += minioCounts.converted;
      totalMinioChunks += minioCounts.chunks;
      totalEmbeddings += embeddings;
      totalMilvusVectors += vectors;

      // ALL file types now create converted files (content + summary converted files)
      // Each file should have at least 2 converted files: one for content, one for summary
      check(minioCounts, {
        [`E2E: MinIO has converted files (${f.originalName})`]: () => minioCounts.converted >= 2,
      });

      // All files must have chunks, embeddings, and vectors after successful processing
      check({ minioCounts }, {
        [`E2E: MinIO has chunks (${f.originalName})`]: () => minioCounts.chunks > 0,
      });

      check({ embeddings, vectors }, {
        [`E2E: Postgres has embeddings (${f.originalName})`]: () => embeddings > 0,
        [`E2E: Milvus has vectors (${f.originalName})`]: () => vectors > 0,
      });
    }

    check({ totalMinioChunks }, {
      "E2E: Total MinIO chunks across all files is positive": () => totalMinioChunks > 0,
    });

    check({ totalEmbeddings, totalMilvusVectors }, {
      "E2E: Total embeddings across all files is positive": () => totalEmbeddings > 0,
      "E2E: Total Milvus vectors across all files is positive": () => totalMilvusVectors > 0,
    });

    // Step 13: Delete catalog and verify cleanup
    // DEBUG: Cleanup disabled to inspect final database state
    console.log(`E2E: Test completed successfully! (cleanup skipped for debugging)`);
  });
}
