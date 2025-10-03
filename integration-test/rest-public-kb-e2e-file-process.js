import http from "k6/http";
import { check, group, sleep } from "k6";
import { randomString } from "https://jslib.k6.io/k6-utils/1.1.0/index.js";

import { artifactPublicHost } from "./const.js";

import * as constant from "./const.js";
import * as helper from "./helper.js";

/**
 * Test the complete end-to-end catalog and file processing workflow.
 *
 * This comprehensive test verifies the entire lifecycle of a knowledge base catalog:
 * 1. Catalog Management: Create, list, update, delete catalog
 * 2. Multi-File Upload: Upload all supported file types (13 files)
 * 3. Batch Processing: Process all files asynchronously
 * 4. Storage Verification: Verify resources in MinIO, Postgres, and Milvus
 * 5. API Completeness: Test all file-related endpoints
 * 6. Type-Specific Validation: Verify file type-specific processing (PDF, DOC, TEXT, etc.)
 * 7. Resource Cleanup: Verify proper cleanup on catalog deletion
 *
 * Test Flow:
 * - Create catalog
 * - List catalogs (verify presence)
 * - Update catalog metadata
 * - Upload 13 files of different types in parallel
 * - Trigger batch processing for all files
 * - Poll until all files reach COMPLETED status
 * - Verify each file's metadata (name, size, chunks, tokens, summary, etc.)
 * - Verify type-specific attributes (pages for PDF/DOC, characters for TEXT)
 * - List all files in catalog
 * - List chunks for each file
 * - Get summary for each file
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
      `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`,
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
      `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`,
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
      `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}`,
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
          url: `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files`,
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
      http.request("DELETE", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}`, null, data.header);
      return;
    }

    // Step 5: Trigger batch processing for all files
    const fileUids = uploaded.map((f) => f.fileUid);
    const pRes = http.request(
      "POST",
      `${artifactPublicHost}/v1alpha/catalogs/files/processAsync`,
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

      // Poll for up to 30 minutes (3600 iterations * 0.5s = 1800s)
      for (let iter = 0; iter < 3600 && pending.size > 0; iter++) {
        lastBatch = http.batch(
          Array.from(pending).map((uid) => ({
            method: "GET",
            url: `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files/${uid}`,
            params: data.header,
          }))
        );

        let idx = 0;
        for (const uid of Array.from(pending)) {
          const r = lastBatch[idx++];
          try {
            const body = r.json();
            const st = (body.file && body.file.processStatus) || "";
            const fileName = (body.file && body.file.name) || uid;

            if (r.status === 200 && st === "FILE_PROCESS_STATUS_COMPLETED") {
              pending.delete(uid);
              completedCount++;
            } else if (r.status === 200 && st === "FILE_PROCESS_STATUS_FAILED") {
              pending.delete(uid);
              failedFiles.push({
                uid,
                name: fileName,
                outcome: (body.file && body.file.processOutcome) || "Unknown error"
              });
            }
          } catch (e) { /* ignore parsing errors, continue polling */ }
        }

        if (pending.size === 0) break;
        sleep(0.5);
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
        http.request("DELETE", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}`, null, data.header);
        return;
      }
    }

    // Step 7: Verify each file's metadata and processing results
    for (const f of uploaded) {
      const viewPath = `/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files?filter.fileUids=${f.fileUid}`;
      const viewRes = http.request("GET", artifactPublicHost + viewPath, null, data.header);

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
        [`E2E: File has summary (${f.originalName})`]: () => fileData && fileData.summary && fileData.summary.length > 0,
        [`E2E: File has download URL (${f.originalName})`]: () => fileData && fileData.downloadUrl && fileData.downloadUrl.includes("v1alpha/blob-urls/"),
      });

      // Check type-specific attributes
      const isDocumentType = ["FILE_TYPE_PDF", "FILE_TYPE_DOC", "FILE_TYPE_DOCX", "FILE_TYPE_PPT", "FILE_TYPE_PPTX"].includes(f.type);
      const isTextType = ["FILE_TYPE_TEXT", "FILE_TYPE_MARKDOWN"].includes(f.type);

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
      `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files?pageSize=100`,
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
    let totalChunksCount = 0;
    for (const f of uploaded) {
      const listChunksRes = http.request(
        "GET",
        `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/chunks?fileUid=${f.fileUid}`,
        null,
        data.header
      );

      let listChunksJson;
      try { listChunksJson = listChunksRes.json(); } catch (e) { listChunksJson = {}; }

      const chunksArray = Array.isArray(listChunksJson.chunks) ? listChunksJson.chunks : [];
      totalChunksCount += chunksArray.length;

      check(listChunksRes, {
        [`E2E: List chunks successful (${f.originalName})`]: (r) => r.status === 200,
        [`E2E: Chunks is array (${f.originalName})`]: () => Array.isArray(listChunksJson.chunks),
      });

      // Only check chunk count if request was successful
      if (listChunksRes.status === 200) {
        check({ chunksArray }, {
          [`E2E: File has chunks (${f.originalName})`]: () => chunksArray.length > 0,
        });
      }
    }

    check({ totalChunks: totalChunksCount }, {
      "E2E: Total chunks across all files is positive": () => totalChunksCount > 0,
    });

    // Step 10: Get summary for each file
    for (const f of uploaded) {
      const getSummaryRes = http.request(
        "GET",
        `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files/${f.fileUid}/summary`,
        null,
        data.header
      );

      check(getSummaryRes, {
        [`E2E: Get summary successful (${f.originalName})`]: (r) => r.status === 200,
      });
    }

    // Step 11: Verify storage layer resources (MinIO, Postgres, Milvus)
    // Wait briefly for asynchronous writes to complete
    sleep(2);

    // Count total resources across all files
    let totalMinioConverted = 0;
    let totalMinioChunks = 0;
    let totalEmbeddings = 0;
    let totalMilvusVectors = 0;

    for (const f of uploaded) {
      const minioCounts = {
        converted: helper.countMinioObjects(catalogUid, f.fileUid, 'converted-file'),
        chunks: helper.countMinioObjects(catalogUid, f.fileUid, 'chunk'),
      };
      const embeddings = helper.countEmbeddings(f.fileUid);
      const vectors = helper.countMilvusVectors(catalogUid, f.fileUid);

      totalMinioConverted += minioCounts.converted;
      totalMinioChunks += minioCounts.chunks;
      totalEmbeddings += embeddings;
      totalMilvusVectors += vectors;

      // Document types (PDF, DOC, etc.) create converted files
      const isDocumentType = ["FILE_TYPE_PDF", "FILE_TYPE_DOC", "FILE_TYPE_DOCX", "FILE_TYPE_PPT", "FILE_TYPE_PPTX"].includes(f.type);
      if (isDocumentType) {
        check(minioCounts, {
          [`E2E: MinIO has converted file (${f.originalName})`]: () => minioCounts.converted > 0,
        });
      }

      // All files should have chunks
      // Embeddings/vectors are conditional based on API key availability
      check({ minioCounts }, {
        [`E2E: MinIO has chunks (${f.originalName})`]: () => minioCounts.chunks > 0,
      });

      // Only check embeddings/vectors if they exist (requires OpenAI API key)
      if (embeddings > 0 || vectors > 0) {
        check({ embeddings, vectors }, {
          [`E2E: Postgres has embeddings (${f.originalName})`]: () => embeddings > 0,
          [`E2E: Milvus has vectors (${f.originalName})`]: () => vectors > 0,
        });
      }
    }

    const hasAnyEmbeddings = totalEmbeddings > 0 || totalMilvusVectors > 0;

    check({ totalMinioChunks }, {
      "E2E: Total MinIO chunks across all files is positive": () => totalMinioChunks > 0,
    });

    if (hasAnyEmbeddings) {
      check({ totalEmbeddings, totalMilvusVectors }, {
        "E2E: Total embeddings across all files is positive": () => totalEmbeddings > 0,
        "E2E: Total Milvus vectors across all files is positive": () => totalMilvusVectors > 0,
      });
    } else {
      console.log("E2E: Skipping total embedding checks - embeddings not created (likely missing OpenAI API key)");
    }

    // Step 12: Delete catalog and verify cleanup
    const dRes = http.request(
      "DELETE",
      `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}`,
      null,
      data.header
    );

    check(dRes, {
      "E2E: Catalog deletion successful": (r) => r.status === 200 || r.status === 204,
    });

    // Wait for cleanup workflow to complete
    sleep(15);

    // Verify resources are cleaned up (sample check on first file)
    if (uploaded.length > 0) {
      const firstFile = uploaded[0];
      const cleanupCounts = {
        converted: helper.countMinioObjects(catalogUid, firstFile.fileUid, 'converted-file'),
        chunks: helper.countMinioObjects(catalogUid, firstFile.fileUid, 'chunk'),
      };
      const cleanupEmbeddings = helper.countEmbeddings(firstFile.fileUid);
      const cleanupVectors = helper.countMilvusVectors(catalogUid, firstFile.fileUid);

      // Verify all resources are cleaned up after catalog deletion
      check({ cleanupCounts, cleanupEmbeddings, cleanupVectors }, {
        "E2E: MinIO resources cleaned up after catalog deletion": () =>
          cleanupCounts.converted === 0 && cleanupCounts.chunks === 0,
        "E2E: Postgres embeddings cleaned up after catalog deletion": () => cleanupEmbeddings === 0,
        "E2E: Milvus vectors cleaned up after catalog deletion": () => cleanupVectors === 0,
      });
    }
  });
}
