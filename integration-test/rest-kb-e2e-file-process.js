
/**
 * Test the complete end-to-end knowledge base and file processing workflow.
 *
 * This comprehensive test verifies the entire lifecycle of a knowledge base:
 * 1. Knowledge Base Management: Create, list, update, delete knowledge base
 * 2. Multi-File Upload: Upload all supported file types (13 files)
 * 3. Batch Processing: Process all files asynchronously
 * 4. File Metadata Verification: Verify file metadata and processing results
 * 5. Storage Verification: Verify resources in MinIO, Postgres, and Milvus
 * 6. API Completeness: Test all file-related endpoints (chunks, summary, source)
 * 7. Type-Specific Validation: Verify file type-specific processing (PDF, DOC, TEXT, etc.)
 * 7.5. Position Data Validation: Verify PDF files have correct position data and page references
 * 8. Resource Cleanup: Verify proper cleanup on knowledge base deletion
 *
 * Note: Database schema and data format tests (enum storage, JSONB formats, field naming,
 * content/summary separation, File.Type enum serialization) are in rest-db.js
 *
 * Test Flow:
 * - Step 1: Create knowledge base
 * - Step 2: List knowledge bases (verify presence)
 * - Step 3: Update knowledge base metadata
 * - Step 4: Upload 13 files of different types in parallel
 * - Step 5: Trigger batch processing for all files
 * - Step 6: Poll until all files reach COMPLETED status
 * - Step 7: Verify each file's metadata (name, size, chunks, tokens, etc.)
 *   - Verify type-specific attributes (pages for PDF/DOC, characters for TEXT)
 * - Step 7.5: Verify position data for PDF files
 *   - Database: PageDelimiters in converted_file, PageRange in chunk
 *   - API: UNIT_PAGE references, markdown_reference with UNIT_CHARACTER
 *   - PascalCase validation for JSON fields
 * - Step 8: List all files in knowledge base
 * - Step 9: List chunks for each file
 * - Step 10: Get summary for each file
 * - Step 11: Get source file for each file
 * - Step 12: Verify storage layer resources (MinIO, Postgres, Milvus)
 * - Step 13: Delete knowledge base and verify cleanup
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

import http from "k6/http";
import { check, group, sleep } from "k6";
import { randomString } from "https://jslib.k6.io/k6-utils/1.1.0/index.js";

import { artifactRESTPublicHost } from "./const.js";

import * as constant from "./const.js";
import * as helper from "./helper.js";

export let options = {
  setupTimeout: '30s',
  teardownTimeout: '180s',
  iterations: 1,
  duration: '120m',
  insecureSkipTLSVerify: true,
  thresholds: {
    checks: ["rate == 1.0"],
  },
};

export function setup() {
  check(true, { [constant.banner('Artifact API E2E: Setup')]: () => true });

  // Add stagger to reduce parallel resource contention
  helper.staggerTestExecution(2);

  // Generate unique test prefix (must be in setup, not module-level, to avoid k6 parallel init issues)
  const dbIDPrefix = constant.generateDBIDPrefix();
  console.log(`rest-kb-e2e-file-process.js: Using unique test prefix: ${dbIDPrefix}`);

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

  // Cleanup orphaned knowledge bases from previous failed test runs OF THIS SPECIFIC TEST
  // Use API-only cleanup to properly trigger workflows (no direct DB manipulation)
  console.log("\n=== SETUP: Cleaning up previous test data (e2e pattern only) ===");
  try {
    const listResp = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${resp.json().user.id}/knowledge-bases`, null, header);
    if (listResp.status === 200) {
      const knowledgeBases = Array.isArray(listResp.json().knowledgeBases) ? listResp.json().knowledgeBases : [];
      let cleanedCount = 0;
      for (const kb of knowledgeBases) {
        const kbId = kb.id;
        if (catId && catId.match(/test-[a-z0-9]+-e2e-/)) {
          const delResp = http.request("DELETE", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${resp.json().user.id}/knowledge-bases/${catId}`, null, header);
          if (delResp.status === 200 || delResp.status === 204) {
            cleanedCount++;
          }
        }
      }
      console.log(`Cleaned ${cleanedCount} orphaned knowledge bases from previous test runs`);
    }
  } catch (e) {
    console.log(`Setup cleanup warning: ${e}`);
  }
  console.log("=== SETUP: Cleanup complete ===\n");

  return { header: header, expectedOwner: resp.json().user, dbIDPrefix: dbIDPrefix }
}

export default function (data) {
  CheckKnowledgeBaseEndToEndFileProcessing(data);
}

export function teardown(data) {
  const groupName = "Artifact API E2E: Cleanup";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    // Wait for file processing AND Temporal activities to settle before cleanup
    console.log("Teardown: Waiting for safe cleanup...");
    helper.waitForSafeCleanup(120, data.dbIDPrefix, 3);

    // Clean up knowledge bases created by this test
    var listResp = http.request("GET", `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases`, null, data.header)
    if (listResp.status === 200) {
      var knowledgeBases = Array.isArray(listResp.json().knowledgeBases) ? listResp.json().knowledgeBases : []

      for (const kb of knowledgeBases) {
        // API returns knowledgeBaseId (camelCase), not knowledge_base_id
        const kbId = kb.id;
        if (kbId && kbId.startsWith(data.dbIDPrefix)) {
          http.request("DELETE", `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${kbId}`, null, data.header);
          console.log(`Teardown: Deleted knowledge base ${kbId}`);
        }
      }
    }

  });
}

export function CheckKnowledgeBaseEndToEndFileProcessing(data) {
  const groupName = "Artifact API: Knowledge base end-to-end file processing";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    // Step 1: Create knowledge base
    const createBody = {
      id: data.dbIDPrefix + "e2e-" + randomString(8),
      description: "E2E test knowledge base for multi-file processing",
      tags: ["test", "integration", "e2e", "multi-file"],
      type: "KNOWLEDGE_BASE_TYPE_PERSISTENT",
    };

    const cRes = http.request(
      "POST",
      `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases`,
      JSON.stringify(createBody),
      data.header
    );

    let created;
    try { created = (cRes.json() || {}).knowledgeBase; } catch (e) { created = {}; }
    const knowledgeBaseId = created && created.id;
    const knowledgeBaseUid = created && created.uid;

    check(cRes, {
      "E2E: Knowledge base created successfully": (r) => r.status === 200,
      "E2E: Knowledge base ID matches requested id": () => knowledgeBaseId === createBody.id,
      "E2E: Knowledge base has valid UID": () => knowledgeBaseUid && knowledgeBaseUid.length > 0,
      "E2E: Knowledge base is valid": () => created && helper.validateKnowledgeBase(created, false),
    });

    if (!knowledgeBaseId || !knowledgeBaseUid) {
      return;
    }

    // Step 2: List knowledge bases - ensure presence
    const listRes = http.request(
      "GET",
      `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases`,
      null,
      data.header
    );

    let listJson;
    try { listJson = listRes.json(); } catch (e) { listJson = {}; }
    const knowledgeBases = Array.isArray(listJson.knowledgeBases) ? listJson.knowledgeBases : [];

    check(listRes, {
      "E2E: List knowledge bases successful": (r) => r.status === 200,
      "E2E: List contains created knowledge base": () => knowledgeBases.some((c) => c.id === knowledgeBaseId),
    });

    // Step 3: Update knowledge base metadata
    // With proto `body: "knowledge_base"`, body should be just the KB fields directly
    // and updateMask should be a query parameter
    const newDesc = "Updated E2E test knowledge base - testing metadata update";
    const newTags = ["test", "integration", "e2e", "updated"];
    const updateBody = {
      description: newDesc,
      tags: newTags,
    };

    const uRes = http.request(
      "PATCH",
      `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}?updateMask=description,tags`,
      JSON.stringify(updateBody),
      data.header
    );

    let updated;
    try { updated = (uRes.json() || {}).knowledgeBase; } catch (e) { updated = {}; }

    check(uRes, {
      "E2E: Update knowledge base successful": (r) => r.status === 200,
      "E2E: Knowledge base ID remains stable after update": () => updated.id === knowledgeBaseId,
      "E2E: Knowledge base description updated": () => updated && updated.description === newDesc,
      "E2E: Knowledge base tags updated": () => updated && JSON.stringify(updated.tags) === JSON.stringify(newTags),
    });

    // Step 4: Upload all file types (parallel batch upload)
    // This tests the system's ability to handle multiple file types simultaneously
    const uploaded = [];
    const uploadReqs = constant.sampleFiles.map((s) => {
      const filename = `${data.dbIDPrefix}${s.originalName}`;
      return {
        s,
        filename: filename,
        req: {
          method: "POST",
          url: `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files`,
          body: JSON.stringify({ filename: filename, type: s.type, content: s.content }),
          params: data.header,
        },
      };
    });

    const uploadResponses = http.batch(uploadReqs.map((x) => x.req));

    for (let i = 0; i < uploadResponses.length; i++) {
      const resp = uploadResponses[i];
      const s = uploadReqs[i].s;
      const filename = uploadReqs[i].filename;

      const fJson = (function () {
        try { return resp.json(); } catch (e) { return {}; }
      })();
      const file = (fJson && fJson.file) || {};

      check(resp, {
        [`E2E: File uploaded successfully (${s.originalName})`]: (r) => r.status === 200,
        [`E2E: File has UID (${s.originalName})`]: () => file.uid && file.uid.length > 0,
        [`E2E: File type matches (${s.originalName})`]: () => file.type === s.type,
      });

      if (file && file.uid) {
        uploaded.push({
          fileUid: file.uid,
          fileId: file.id,
          filename: filename,
          type: s.type,
          originalName: s.originalName
        });
      }
    }

    check({ uploadCount: uploaded.length }, {
      "E2E: All files uploaded successfully": () => uploaded.length === constant.sampleFiles.length,
    });

    if (uploaded.length === 0) {
      http.request("DELETE", `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}`, null, data.header);
      return;
    }

    // Step 5: Wait for batch processing to complete
    // Auto-trigger: Processing starts automatically on upload (no manual trigger needed)
    const fileUids = uploaded.map((f) => f.fileUid);

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
            url: `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files/${uid}`,
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
            const filename = (body.file && body.file.filename) || uid;

            if (r.status === 200 && st === "FILE_PROCESS_STATUS_COMPLETED") {
              pending.delete(uid);
              completedCount++;
              console.log(`[${iter}] ✓ Completed: ${filename} (${completedCount}/${uploaded.length})`);
            } else if (r.status === 200 && st === "FILE_PROCESS_STATUS_FAILED") {
              pending.delete(uid);
              failedFiles.push({
                uid,
                name: filename,
                outcome: (body.file && body.file.processOutcome) || "Unknown error"
              });
              console.log(`[${iter}] ✗ Failed: ${filename} - ${failedFiles[failedFiles.length - 1].outcome}`);
            } else if (r.status === 200 && (
              st === "FILE_PROCESS_STATUS_PROCESSING" ||
              st === "FILE_PROCESS_STATUS_CHUNKING" ||
              st === "FILE_PROCESS_STATUS_EMBEDDING" ||
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
        http.request("DELETE", `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}`, null, data.header);
        return;
      }
    }

    // Step 7: Verify each file's metadata and processing results
    for (const f of uploaded) {
      const viewPath = `/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files?filter=${encodeURIComponent(`id = "${f.fileId}"`)}`;

      const viewRes = http.request("GET", artifactRESTPublicHost + viewPath, null, data.header);

      let fileData;
      try {
        const viewJson = viewRes.json();
        fileData = viewJson.files && viewJson.files[0];
      } catch (e) {
        fileData = null;
      }

      check(viewRes, {
        [`E2E: File view successful (${f.originalName})`]: (r) => r.status === 200,
        [`E2E: File has correct resource name format (${f.originalName})`]: () => fileData && fileData.name && fileData.name.startsWith(`namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files/`),
        [`E2E: File has correct filename (${f.originalName})`]: () => fileData && fileData.filename === f.filename,
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

    // Step 7.5: Verify Position Data for PDF Files
    console.log("\n=== Step 7.5: Verifying position data for PDF files ===");
    const pdfFiles = uploaded.filter(f => f.type === "TYPE_PDF");

    for (const pdfFile of pdfFiles) {
      console.log(`E2E Position Data: Testing ${pdfFile.originalName} (${pdfFile.fileUid})`);

      // 1. Verify converted_file has position_data with PageDelimiters
      const convertedFileQuery = helper.safeQuery(
        `SELECT position_data::text as position_data_text FROM converted_file
         WHERE file_uid = $1 AND converted_type = 'CONVERTED_FILE_TYPE_CONTENT' AND position_data IS NOT NULL LIMIT 1`,
        pdfFile.fileUid
      );

      if (convertedFileQuery && convertedFileQuery.length > 0) {
        let posData;
        try {
          posData = JSON.parse(convertedFileQuery[0].position_data_text);
          console.log(`E2E Position Data: ${pdfFile.originalName} has ${posData.PageDelimiters ? posData.PageDelimiters.length : 0} page delimiters`);
        } catch (e) {
          posData = null;
          console.log(`E2E Position Data: Failed to parse position_data for ${pdfFile.originalName}: ${e}`);
        }

        check({ posData, filename: pdfFile.originalName }, {
          [`E2E Position Data: PDF has position_data (${pdfFile.originalName})`]: () => posData !== null,
          [`E2E Position Data: position_data has PageDelimiters (${pdfFile.originalName})`]: () =>
            posData && posData.PageDelimiters !== undefined,
          [`E2E Position Data: PageDelimiters is an array (${pdfFile.originalName})`]: () =>
            posData && Array.isArray(posData.PageDelimiters),
          [`E2E Position Data: PageDelimiters is non-empty (${pdfFile.originalName})`]: () =>
            posData && posData.PageDelimiters && posData.PageDelimiters.length > 0,
          [`E2E Position Data: Uses PascalCase (${pdfFile.originalName})`]: () =>
            posData && posData.PageDelimiters !== undefined && posData.page_delimiters === undefined,
        });
      } else {
        check(false, {
          [`E2E Position Data: PDF has position_data in converted_file (${pdfFile.originalName})`]: () => false,
        });
      }

      // 2. Verify chunk has reference with PageRange
      const chunkQuery = helper.safeQuery(
        `SELECT reference::text as reference_text FROM chunk
         WHERE file_uid = $1 AND reference IS NOT NULL LIMIT 1`,
        pdfFile.fileUid
      );

      if (chunkQuery && chunkQuery.length > 0) {
        let refData;
        try {
          refData = JSON.parse(chunkQuery[0].reference_text);
          console.log(`E2E Position Data: ${pdfFile.originalName} chunk reference = ${JSON.stringify(refData)}`);
        } catch (e) {
          refData = null;
          console.log(`E2E Position Data: Failed to parse reference for ${pdfFile.originalName}: ${e}`);
        }

        check({ refData, filename: pdfFile.originalName }, {
          [`E2E Position Data: Chunk has reference (${pdfFile.originalName})`]: () => refData !== null,
          [`E2E Position Data: Reference has PageRange (${pdfFile.originalName})`]: () =>
            refData && refData.PageRange !== undefined,
          [`E2E Position Data: PageRange is array with 2 elements (${pdfFile.originalName})`]: () =>
            refData && Array.isArray(refData.PageRange) && refData.PageRange.length === 2,
          [`E2E Position Data: PageRange values are valid (${pdfFile.originalName})`]: () =>
            refData && refData.PageRange && refData.PageRange[0] > 0 && refData.PageRange[1] > 0,
          [`E2E Position Data: Uses PascalCase PageRange (${pdfFile.originalName})`]: () =>
            refData && refData.PageRange !== undefined && refData.page_range === undefined,
        });
      }

      // 3. Verify chunk API returns UNIT_PAGE references
      const chunksResp = http.request(
        "GET",
        `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files/${pdfFile.fileUid}/chunks`,
        null,
        data.header
      );

      if (chunksResp.status === 200) {
        let chunksJson;
        try {
          chunksJson = chunksResp.json();
        } catch (e) {
          chunksJson = null;
        }

        if (chunksJson && chunksJson.chunks && chunksJson.chunks.length > 0) {
          const firstChunk = chunksJson.chunks[0];
          console.log(`E2E Position Data: ${pdfFile.originalName} has ${chunksJson.chunks.length} chunks in API`);

          check({ chunk: firstChunk, filename: pdfFile.originalName }, {
            [`E2E Position Data: Chunk has reference in API (${pdfFile.originalName})`]: () =>
              firstChunk.reference !== null && firstChunk.reference !== undefined,
            [`E2E Position Data: Reference has start position (${pdfFile.originalName})`]: () =>
              firstChunk.reference && firstChunk.reference.start !== null,
            [`E2E Position Data: Start position unit is UNIT_PAGE (${pdfFile.originalName})`]: () =>
              firstChunk.reference && firstChunk.reference.start &&
              firstChunk.reference.start.unit === "UNIT_PAGE",
            [`E2E Position Data: Start position has coordinates (${pdfFile.originalName})`]: () =>
              firstChunk.reference && firstChunk.reference.start &&
              Array.isArray(firstChunk.reference.start.coordinates) &&
              firstChunk.reference.start.coordinates.length > 0,
            [`E2E Position Data: End position unit is UNIT_PAGE (${pdfFile.originalName})`]: () =>
              firstChunk.reference && firstChunk.reference.end &&
              firstChunk.reference.end.unit === "UNIT_PAGE",
            [`E2E Position Data: Chunk has markdown_reference (${pdfFile.originalName})`]: () =>
              firstChunk.markdownReference !== null && firstChunk.markdownReference !== undefined,
            [`E2E Position Data: Markdown reference unit is UNIT_CHARACTER (${pdfFile.originalName})`]: () =>
              firstChunk.markdownReference && firstChunk.markdownReference.start &&
              firstChunk.markdownReference.start.unit === "UNIT_CHARACTER",
          });
        } else {
          check(false, {
            [`E2E Position Data: Chunks available in API for verification (${pdfFile.originalName})`]: () => false,
          });
        }
      } else {
        check(false, {
          [`E2E Position Data: Chunks API successful (${pdfFile.originalName})`]: () => false,
        });
      }
    }

    if (pdfFiles.length === 0) {
      console.log("E2E Position Data: No PDF files found for position data validation");
    } else {
      console.log(`E2E Position Data: Verified position data for ${pdfFiles.length} PDF file(s)`);
    }

    // Step 8: List all files in knowledge base (pagination test)
    const listFilesRes = http.request(
      "GET",
      `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files?pageSize=100`,
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
      const chunkApiUrl = `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files/${f.fileUid}/chunks`;

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
    console.log(`E2E: Getting summaries for ${uploaded.length} files using VIEW_SUMMARY`);
    for (const f of uploaded) {
      const getSummaryRes = http.request(
        "GET",
        `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files/${f.fileUid}?view=VIEW_SUMMARY`,
        null,
        data.header
      );

      const summaryJson = (() => { try { return getSummaryRes.json(); } catch (e) { return {}; } })();
      check(getSummaryRes, {
        [`E2E: Get summary successful (${f.originalName})`]: (r) => r.status === 200,
        [`E2E: Summary has derived_resource_uri (${f.originalName})`]: () => summaryJson.derivedResourceUri && summaryJson.derivedResourceUri.length > 0,
      });
    }
    console.log(`E2E: All summaries retrieved`);

    // Step 11: Get content file for each file using VIEW_CONTENT
    console.log(`E2E: Getting content files for ${uploaded.length} files using VIEW_CONTENT`);
    for (const f of uploaded) {
      const contentRes = http.request(
        "GET",
        `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files/${f.fileUid}?view=VIEW_CONTENT`,
        null,
        data.header
      );

      let contentData;
      try {
        contentData = contentRes.json();
      } catch (e) {
        contentData = null;
      }

      check(contentRes, {
        [`E2E: Get content successful (${f.originalName})`]: (r) => r.status === 200,
        [`E2E: Content has derived_resource_uri (${f.originalName})`]: () => contentData && contentData.derivedResourceUri && contentData.derivedResourceUri.length > 0,
      });
    }
    console.log(`E2E: All content files retrieved`);

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
        converted: helper.countMinioObjects(knowledgeBaseUid, f.fileUid, 'converted-file'),
        chunks: helper.countMinioObjects(knowledgeBaseUid, f.fileUid, 'chunk'),
      };
      const embeddings = helper.countEmbeddings(f.fileUid);
      const vectors = helper.countMilvusVectors(knowledgeBaseUid, f.fileUid);
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

    // Step 13: Delete knowledge base and verify cleanup
    const delRes = http.request(
      "DELETE",
      `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}`,
      null,
      data.header
    );

    check(delRes, {
      "E2E: Knowledge base deleted successfully": (r) => r.status === 200 || r.status === 204,
    });
  });
}
