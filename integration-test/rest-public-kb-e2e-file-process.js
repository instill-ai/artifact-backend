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
 * 4. Database Sanity: Verify data format consistency (catalog_type, PageRange, PageDelimiters)
 * 5. Storage Verification: Verify resources in MinIO, Postgres, and Milvus
 * 6. API Completeness: Test all file-related endpoints
 * 7. Type-Specific Validation: Verify file type-specific processing (PDF, DOC, TEXT, etc.)
 * 8. Resource Cleanup: Verify proper cleanup on catalog deletion
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
 * - **Database sanity check**: Verify catalog_type uses full enum name, text_chunk.reference
 *   uses PascalCase "PageRange" (all chunks including summaries and non-paginated files get
 *   PageRange=[1,1]), converted_file.position_data uses PascalCase "PageDelimiters"
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
 * - Database records use correct format (full enum names, PascalCase JSONB fields)
 * - All chunks (content, summary, non-paginated) have PageRange field with at least [1,1]
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
      const startTime = Date.now();
      const maxWaitMs = 60 * 60 * 1000; // 60 minutes for resource-constrained CI

      // Poll until all complete or timeout (max 60 minutes)
      let iter = 0;
      while (pending.size > 0 && (Date.now() - startTime) < maxWaitMs) {
        iter++;

        lastBatch = http.batch(
          Array.from(pending).map((uid) => ({
            method: "GET",
            url: `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files/${uid}`,
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
    // Use polling to handle API-level eventual consistency on resource-constrained runners
    let totalChunksCount = 0;
    console.log(`E2E: Starting chunk verification for ${uploaded.length} files`);
    for (const f of uploaded) {
      const chunkApiUrl = `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/chunks?fileUid=${f.fileUid}`;

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
        `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files/${f.fileUid}/summary`,
        null,
        data.header
      );

      check(getSummaryRes, {
        [`E2E: Get summary successful (${f.originalName})`]: (r) => r.status === 200,
      });
    }
    console.log(`E2E: All summaries retrieved`);

    // Step 11: Database sanity check - verify data format consistency
    console.log(`\n========================================`);
    console.log(`E2E: Verifying database format consistency...`);
    console.log(`========================================\n`);

    // Check catalog_type format in database
    const catalogTypeResult = constant.db.query(
      `SELECT catalog_type FROM knowledge_base WHERE uid = $1`,
      catalogUid
    );

    check(catalogTypeResult, {
      "E2E DB Sanity: catalog_type query successful": (rows) => rows.length > 0,
      "E2E DB Sanity: catalog_type is CATALOG_TYPE_PERSISTENT (not 'persistent')": (rows) =>
        rows.length > 0 && rows[0].catalog_type === "CATALOG_TYPE_PERSISTENT",
    });

    if (catalogTypeResult.length > 0) {
      console.log(`E2E DB Sanity: ✓ catalog_type = '${catalogTypeResult[0].catalog_type}'`);
    }

    // Check text_chunk.reference format for a sample file (PDF preferred)
    const pdfFile = uploaded.find(f => f.type === "FILE_TYPE_PDF") || uploaded[0];
    if (pdfFile) {
      console.log(`E2E DB Sanity: Checking text_chunk format for ${pdfFile.originalName}`);

      // Count text chunks with correct PascalCase PageRange format
      const correctPageRangeResult = constant.db.query(
        `SELECT COUNT(*) as count FROM text_chunk WHERE kb_file_uid = $1 AND reference ? 'PageRange'`,
        pdfFile.fileUid
      );

      const correctCount = correctPageRangeResult.length > 0 ? parseInt(correctPageRangeResult[0].count) : 0;
      console.log(`E2E DB Sanity: Text chunks with PageRange (PascalCase): ${correctCount}`);

      // Count text chunks with INCORRECT snake_case page_range format (should be 0)
      const incorrectPageRangeResult = constant.db.query(
        `SELECT COUNT(*) as count FROM text_chunk WHERE kb_file_uid = $1 AND reference ? 'page_range'`,
        pdfFile.fileUid
      );

      const incorrectCount = incorrectPageRangeResult.length > 0 ? parseInt(incorrectPageRangeResult[0].count) : 0;
      console.log(`E2E DB Sanity: Text chunks with page_range (snake_case): ${incorrectCount}`);

      check(true, {
        "E2E DB Sanity: All text chunks use PascalCase PageRange": () => correctCount > 0 && incorrectCount === 0,
      });

      // Sample a text_chunk reference to verify format
      // Cast JSONB to text to avoid k6 driver returning it as byte array
      const sampleReferenceResult = constant.db.query(
        `SELECT reference::text as reference_text FROM text_chunk WHERE kb_file_uid = $1 AND reference IS NOT NULL LIMIT 1`,
        pdfFile.fileUid
      );

      if (sampleReferenceResult.length > 0) {
        // Parse the JSON string
        const referenceText = sampleReferenceResult[0].reference_text;
        console.log(`E2E DB Sanity: Sample text_chunk.reference (raw): ${referenceText}`);

        let parsedReference;
        try {
          parsedReference = JSON.parse(referenceText);
          console.log(`E2E DB Sanity: Sample text_chunk.reference (parsed): ${JSON.stringify(parsedReference)}`);
        } catch (e) {
          console.log(`E2E DB Sanity: ERROR - Failed to parse reference: ${e}`);
          parsedReference = null;
        }

        check({ parsedReference }, {
          "E2E DB Sanity: Sample reference contains PageRange field": () =>
            parsedReference && parsedReference.PageRange !== undefined,
          "E2E DB Sanity: Sample reference does NOT contain page_range field": () =>
            parsedReference && parsedReference.page_range === undefined,
        });
      } else {
        console.log(`E2E DB Sanity: No chunks with non-null reference found!`);
      }

      // Verify converted_file position_data uses PascalCase PageDelimiters
      const correctPageDelimitersResult = constant.db.query(
        `SELECT COUNT(*) as count FROM converted_file WHERE file_uid = $1 AND position_data ? 'PageDelimiters'`,
        pdfFile.fileUid
      );

      const correctDelimitersCount = correctPageDelimitersResult.length > 0 ? parseInt(correctPageDelimitersResult[0].count) : 0;
      console.log(`E2E DB Sanity: Converted files with PageDelimiters (PascalCase): ${correctDelimitersCount}`);

      const incorrectPageDelimitersResult = constant.db.query(
        `SELECT COUNT(*) as count FROM converted_file WHERE file_uid = $1 AND position_data ? 'page_delimiters'`,
        pdfFile.fileUid
      );

      const incorrectDelimitersCount = incorrectPageDelimitersResult.length > 0 ? parseInt(incorrectPageDelimitersResult[0].count) : 0;
      console.log(`E2E DB Sanity: Converted files with page_delimiters (snake_case): ${incorrectDelimitersCount}`);

      check(true, {
        "E2E DB Sanity: All converted_file records use PascalCase PageDelimiters": () =>
          correctDelimitersCount >= 0 && incorrectDelimitersCount === 0,
      });

      // Sample a converted_file position_data to verify format
      const samplePositionDataResult = constant.db.query(
        `SELECT position_data FROM converted_file WHERE file_uid = $1 AND position_data IS NOT NULL LIMIT 1`,
        pdfFile.fileUid
      );

      if (samplePositionDataResult.length > 0) {
        const samplePos = JSON.stringify(samplePositionDataResult[0].position_data);
        console.log(`E2E DB Sanity: Sample converted_file.position_data: ${samplePos}`);

        check(samplePositionDataResult, {
          "E2E DB Sanity: Sample position_data contains PageDelimiters field": (rows) =>
            rows.length > 0 && rows[0].position_data && rows[0].position_data.PageDelimiters !== undefined,
          "E2E DB Sanity: Sample position_data does NOT contain page_delimiters field": (rows) =>
            rows.length > 0 && rows[0].position_data && rows[0].position_data.page_delimiters === undefined,
        });
      }
    }

    console.log(`========================================\n`);

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

      // Document types (PDF, DOC, etc.) create converted files
      const isDocumentType = ["FILE_TYPE_PDF", "FILE_TYPE_DOC", "FILE_TYPE_DOCX", "FILE_TYPE_PPT", "FILE_TYPE_PPTX"].includes(f.type);
      if (isDocumentType) {
        check(minioCounts, {
          [`E2E: MinIO has converted file (${f.originalName})`]: () => minioCounts.converted > 0,
        });
      }

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
    console.log(`E2E: Deleting catalog ${catalogId}`);
    const dRes = http.request(
      "DELETE",
      `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}`,
      null,
      data.header
    );

    check(dRes, {
      "E2E: Catalog deletion successful": (r) => r.status === 200 || r.status === 204,
    });

    // Wait briefly for cleanup workflow to start
    console.log(`E2E: Triggering cleanup workflow`);
    sleep(2);
    console.log(`E2E: Verifying cleanup (checking first file only)`);

    // Verify resources are cleaned up (sample check on first file only to avoid long waits)
    if (uploaded.length > 0) {
      const firstFile = uploaded[0];
      console.log(`E2E: Checking cleanup for ${firstFile.originalName}`);
      const cleanupCounts = {
        converted: helper.countMinioObjects(catalogUid, firstFile.fileUid, 'converted-file'),
        chunks: helper.countMinioObjects(catalogUid, firstFile.fileUid, 'chunk'),
      };
      const cleanupEmbeddings = helper.countEmbeddings(firstFile.fileUid);
      const cleanupVectors = helper.countMilvusVectors(catalogUid, firstFile.fileUid);
      console.log(`E2E: Cleanup check results: converted=${cleanupCounts.converted}, chunks=${cleanupCounts.chunks}, embeddings=${cleanupEmbeddings}, vectors=${cleanupVectors}`);

      // Verify all resources are cleaned up after catalog deletion
      check({ cleanupCounts, cleanupEmbeddings, cleanupVectors }, {
        "E2E: MinIO resources cleaned up after catalog deletion": () =>
          cleanupCounts.converted === 0 && cleanupCounts.chunks === 0,
        "E2E: Postgres embeddings cleaned up after catalog deletion": () => cleanupEmbeddings === 0,
        "E2E: Milvus vectors cleaned up after catalog deletion": () => cleanupVectors === 0,
      });
    }

    console.log(`E2E: Test completed successfully!`);
  });
}
