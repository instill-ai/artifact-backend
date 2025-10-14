import http from "k6/http";
import { check, group, sleep } from "k6";
import { randomString } from "https://jslib.k6.io/k6-utils/1.1.0/index.js";
import encoding from "k6/encoding";

import { artifactPublicHost } from "./const.js";

import * as constant from "./const.js";
import * as helper from "./helper.js";

/**
 * Test file reprocessing to ensure old intermediate data is cleaned up across all storage systems.
 *
 * This test verifies that when a file is reprocessed:
 * 1. Old intermediate files (converted files, chunks) are deleted from MinIO
 * 2. Old embeddings are deleted from Postgres and Milvus
 * 3. Old pages and text chunks are deleted from the database
 * 4. New intermediate files, embeddings, pages, and chunks are created with fresh content
 * 5. All resource counts remain constant (proving old data was cleaned up)
 *
 * Test Flow:
 * - Upload a PDF file (full processing pipeline: convert → chunk → embed)
 * - Process it once, count MinIO blobs, embeddings, pages, and text chunks (baseline)
 * - Reprocess the same file
 * - Count all resources again
 * - Verify counts are unchanged (old deleted, new created)
 *
 * This prevents resource accumulation bugs where reprocessing would
 * create new resources without deleting old ones, leading to:
 * - Wasted storage in MinIO
 * - Duplicate embeddings in Milvus
 * - Orphaned pages and chunks in the database
 * - Inconsistent search results
 */
export function CheckFileReprocessing(data) {
  const groupName = "Artifact API: File reprocessing cleanup";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });
    console.log("=== Starting File Reprocessing Test ===");

    // Step 1: Create a test catalog
    console.log("Step 1: Creating test catalog...");
    const catalogName = constant.dbIDPrefix + "reprocess-" + randomString(5);
    const createRes = http.request(
      "POST",
      `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`,
      JSON.stringify({
        name: catalogName,
        description: "Reprocessing test",
        tags: ["test", "reprocess"]
      }),
      data.header
    );

    let catalog;
    try { catalog = createRes.json().catalog; } catch (e) { catalog = {}; }
    const catalogId = catalog ? catalog.catalogId : null;
    const catalogUid = catalog ? catalog.catalogUid : null;

    check(createRes, {
      "Reprocess: Catalog created": (r) => r.status === 200 && catalogId && catalogUid,
    });
    console.log(`✓ Catalog created: ${catalogId} (UID: ${catalogUid})`);

    if (!catalogId || !catalogUid) {
      console.log("✗ Failed to create catalog, aborting test");
      return;
    }

    // Step 2: Upload a PDF file
    console.log("Step 2: Uploading PDF file...");
    // Using PDF to test the full processing pipeline:
    // - Conversion step (creates converted-file blobs in MinIO)
    // - Chunking step (creates chunk blobs in MinIO)
    // - Embedding step (creates vectors in Milvus)
    // This provides comprehensive coverage of reprocessing cleanup logic
    const fileName = `${constant.dbIDPrefix}reprocess-test.pdf`;
    const uploadRes = http.request(
      "POST",
      `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files`,
      JSON.stringify({
        name: fileName,
        type: "FILE_TYPE_PDF",
        content: constant.samplePdf
      }),
      data.header
    );

    let uploadedFile;
    try { uploadedFile = uploadRes.json().file; } catch (e) { uploadedFile = {}; }
    const fileUid = uploadedFile ? (uploadedFile.fileUid || uploadedFile.file_uid) : null;

    check(uploadRes, {
      "Reprocess: File uploaded": (r) => r.status === 200 && fileUid,
    });
    console.log(`✓ File uploaded: ${fileName} (UID: ${fileUid})`);

    if (!fileUid) {
      console.log("✗ Failed to upload file, cleaning up and aborting");
      http.request("DELETE", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}`, null, data.header);
      sleep(5); // Wait for cleanup workflow
      return;
    }

    // Step 3: Trigger first processing and wait for completion
    console.log("Step 3: Triggering first processing...");
    const process1Res = http.request(
      "POST",
      `${artifactPublicHost}/v1alpha/catalogs/files/processAsync`,
      JSON.stringify({ fileUids: [fileUid] }),
      data.header
    );

    check(process1Res, {
      "Reprocess: First processing triggered": (r) => r.status === 200,
    });
    console.log("✓ First processing triggered, waiting for completion...");

    // Poll for completion (300 timeout for PDF processing)
    // PDF files require: conversion -> summarizing -> chunking -> embedding
    let firstProcessCompleted = false;
    for (let i = 0; i < 300; i++) {
      sleep(1);
      const statusRes = http.request(
        "GET",
        `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files/${fileUid}`,
        null,
        data.header
      );

      try {
        const body = statusRes.json();
        const status = body.file ? (body.file.processStatus || body.file.process_status) : "";
        if (status === "FILE_PROCESS_STATUS_COMPLETED") {
          firstProcessCompleted = true;
          break;
        } else if (status === "FILE_PROCESS_STATUS_FAILED") {
          const errorMsg = body.file && body.file.processOutcome ? body.file.processOutcome : "Unknown error";
          check(false, { [`Reprocess: First processing failed - ${errorMsg}`]: () => false });
          http.request("DELETE", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}`, null, data.header);
          sleep(5); // Wait for cleanup workflow
          return;
        }
      } catch (e) { /* continue polling */ }
    }

    check({ completed: firstProcessCompleted }, {
      "Reprocess: First processing completed": () => firstProcessCompleted,
    });

    if (!firstProcessCompleted) {
      console.log("✗ First processing did not complete within timeout, cleaning up and aborting");
      http.request("DELETE", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}`, null, data.header);
      sleep(5); // Wait for cleanup workflow
      return;
    }
    console.log("✓ First processing completed successfully");

    // Step 4: Verify intermediate data created after first processing (BASELINE COUNT)
    console.log("Step 4: Verifying baseline - counting resources after first processing...");
    // Poll storage systems until data appears (handles eventual consistency)
    // MinIO: S3 list operations may show stale data immediately after writes
    // Milvus: Vector inserts need time to be indexed and become queryable
    // This polling approach fixes race conditions that caused intermittent test failures
    const minioBlobsAfterFirst = {
      converted: helper.pollMinIOObjects(catalogUid, fileUid, 'converted-file'),
      chunks: helper.pollMinIOObjects(catalogUid, fileUid, 'chunk'),
    };

    // Poll database embeddings (handles transaction commit delays)
    const embeddingsAfterFirst = helper.pollEmbeddings(fileUid);

    // Poll Milvus vectors (handles indexing delays - can take 5-10 seconds)
    const milvusVectorsAfterFirst = helper.pollMilvusVectors(catalogUid, fileUid);

    // Poll pages and text chunks from API endpoint
    const metadataAfterFirst = helper.pollFileMetadata(data.expectedOwner.id, catalogId, fileUid, data.header);
    const pagesAfterFirst = metadataAfterFirst.pages;
    const textChunksAfterFirst = metadataAfterFirst.chunks;

    // Verify all resources exist after first processing
    // If file processing completed successfully, ALL resources must exist including embeddings
    check(minioBlobsAfterFirst, {
      "Reprocess: MinIO has converted file blob after first processing": (r) => r.converted > 0,
      "Reprocess: MinIO has chunk blobs after first processing": (r) => r.chunks > 0,
    });

    check({ embeddings: embeddingsAfterFirst }, {
      "Reprocess: Database has embeddings after first processing": (r) => r.embeddings > 0,
    });

    check({ vectors: milvusVectorsAfterFirst }, {
      "Reprocess: Milvus has vectors after first processing": (r) => r.vectors > 0,
    });

    check({ pages: pagesAfterFirst }, {
      "Reprocess: Database has pages after first processing": (r) => r.pages > 0,
    });

    check({ chunks: textChunksAfterFirst }, {
      "Reprocess: Database has text chunks after first processing": (r) => r.chunks > 0,
    });

    console.log(`✓ Baseline counts: MinIO(converted=${minioBlobsAfterFirst.converted}, chunks=${minioBlobsAfterFirst.chunks}), Postgres(embeddings=${embeddingsAfterFirst}, chunks=${textChunksAfterFirst}), Milvus(vectors=${milvusVectorsAfterFirst}), Pages=${pagesAfterFirst}`);

    // Early exit if baseline verification fails (resources weren't created)
    if (minioBlobsAfterFirst.converted === 0 || minioBlobsAfterFirst.chunks === 0 ||
      embeddingsAfterFirst === 0 || milvusVectorsAfterFirst === 0 ||
      pagesAfterFirst === 0 || textChunksAfterFirst === 0) {
      console.log("✗ Baseline verification failed, cleaning up and aborting");
      http.request("DELETE", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}`, null, data.header);
      sleep(5); // Wait for cleanup workflow
      return;
    }

    // Step 5: Trigger reprocessing (second processing of the same file)
    console.log("Step 5: Triggering reprocessing (second processing)...");
    // This should:
    // 1. Delete old converted-file and chunks from MinIO (cleanup before reprocessing)
    // 2. Delete old embeddings from database and Milvus (cleanup before reprocessing)
    // 3. Re-convert the PDF to markdown
    // 4. Generate new chunks with potentially different content/boundaries
    // 5. Generate new embeddings and store in Milvus
    // 6. Save new converted-file and chunks to MinIO
    // 7. Result: Same counts for all resources, different content
    const process2Res = http.request(
      "POST",
      `${artifactPublicHost}/v1alpha/catalogs/files/processAsync`,
      JSON.stringify({ fileUids: [fileUid] }),
      data.header
    );

    check(process2Res, {
      "Reprocess: Second processing triggered": (r) => r.status === 200,
    });
    console.log("✓ Reprocessing triggered, waiting for completion...");

    // Poll for completion (300s timeout for PDF reprocessing)
    // Note: Temporal workflows may take a few seconds to start due to task queue polling
    let secondProcessCompleted = false;
    for (let i = 0; i < 300; i++) {
      sleep(1);
      const statusRes = http.request(
        "GET",
        `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files/${fileUid}`,
        null,
        data.header
      );

      try {
        const body = statusRes.json();
        const status = body.file ? (body.file.processStatus || body.file.process_status) : "";
        if (status === "FILE_PROCESS_STATUS_COMPLETED") {
          secondProcessCompleted = true;
          break;
        } else if (status === "FILE_PROCESS_STATUS_FAILED") {
          check(false, { "Reprocess: Second processing failed": () => false });
          http.request("DELETE", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}`, null, data.header);
          sleep(5); // Wait for cleanup workflow
          return;
        }
      } catch (e) { /* continue polling */ }
    }

    check({ completed: secondProcessCompleted }, {
      "Reprocess: Second processing completed": () => secondProcessCompleted,
    });

    if (!secondProcessCompleted) {
      console.log("✗ Reprocessing did not complete within timeout, cleaning up and aborting");
      http.request("DELETE", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}`, null, data.header);
      sleep(5); // Wait for cleanup workflow
      return;
    }
    console.log("✓ Reprocessing completed successfully");

    // Step 6: Verify all intermediate data after reprocessing (FINAL COUNT)
    console.log("Step 6: Verifying resource counts after reprocessing...");
    // Poll storage systems again to handle eventual consistency after reprocessing
    // IMPORTANT: Must count BEFORE catalog deletion, which triggers cleanup workflow
    const minioBlobsAfterSecond = {
      converted: helper.pollMinIOObjects(catalogUid, fileUid, 'converted-file'),
      chunks: helper.pollMinIOObjects(catalogUid, fileUid, 'chunk'),
    };

    const embeddingsAfterSecond = helper.pollEmbeddings(fileUid);
    const milvusVectorsAfterSecond = helper.pollMilvusVectors(catalogUid, fileUid);

    // Poll pages and text chunks from API endpoint after reprocessing
    const metadataAfterSecond = helper.pollFileMetadata(data.expectedOwner.id, catalogId, fileUid, data.header);
    const pagesAfterSecond = metadataAfterSecond.pages;
    const textChunksAfterSecond = metadataAfterSecond.chunks;

    check(minioBlobsAfterSecond, {
      "Reprocess: MinIO still has converted file blob after reprocessing": (r) => r.converted > 0,
      "Reprocess: MinIO still has chunk blobs after reprocessing": (r) => r.chunks > 0,
    });

    // Verify all resources exist after reprocessing
    check({ embeddings: embeddingsAfterSecond }, {
      "Reprocess: Database still has embeddings after reprocessing": (r) => r.embeddings > 0,
    });

    check({ vectors: milvusVectorsAfterSecond }, {
      "Reprocess: Milvus still has vectors after reprocessing": (r) => r.vectors > 0,
    });

    check({ pages: pagesAfterSecond }, {
      "Reprocess: Database still has pages after reprocessing": (r) => r.pages > 0,
    });

    check({ chunks: textChunksAfterSecond }, {
      "Reprocess: Database still has text chunks after reprocessing": (r) => r.chunks > 0,
    });

    // Early exit if reprocessing didn't create resources (indicates failure)
    if (minioBlobsAfterSecond.converted === 0 || minioBlobsAfterSecond.chunks === 0 ||
      embeddingsAfterSecond === 0 || milvusVectorsAfterSecond === 0 ||
      pagesAfterSecond === 0 || textChunksAfterSecond === 0) {
      http.request("DELETE", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}`, null, data.header);
      sleep(5); // Wait for cleanup workflow
      return;
    }

    // THE CRITICAL VERIFICATION: All resource counts should be UNCHANGED
    // If counts increase: Bug! Old resources weren't deleted, causing accumulation
    // If counts unchanged: Correct! Old resources were deleted before new ones were created
    // This test validates the fix for reprocessing cleanup logic in:
    // - ConvertToMarkdownActivity (deletes old converted files before creating new ones)
    // - SaveChunksToDBActivity (deletes old chunks before creating new ones)
    // - SaveEmbeddingsToVectorDBWorkflow (deletes old embeddings and vectors before creating new ones)
    // Verify resource counts are unchanged (critical for reprocessing cleanup validation)
    const verificationChecks = {
      convertedBlobsUnchanged: minioBlobsAfterSecond.converted === minioBlobsAfterFirst.converted,
      chunkBlobsUnchanged: minioBlobsAfterSecond.chunks === minioBlobsAfterFirst.chunks,
      embeddingsUnchanged: embeddingsAfterSecond === embeddingsAfterFirst,
      milvusVectorsUnchanged: milvusVectorsAfterSecond === milvusVectorsAfterFirst,
      pagesUnchanged: pagesAfterSecond === pagesAfterFirst,
      textChunksUnchanged: textChunksAfterSecond === textChunksAfterFirst,
    };

    const checkDefinitions = {
      "Reprocess: MinIO converted file count UNCHANGED (old deleted, new created)": (r) => r.convertedBlobsUnchanged,
      "Reprocess: MinIO chunk count UNCHANGED (old deleted, new created)": (r) => r.chunkBlobsUnchanged,
      "Reprocess: Embedding count UNCHANGED (old deleted, new created)": (r) => r.embeddingsUnchanged,
      "Reprocess: Milvus vector count UNCHANGED (old deleted, new created)": (r) => r.milvusVectorsUnchanged,
      "Reprocess: Page count UNCHANGED (old deleted, new created)": (r) => r.pagesUnchanged,
      "Reprocess: Text chunk count UNCHANGED (old deleted, new created)": (r) => r.textChunksUnchanged,
    };

    check(verificationChecks, checkDefinitions);
    console.log(`✓ Reprocessing counts: MinIO(converted=${minioBlobsAfterSecond.converted}, chunks=${minioBlobsAfterSecond.chunks}), Postgres(embeddings=${embeddingsAfterSecond}, chunks=${textChunksAfterSecond}), Milvus(vectors=${milvusVectorsAfterSecond}), Pages=${pagesAfterSecond}`);
    console.log(`✓ Verification: All counts unchanged (old resources deleted, new ones created)`);
    console.log("=== File Reprocessing Test Complete ===\n");

    // Cleanup catalog - this triggers the cleanup workflow which deletes all remaining blobs
    http.request("DELETE", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}`, null, data.header);

    // Wait for cleanup workflow to complete before next test starts
    // This prevents race conditions with subsequent tests
    // Increased wait time to ensure cleanup completes in resource-limited environments
    console.log("Reprocess: Waiting for cleanup workflow to complete...");
    sleep(15);
  });
}
