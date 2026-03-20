import { check, group, sleep } from "k6";
import { randomString } from "https://jslib.k6.io/k6-utils/1.1.0/index.js";
import encoding from "k6/encoding";

import * as constant from "./const.js";
import * as helper from "./helper.js";

// Use httpRetry for automatic retry on transient errors (429, 5xx)
const http = helper.httpRetry;

export let options = {
  setupTimeout: '30s',
  teardownTimeout: '180s', // Increased to accommodate file processing wait (120s) + cleanup
  iterations: 1,
  duration: '120m',
  insecureSkipTLSVerify: true,
  thresholds: {
    checks: ["rate == 1.0"],
  },
};

export function setup() {
  check(true, { [constant.banner('Artifact API Reprocess: Setup')]: () => true });

  // Add stagger to reduce parallel resource contention
  helper.staggerTestExecution(2);

  // Generate unique test prefix (must be in setup, not module-level, to avoid k6 parallel init issues)
  const dbIDPrefix = constant.generateDBIDPrefix();
  console.log(`rest-file-reprocess.js: Using unique test prefix: ${dbIDPrefix}`);

  // Authenticate with retry to handle transient failures
  const authHeader = helper.getBasicAuthHeader(constant.defaultUsername, constant.defaultPassword);
  var header = {
    "headers": {
      "Authorization": authHeader,
      "Content-Type": "application/json",
    },
    "timeout": "600s",
  }

  var resp = http.request("GET", `${constant.mgmtRESTPublicHost}/v1beta/user`, {}, {
    headers: { "Authorization": authHeader }
  })

  // Cleanup orphaned knowledge bases from previous failed test runs OF THIS SPECIFIC TEST
  // Use API-only cleanup to properly trigger workflows (no direct DB manipulation)
  console.log("\n=== SETUP: Cleaning up previous test data (reprocess pattern only) ===");
  try {
    const listResp = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${resp.json().user.id}/knowledge-bases`, null, header);
    if (listResp.status === 200) {
      const knowledgeBases = Array.isArray(listResp.json().knowledgeBases) ? listResp.json().knowledgeBases : [];
      let cleanedCount = 0;
      for (const kb of knowledgeBases) {
        const kbId = kb.id;
        if (kbId && kbId.match(/test-[a-z0-9]+-reprocess-/)) {
          const delResp = http.request("DELETE", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${resp.json().user.id}/knowledge-bases/${kbId}`, null, header);
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
  CheckFileReprocessing(data);
  CheckPatchMerge(data);
}

export function teardown(data) {
  const groupName = "Artifact API Reprocess: Cleanup";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    // Wait for file processing AND Temporal activities to settle before cleanup
    console.log("Teardown: Waiting for safe cleanup...");
    helper.waitForSafeCleanup(120, data.dbIDPrefix, 3);

    // Clean up knowledge bases created by this test
    var listResp = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases`, null, data.header)
    if (listResp.status === 200) {
      var knowledgeBases = Array.isArray(listResp.json().knowledgeBases) ? listResp.json().knowledgeBases : []

      for (const kb of knowledgeBases) {
        // API returns knowledgeBaseId (camelCase), not knowledge_base_id
        const kbId = kb.id;
        if (kbId && kbId.startsWith(data.dbIDPrefix)) {
          http.request("DELETE", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${kbId}`, null, data.header);
          console.log(`Teardown: Deleted knowledge base ${kbId}`);
        }
      }
    }
  });
}

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
 * - Step 1: Create knowledge base
 * - Step 2: Upload a PDF file (full processing pipeline: convert → chunk → embed)
 * - Step 3: Wait for first processing to complete
 * - Step 4: Count MinIO blobs, embeddings, pages, and text chunks (baseline)
 * - Step 5: Call reprocess API endpoint
 * - Step 6: Wait for reprocessing to complete
 * - Step 7: Count all resources again
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

    // Step 1: Create a test knowledge base
    console.log("Step 1: Creating test knowledge base...");
    const kbDisplayName = data.dbIDPrefix + " Reprocess " + randomString(5);
    const createRes = http.request(
      "POST",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases`,
      JSON.stringify({
        displayName: kbDisplayName,
        description: "Reprocessing test",
        tags: ["test", "reprocess"]
      }),
      data.header
    );

    let kb;
    try { kb = createRes.json().knowledgeBase; } catch (e) { kb = {}; }
    // Note: uid field removed in AIP refactoring - use id for identification
    const knowledgeBaseId = kb ? kb.id : null;

    check(createRes, {
      "Reprocess: Knowledge base created": (r) => r.status === 200 && knowledgeBaseId,
    });
    console.log(`✓ Knowledge base created: ${knowledgeBaseId}`);

    if (!knowledgeBaseId) {
      console.log("✗ Failed to create knowledge base, aborting test");
      return;
    }

    // Step 2: Upload a PDF file
    console.log("Step 2: Uploading PDF file...");
    // Using PDF to test the full processing pipeline:
    // - Conversion step (creates converted-file blobs in MinIO)
    // - Chunking step (creates chunk blobs in MinIO)
    // - Embedding step (creates vectors in Milvus)
    // This provides comprehensive coverage of reprocessing cleanup logic
    const fileDisplayName = `${data.dbIDPrefix}reprocess-test.pdf`;
    const uploadRes = http.request(
      "POST",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files`,
      JSON.stringify({
        displayName: fileDisplayName,
        type: "TYPE_PDF",
        content: constant.docSamplePdf
      }),
      data.header
    );

    let uploadedFile;
    try { uploadedFile = uploadRes.json().file; } catch (e) { uploadedFile = {}; }
    // Note: uid field removed in AIP refactoring - use id for identification
    const fileId = uploadedFile ? uploadedFile.id : null;

    check(uploadRes, {
      "Reprocess: File uploaded": (r) => r.status === 200 && fileId,
    });
    console.log(`✓ File uploaded: ${fileDisplayName} (ID: ${fileId})`);

    if (!fileId) {
      console.log("✗ Failed to upload file, cleaning up and aborting");
      http.request("DELETE", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}`, null, data.header);
      sleep(5); // Wait for cleanup workflow
      return;
    }

    // Step 3: Wait for first processing to complete
    // Auto-trigger: Processing starts automatically on upload (no manual trigger needed)
    console.log("Step 3: Waiting for first processing to complete...");

    // Wait for first processing to complete (600s timeout for PDF processing)
    // PDF files require: conversion -> summarizing -> chunking -> embedding
    const firstProcessResult = helper.waitForFileProcessingComplete(
      data.expectedOwner.id,
      knowledgeBaseId,
      fileId,
      data.header,
      600 // 5 minutes for PDF processing
    );

    if (firstProcessResult.status === "FAILED") {
      check(false, { [`Reprocess: First processing failed - ${firstProcessResult.error}`]: () => false });
      http.request("DELETE", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}`, null, data.header);
      sleep(5); // Wait for cleanup workflow
      return;
    }

    check(firstProcessResult, {
      "Reprocess: First processing completed": (r) => r.completed && r.status === "COMPLETED",
    });

    if (!firstProcessResult.completed) {
      console.log(`✗ First processing did not complete (${firstProcessResult.status}), cleaning up and aborting`);
      http.request("DELETE", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}`, null, data.header);
      sleep(5); // Wait for cleanup workflow
      return;
    }
    console.log("✓ First processing completed successfully");

    // Step 4: Verify intermediate data created after first processing (BASELINE COUNT)
    console.log("Step 4: Verifying baseline - checking file metadata after first processing...");

    // Poll pages and text chunks from API endpoint
    const metadataAfterFirst = helper.pollFileMetadata(data.expectedOwner.id, knowledgeBaseId, fileId, data.header);
    const pagesAfterFirst = metadataAfterFirst.pages;
    const textChunksAfterFirst = metadataAfterFirst.chunks;

    check({ pages: pagesAfterFirst }, {
      "Reprocess: Database has pages after first processing": (r) => r.pages > 0,
    });

    check({ chunks: textChunksAfterFirst }, {
      "Reprocess: Database has text chunks after first processing": (r) => r.chunks > 0,
    });

    // Get internal UIDs for MinIO/Milvus verification (via database lookup)
    const kbUid = helper.getKnowledgeBaseUidFromId(knowledgeBaseId);
    const fileUid = helper.getFileUidFromId(fileId);
    console.log(`✓ Internal UIDs: kbUid=${kbUid}, fileUid=${fileUid}`);

    // Verify MinIO and Milvus resources after first processing
    const minioChunksAfterFirst = (kbUid && fileUid) ? helper.countMinioObjects(kbUid, fileUid, "chunk") : 0;
    const milvusVectorsAfterFirst = (kbUid && fileUid) ? helper.countMilvusVectors(kbUid, fileUid) : 0;
    const dbEmbeddingsAfterFirst = fileUid ? helper.countEmbeddings(fileUid) : 0;

    check({ minioChunks: minioChunksAfterFirst }, {
      "Reprocess: MinIO has chunks after first processing": (r) => r.minioChunks > 0,
    });

    check({ milvusVectors: milvusVectorsAfterFirst, dbEmbeddings: dbEmbeddingsAfterFirst }, {
      "Reprocess: Milvus vectors match DB embeddings after first processing": (r) => r.milvusVectors === r.dbEmbeddings,
    });

    console.log(`✓ Baseline counts: Postgres(chunks=${textChunksAfterFirst}), Pages=${pagesAfterFirst}, MinIO(chunks=${minioChunksAfterFirst}), Milvus(vectors=${milvusVectorsAfterFirst}), DB(embeddings=${dbEmbeddingsAfterFirst})`);

    // Early exit if baseline verification fails (resources weren't created)
    if (pagesAfterFirst === 0 || textChunksAfterFirst === 0) {
      console.log("✗ Baseline verification failed, cleaning up and aborting");
      http.request("DELETE", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}`, null, data.header);
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

    const reprocessRes = http.request(
      "POST",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files/${fileId}/reprocess`,
      null,
      data.header
    );

    check(reprocessRes, {
      "Reprocess: Reprocess API call successful": (r) => r.status === 200,
    });

    if (reprocessRes.status !== 200) {
      console.log(`✗ Failed to trigger reprocessing (status ${reprocessRes.status}), cleaning up and aborting`);
      try {
        console.log(`Reprocess error response: ${reprocessRes.body}`);
      } catch (e) {
        console.log(`Could not parse error response: ${e}`);
      }
      http.request("DELETE", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}`, null, data.header);
      sleep(5); // Wait for cleanup workflow
      return;
    }
    console.log("✓ Reprocess API triggered successfully");

    console.log("Step 6: Waiting for reprocessing to complete...");
    // Wait for reprocessing to complete (600s timeout for PDF reprocessing)
    // Note: Temporal workflows may take a few seconds to start due to task queue polling
    const secondProcessResult = helper.waitForFileProcessingComplete(
      data.expectedOwner.id,
      knowledgeBaseId,
      fileId,
      data.header,
      600
    );

    if (secondProcessResult.status === "FAILED") {
      check(false, { [`Reprocess: Second processing failed - ${secondProcessResult.error}`]: () => false });
      http.request("DELETE", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}`, null, data.header);
      sleep(5); // Wait for cleanup workflow
      return;
    }

    check(secondProcessResult, {
      "Reprocess: Second processing completed": (r) => r.completed && r.status === "COMPLETED",
    });

    if (!secondProcessResult.completed) {
      console.log(`✗ Reprocessing did not complete (${secondProcessResult.status}), cleaning up and aborting`);
      http.request("DELETE", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}`, null, data.header);
      sleep(5); // Wait for cleanup workflow
      return;
    }
    console.log("✓ Reprocessing completed successfully");

    // Step 7: Verify resource counts after reprocessing (FINAL COUNT)
    console.log("Step 7: Verifying resource counts after reprocessing...");

    // Poll pages and text chunks from API endpoint after reprocessing
    const metadataAfterSecond = helper.pollFileMetadata(data.expectedOwner.id, knowledgeBaseId, fileId, data.header);
    const pagesAfterSecond = metadataAfterSecond.pages;
    const textChunksAfterSecond = metadataAfterSecond.chunks;

    // Verify MinIO and Milvus resources after reprocessing
    const minioChunksAfterSecond = (kbUid && fileUid) ? helper.countMinioObjects(kbUid, fileUid, "chunk") : 0;
    const milvusVectorsAfterSecond = (kbUid && fileUid) ? helper.countMilvusVectors(kbUid, fileUid) : 0;
    const dbEmbeddingsAfterSecond = fileUid ? helper.countEmbeddings(fileUid) : 0;

    console.log(`✓ After reprocess: Postgres(chunks=${textChunksAfterSecond}), Pages=${pagesAfterSecond}, MinIO(chunks=${minioChunksAfterSecond}), Milvus(vectors=${milvusVectorsAfterSecond}), DB(embeddings=${dbEmbeddingsAfterSecond})`);

    // THE CRITICAL VERIFICATION: Compare counts to detect resource accumulation bugs
    // If counts INCREASE: Bug! Old resources weren't deleted (accumulation)
    // If counts SAME: Correct! Old resources were deleted before new ones were created
    // If counts DECREASE: Bug! Some resources weren't recreated
    check({ pagesAfterSecond, pagesAfterFirst }, {
      "Reprocess: Page count UNCHANGED (no accumulation)": (r) => r.pagesAfterSecond === r.pagesAfterFirst,
    });

    check({ textChunksAfterSecond, textChunksAfterFirst }, {
      "Reprocess: Text chunk count UNCHANGED (no accumulation)": (r) => r.textChunksAfterSecond === r.textChunksAfterFirst,
    });

    check({ minioChunksAfterSecond, minioChunksAfterFirst }, {
      "Reprocess: MinIO chunk count UNCHANGED (no accumulation)": (r) => r.minioChunksAfterSecond === r.minioChunksAfterFirst,
    });

    check({ milvusVectorsAfterSecond, milvusVectorsAfterFirst }, {
      "Reprocess: Milvus vector count UNCHANGED (no accumulation)": (r) => r.milvusVectorsAfterSecond === r.milvusVectorsAfterFirst,
    });

    check({ dbEmbeddingsAfterSecond, dbEmbeddingsAfterFirst }, {
      "Reprocess: DB embedding count UNCHANGED (no accumulation)": (r) => r.dbEmbeddingsAfterSecond === r.dbEmbeddingsAfterFirst,
    });

    check({ milvusVectorsAfterSecond, dbEmbeddingsAfterSecond }, {
      "Reprocess: Milvus vectors match DB embeddings after reprocessing": (r) => r.milvusVectorsAfterSecond === r.dbEmbeddingsAfterSecond,
    });

    // Early exit if reprocessing didn't create resources (indicates failure)
    if (pagesAfterSecond === 0 || textChunksAfterSecond === 0) {
      http.request("DELETE", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}`, null, data.header);
      sleep(5); // Wait for cleanup workflow
      return;
    }

    // THE CRITICAL VERIFICATION: Page and chunk counts should be UNCHANGED
    // If counts increase: Bug! Old resources weren't deleted, causing accumulation
    // If counts unchanged: Correct! Old resources were deleted before new ones were created
    // Note: MinIO/Milvus verification skipped in AIP refactoring - internal UUIDs no longer exposed
    const verificationChecks = {
      pagesUnchanged: pagesAfterSecond === pagesAfterFirst,
      textChunksUnchanged: textChunksAfterSecond === textChunksAfterFirst,
    };

    const checkDefinitions = {
      "Reprocess: Page count UNCHANGED (old deleted, new created)": (r) => r.pagesUnchanged,
      "Reprocess: Text chunk count UNCHANGED (old deleted, new created)": (r) => r.textChunksUnchanged,
    };

    check(verificationChecks, checkDefinitions);
    console.log(`✓ Reprocessing counts: Postgres(chunks=${textChunksAfterSecond}), Pages=${pagesAfterSecond}`);
    console.log(`✓ Verification: API-level counts unchanged (old resources deleted, new ones created)`);
    console.log("=== File Reprocessing Test Complete ===\n");

    // Cleanup knowledge base - this triggers the cleanup workflow which deletes all remaining blobs
    http.request("DELETE", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}`, null, data.header);

    // Wait for cleanup workflow to complete before next test starts
    // This prevents race conditions with subsequent tests
    // Increased wait time to ensure cleanup completes in resource-limited environments
    console.log("Reprocess: Waiting for cleanup workflow to complete...");
    sleep(15);
  });
}

/**
 * Test patch merge workflow:
 * 1. Create KB and upload a text file.
 * 2. Wait for first processing.
 * 3. Submit a patch via UpdateFile (content field mask) → verify ExternalMetadata flag.
 * 4. Trigger reprocess → wait → verify ExternalMetadata flag is still present (patch persists).
 * 5. Clear patch via UpdateFile with empty content → verify flag removed.
 * 6. Trigger reprocess → wait → verify clean completion without patch.
 */
export function CheckPatchMerge(data) {
  const groupName = "Artifact API: Patch merge workflow";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });
    console.log("=== Starting Patch Merge Test ===");

    // Step 1: Create KB
    console.log("PatchMerge Step 1: Creating knowledge base...");
    const kbDisplayName = data.dbIDPrefix + " Patch " + randomString(5);
    const createRes = http.request(
      "POST",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases`,
      JSON.stringify({
        displayName: kbDisplayName,
        description: "Patch merge integration test",
        tags: ["test", "patch"]
      }),
      data.header
    );

    let kb;
    try { kb = createRes.json().knowledgeBase; } catch (e) { kb = {}; }
    const knowledgeBaseId = kb ? kb.id : null;

    check(createRes, {
      "PatchMerge: Knowledge base created": (r) => r.status === 200 && knowledgeBaseId,
    });

    if (!knowledgeBaseId) {
      console.log("✗ PatchMerge: Failed to create knowledge base, aborting");
      return;
    }
    console.log(`✓ PatchMerge: KB created: ${knowledgeBaseId}`);

    // Step 2: Upload a small markdown file
    console.log("PatchMerge Step 2: Uploading markdown file...");
    const fileDisplayName = `${data.dbIDPrefix}patch-test.md`;
    // Simple text file with a deliberate "error" for the patch to fix
    const mdContent = encoding.b64encode("# Test Document\n\nThe speaker nme is Alice.\n\nShe said hello.");
    const uploadRes = http.request(
      "POST",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files`,
      JSON.stringify({
        displayName: fileDisplayName,
        type: "TYPE_MARKDOWN",
        content: mdContent
      }),
      data.header
    );

    let uploadedFile;
    try { uploadedFile = uploadRes.json().file; } catch (e) { uploadedFile = {}; }
    const fileId = uploadedFile ? uploadedFile.id : null;

    check(uploadRes, {
      "PatchMerge: File uploaded": (r) => r.status === 200 && fileId,
    });

    if (!fileId) {
      console.log("✗ PatchMerge: Failed to upload file, aborting");
      http.request("DELETE", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}`, null, data.header);
      sleep(5);
      return;
    }
    console.log(`✓ PatchMerge: File uploaded: ${fileId}`);

    // Step 3: Wait for first processing to complete
    console.log("PatchMerge Step 3: Waiting for first processing...");
    const firstResult = helper.waitForFileProcessingComplete(
      data.expectedOwner.id, knowledgeBaseId, fileId, data.header, 300
    );

    check(firstResult, {
      "PatchMerge: First processing completed": (r) => r.completed && r.status === "COMPLETED",
    });

    if (!firstResult.completed) {
      console.log(`✗ PatchMerge: First processing did not complete (${firstResult.status}), aborting`);
      http.request("DELETE", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}`, null, data.header);
      sleep(5);
      return;
    }
    console.log("✓ PatchMerge: First processing complete");

    // Step 4: Submit a patch via UpdateFile with content field mask
    console.log("PatchMerge Step 4: Submitting patch...");
    const patchText = "Fix typo: 'nme' should be 'name' throughout the document.";
    const updateRes = http.request(
      "PATCH",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files/${fileId}?update_mask=content`,
      JSON.stringify({
        file: {
          content: patchText
        }
      }),
      data.header
    );

    check(updateRes, {
      "PatchMerge: UpdateFile with patch returned 200": (r) => r.status === 200,
    });

    if (updateRes.status === 200) {
      // Verify the x-instill-patch flag is set in ExternalMetadata
      const fileRes = http.request(
        "GET",
        `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files/${fileId}`,
        null,
        data.header
      );
      if (fileRes.status === 200) {
        let fileData;
        try { fileData = fileRes.json().file; } catch (e) { fileData = null; }
        const externalMeta = fileData && fileData.externalMetadata ? fileData.externalMetadata : {};
        check(externalMeta, {
          "PatchMerge: x-instill-patch flag set to true": (m) => m["x-instill-patch"] === "true",
        });
        console.log(`✓ PatchMerge: ExternalMetadata flag: ${JSON.stringify(externalMeta)}`);
      }
    }
    console.log("✓ PatchMerge: Patch submitted");

    // Step 5: Trigger reprocess with patch active (should use patch-only fast path)
    console.log("PatchMerge Step 5: Triggering reprocess with patch (patch-only fast path)...");
    const patchReprocessStart = Date.now();
    const reprocessRes = http.request(
      "POST",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files/${fileId}/reprocess`,
      null,
      data.header
    );

    check(reprocessRes, {
      "PatchMerge: Reprocess with patch triggered": (r) => r.status === 200,
    });

    if (reprocessRes.status === 200) {
      const reprocessResult = helper.waitForFileProcessingComplete(
        data.expectedOwner.id, knowledgeBaseId, fileId, data.header, 300
      );
      const patchReprocessDuration = (Date.now() - patchReprocessStart) / 1000;
      console.log(`PatchMerge: Patch-only reprocess took ${patchReprocessDuration.toFixed(1)}s`);

      check(reprocessResult, {
        "PatchMerge: Reprocess with patch completed": (r) => r.completed && r.status === "COMPLETED",
      });

      if (reprocessResult.completed) {
        // Verify patch flag still present (persists across reprocesses)
        const fileRes2 = http.request(
          "GET",
          `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files/${fileId}`,
          null,
          data.header
        );
        if (fileRes2.status === 200) {
          let fileData2;
          try { fileData2 = fileRes2.json().file; } catch (e) { fileData2 = null; }
          const externalMeta2 = fileData2 && fileData2.externalMetadata ? fileData2.externalMetadata : {};
          check(externalMeta2, {
            "PatchMerge: x-instill-patch flag persists after reprocess": (m) => m["x-instill-patch"] === "true",
          });
        }

        // Verify patch was actually applied: download VIEW_CONTENT and check text
        console.log("PatchMerge Step 5b: Verifying patch applied to content...");
        const contentRes = http.request(
          "GET",
          `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files/${fileId}?view=VIEW_CONTENT`,
          null,
          data.header
        );
        if (contentRes.status === 200) {
          let contentJson;
          try { contentJson = contentRes.json(); } catch (e) { contentJson = null; }
          const contentUrl = contentJson && contentJson.derivedResourceUri;
          if (contentUrl) {
            const mdRes = http.request("GET", contentUrl, null, { timeout: "30s" });
            if (mdRes.status === 200) {
              const markdown = mdRes.body || "";
              check({ markdown }, {
                "PatchMerge: Patched content contains 'speaker name' (typo fixed)": (o) => o.markdown.includes("speaker name"),
                "PatchMerge: Patched content no longer contains 'speaker nme' (typo removed)": (o) => !o.markdown.includes("speaker nme"),
              });
              console.log(`✓ PatchMerge: Content verified — contains 'name': ${markdown.includes("speaker name")}, free of 'nme': ${!markdown.includes("speaker nme")}`);
            } else {
              console.log(`⚠ PatchMerge: Could not download content markdown (status ${mdRes.status}), skipping text verification`);
            }
          }
        }
        // Verify chunks were regenerated by checking chunk listing
        console.log("PatchMerge Step 5c: Verifying chunks were regenerated...");
        const chunkRes = http.request(
          "GET",
          `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/chunks?fileUid=${fileId}`,
          null,
          data.header
        );
        if (chunkRes.status === 200) {
          let chunkData;
          try { chunkData = chunkRes.json(); } catch (e) { chunkData = null; }
          const chunkCount = chunkData && chunkData.chunks ? chunkData.chunks.length : 0;
          check({ chunkCount }, {
            "PatchMerge: Chunks regenerated after patch-only reprocess": (o) => o.chunkCount > 0,
          });
          console.log(`✓ PatchMerge: ${chunkCount} chunks present after patch-only reprocess`);
        }

        console.log("✓ PatchMerge: Reprocess with patch completed and flag persists");
      }
    }

    // Step 6: Clear patch by sending empty content
    console.log("PatchMerge Step 6: Clearing patch...");
    const clearRes = http.request(
      "PATCH",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files/${fileId}?update_mask=content`,
      JSON.stringify({
        file: {
          content: ""
        }
      }),
      data.header
    );

    check(clearRes, {
      "PatchMerge: UpdateFile with empty content (clear) returned 200": (r) => r.status === 200,
    });

    if (clearRes.status === 200) {
      // Verify the flag is removed
      const fileRes3 = http.request(
        "GET",
        `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files/${fileId}`,
        null,
        data.header
      );
      if (fileRes3.status === 200) {
        let fileData3;
        try { fileData3 = fileRes3.json().file; } catch (e) { fileData3 = null; }
        const externalMeta3 = fileData3 && fileData3.externalMetadata ? fileData3.externalMetadata : {};
        check(externalMeta3, {
          "PatchMerge: x-instill-patch flag removed after clear": (m) => !m["x-instill-patch"] || m["x-instill-patch"] !== "true",
        });
        console.log(`✓ PatchMerge: ExternalMetadata after clear: ${JSON.stringify(externalMeta3)}`);
      }
    }
    console.log("✓ PatchMerge: Patch cleared");

    // Step 7: Reprocess without patch
    console.log("PatchMerge Step 7: Triggering reprocess without patch...");
    const finalReprocessRes = http.request(
      "POST",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files/${fileId}/reprocess`,
      null,
      data.header
    );

    check(finalReprocessRes, {
      "PatchMerge: Final reprocess (no patch) triggered": (r) => r.status === 200,
    });

    if (finalReprocessRes.status === 200) {
      const finalResult = helper.waitForFileProcessingComplete(
        data.expectedOwner.id, knowledgeBaseId, fileId, data.header, 300
      );
      check(finalResult, {
        "PatchMerge: Final reprocess completed": (r) => r.completed && r.status === "COMPLETED",
      });

      if (finalResult.completed) {
        // After patch is cleared, AI-generated content should revert to original typo
        console.log("PatchMerge Step 7b: Verifying content reverted to original after patch cleared...");
        const finalContentRes = http.request(
          "GET",
          `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files/${fileId}?view=VIEW_CONTENT`,
          null,
          data.header
        );
        if (finalContentRes.status === 200) {
          let finalContentJson;
          try { finalContentJson = finalContentRes.json(); } catch (e) { finalContentJson = null; }
          const finalContentUrl = finalContentJson && finalContentJson.derivedResourceUri;
          if (finalContentUrl) {
            const finalMdRes = http.request("GET", finalContentUrl, null, { timeout: "30s" });
            if (finalMdRes.status === 200) {
              const finalMarkdown = finalMdRes.body || "";
              check({ finalMarkdown }, {
                "PatchMerge: Content after clear contains 'Alice' (document preserved)": (o) => o.finalMarkdown.includes("Alice"),
                "PatchMerge: Content after clear no longer contains 'speaker name' (patch gone)": (o) => !o.finalMarkdown.includes("speaker name"),
              });
              console.log(`✓ PatchMerge: Content after clear — 'Alice' present: ${finalMarkdown.includes("Alice")}, 'speaker name' absent: ${!finalMarkdown.includes("speaker name")}`);
            } else {
              console.log(`⚠ PatchMerge: Could not download final content (status ${finalMdRes.status}), skipping text verification`);
            }
          }
        }
      }
      console.log(`✓ PatchMerge: Final reprocess completed (${finalResult.status})`);
    }

    console.log("=== Patch Merge Test Complete ===\n");

    // Cleanup
    http.request("DELETE", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}`, null, data.header);
    sleep(15);
  });
}
