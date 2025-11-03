import http from "k6/http";
import { check, group, sleep } from "k6";
import { randomString } from "https://jslib.k6.io/k6-utils/1.1.0/index.js";
import encoding from "k6/encoding";

import * as constant from "./const.js";
import * as helper from "./helper.js";

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
}

export function teardown(data) {
  const groupName = "Artifact API Reprocess: Cleanup";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    // CRITICAL: Wait for THIS TEST's file processing to complete before deleting knowledge bases
    // Deleting knowledge bases triggers cleanup workflows that drop vector DB collections
    // If we delete while files are still processing, we get "collection does not exist" errors
    console.log("Teardown: Waiting for this test's file processing to complete...");
    const allProcessingComplete = helper.waitForAllFileProcessingComplete(120, data.dbIDPrefix);
    if (!allProcessingComplete) {
      console.warn("Teardown: Some files still processing after 120s, proceeding anyway");
    }

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
    const kbName = data.dbIDPrefix + "reprocess-" + randomString(5);
    const createRes = http.request(
      "POST",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases`,
      JSON.stringify({
        id: kbName,
        description: "Reprocessing test",
        tags: ["test", "reprocess"]
      }),
      data.header
    );

    let kb;
    try { kb = createRes.json().knowledgeBase; } catch (e) { kb = {}; }
    const knowledgeBaseId = kb ? kb.id : null;
    const knowledgeBaseUid = kb ? kb.uid : null;

    check(createRes, {
      "Reprocess: Knowledge base created": (r) => r.status === 200 && knowledgeBaseId && knowledgeBaseUid,
    });
    console.log(`✓ Knowledge base created: ${knowledgeBaseId} (UID: ${knowledgeBaseUid})`);

    if (!knowledgeBaseId || !knowledgeBaseUid) {
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
    const filename = `${data.dbIDPrefix}reprocess-test.pdf`;
    const uploadRes = http.request(
      "POST",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files`,
      JSON.stringify({
        filename: filename,
        type: "TYPE_PDF",
        content: constant.samplePdf
      }),
      data.header
    );

    let uploadedFile;
    try { uploadedFile = uploadRes.json().file; } catch (e) { uploadedFile = {}; }
    const fileUid = uploadedFile ? uploadedFile.uid : null;

    check(uploadRes, {
      "Reprocess: File uploaded": (r) => r.status === 200 && fileUid,
    });
    console.log(`✓ File uploaded: ${filename} (UID: ${fileUid})`);

    if (!fileUid) {
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
      fileUid,
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
    console.log("Step 4: Verifying baseline - counting resources after first processing...");
    // Poll storage systems until data appears (handles eventual consistency)
    // MinIO: S3 list operations may show stale data immediately after writes
    // Milvus: Vector inserts need time to be indexed and become queryable
    // This polling approach fixes race conditions that caused intermittent test failures
    const minioBlobsAfterFirst = {
      converted: helper.pollMinIOObjects(knowledgeBaseUid, fileUid, 'converted-file'),
      chunks: helper.pollMinIOObjects(knowledgeBaseUid, fileUid, 'chunk'),
    };

    // Poll database embeddings (handles transaction commit delays)
    const embeddingsAfterFirst = helper.pollEmbeddings(fileUid);

    // Poll Milvus vectors (handles indexing delays - can take 5-10 seconds)
    const milvusVectorsAfterFirst = helper.pollMilvusVectors(knowledgeBaseUid, fileUid);

    // Poll pages and text chunks from API endpoint
    const metadataAfterFirst = helper.pollFileMetadata(data.expectedOwner.id, knowledgeBaseId, fileUid, data.header);
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
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files/${fileUid}/reprocess`,
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
      fileUid,
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

    // Step 7: Verify all intermediate data after reprocessing (FINAL COUNT)
    console.log("Step 7: Verifying resource counts after reprocessing...");
    // Poll storage systems again to handle eventual consistency after reprocessing
    // IMPORTANT: Must count BEFORE knowledge base deletion, which triggers cleanup workflow
    const minioBlobsAfterSecond = {
      converted: helper.pollMinIOObjects(knowledgeBaseUid, fileUid, 'converted-file'),
      chunks: helper.pollMinIOObjects(knowledgeBaseUid, fileUid, 'chunk'),
    };

    const embeddingsAfterSecond = helper.pollEmbeddings(fileUid);
    const milvusVectorsAfterSecond = helper.pollMilvusVectors(knowledgeBaseUid, fileUid);

    // Poll pages and text chunks from API endpoint after reprocessing
    const metadataAfterSecond = helper.pollFileMetadata(data.expectedOwner.id, knowledgeBaseId, fileUid, data.header);
    const pagesAfterSecond = metadataAfterSecond.pages;
    const textChunksAfterSecond = metadataAfterSecond.chunks;

    // THE CRITICAL VERIFICATION: Compare counts to detect resource accumulation bugs
    // If counts INCREASE: Bug! Old resources weren't deleted (accumulation)
    // If counts SAME: Correct! Old resources were deleted before new ones were created
    // If counts DECREASE: Bug! Some resources weren't recreated
    check(minioBlobsAfterSecond, {
      "Reprocess: MinIO converted file count UNCHANGED (no accumulation)": (r) => r.converted === minioBlobsAfterFirst.converted,
      "Reprocess: MinIO chunk count UNCHANGED (no accumulation)": (r) => r.chunks === minioBlobsAfterFirst.chunks,
    });

    check({ embeddingsAfterSecond, embeddingsAfterFirst }, {
      "Reprocess: Database embedding count UNCHANGED (no accumulation)": (r) => r.embeddingsAfterSecond === r.embeddingsAfterFirst,
    });

    check({ milvusVectorsAfterSecond, milvusVectorsAfterFirst }, {
      "Reprocess: Milvus vector count UNCHANGED (no accumulation)": (r) => r.milvusVectorsAfterSecond === r.milvusVectorsAfterFirst,
    });

    check({ pagesAfterSecond, pagesAfterFirst }, {
      "Reprocess: Page count UNCHANGED (no accumulation)": (r) => r.pagesAfterSecond === r.pagesAfterFirst,
    });

    check({ textChunksAfterSecond, textChunksAfterFirst }, {
      "Reprocess: Text chunk count UNCHANGED (no accumulation)": (r) => r.textChunksAfterSecond === r.textChunksAfterFirst,
    });

    // Early exit if reprocessing didn't create resources (indicates failure)
    if (minioBlobsAfterSecond.converted === 0 || minioBlobsAfterSecond.chunks === 0 ||
      embeddingsAfterSecond === 0 || milvusVectorsAfterSecond === 0 ||
      pagesAfterSecond === 0 || textChunksAfterSecond === 0) {
      http.request("DELETE", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}`, null, data.header);
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

    // Cleanup knowledge base - this triggers the cleanup workflow which deletes all remaining blobs
    http.request("DELETE", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}`, null, data.header);

    // Wait for cleanup workflow to complete before next test starts
    // This prevents race conditions with subsequent tests
    // Increased wait time to ensure cleanup completes in resource-limited environments
    console.log("Reprocess: Waiting for cleanup workflow to complete...");
    sleep(15);
  });
}
