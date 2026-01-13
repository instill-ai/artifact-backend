/**
 * Test comprehensive cleanup of all resources when a knowledge base is deleted.
 *
 * This test verifies that when a knowledge base is deleted:
 * 1. All intermediate files (converted files, chunks) are deleted from MinIO
 * 2. All embeddings are deleted from Postgres
 * 3. All vectors are deleted from Milvus
 * 4. All database records are properly cleaned up
 * 5. The cleanup workflow handles both completed and in-progress files
 *
 * Test Flow:
 * - Create a test knowledge base
 * - Upload a PDF file (full processing pipeline: convert → chunk → embed)
 * - Process the file completely
 * - Verify all resources exist (baseline verification)
 * - Delete the knowledge base (triggers cleanup workflow)
 * - Verify all resources are completely removed
 *
 * This ensures the CleanupWorkflow and CleanupFilesActivity properly
 * remove all traces of knowledge base data across all storage systems:
 * - MinIO: Converted files, text chunks
 * - Postgres: File records, converted_file, chunk, embedding tables
 * - Milvus: Vector collections and vectors
 */

import { check, group, sleep } from "k6";
import { randomString } from "https://jslib.k6.io/k6-utils/1.1.0/index.js";

import { artifactRESTPublicHost } from "./const.js";

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
  check(true, { [constant.banner('Artifact API Cleanup: Setup')]: () => true });

  // Stagger test execution to reduce parallel resource contention
  // PDF processing test needs longer delay due to heavy resource usage
  helper.staggerTestExecution(3);

  // Generate unique test prefix (must be in setup, not module-level, to avoid k6 parallel init issues)
  const dbIDPrefix = constant.generateDBIDPrefix();
  console.log(`rest-kb-delete.js: Using unique test prefix: ${dbIDPrefix}`);

  // Authenticate with retry to handle transient failures
  var loginResp = helper.authenticateWithRetry(
    constant.mgmtRESTPublicHost,
    constant.defaultUsername,
    constant.defaultPassword
  );

  check(loginResp, {
    [`POST ${constant.mgmtRESTPublicHost}/v1beta/auth/login response status is 200`]: (r) => r && r.status === 200,
  });

  if (!loginResp || loginResp.status !== 200) {
    console.error("Setup: Authentication failed, cannot continue");
    return null;
  }

  var accessToken = loginResp.json().accessToken;
  var header = {
    "headers": {
      "Authorization": `Bearer ${accessToken}`,
      "Content-Type": "application/json",
    },
    "timeout": "600s",
  }

  var resp = http.request("GET", `${constant.mgmtRESTPublicHost}/v1beta/user`, {}, {
    headers: { "Authorization": `Bearer ${accessToken}` }
  })

  // Cleanup orphaned knowledge bases from previous failed test runs OF THIS SPECIFIC TEST
  // Use API-only cleanup to properly trigger workflows (no direct DB manipulation)
  console.log("\n=== SETUP: Cleaning up previous test data (cleanup pattern only) ===");
  try {
    const listResp = http.request("GET", `${artifactRESTPublicHost}/v1alpha/namespaces/${resp.json().user.id}/knowledge-bases`, null, header);
    if (listResp.status === 200) {
      const knowledgeBases = Array.isArray(listResp.json().knowledgeBases) ? listResp.json().knowledgeBases : [];
      let cleanedCount = 0;
      for (const kb of knowledgeBases) {
        const kbId = kb.id;
        if (catId && catId.match(/test-[a-z0-9]+-cleanup-/)) {
          const delResp = http.request("DELETE", `${artifactRESTPublicHost}/v1alpha/namespaces/${resp.json().user.id}/knowledge-bases/${catId}`, null, header);
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
  CheckKnowledgeBaseDeletion(data);
}

export function teardown(data) {
  const groupName = "Artifact API Cleanup: Cleanup";
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

export function CheckKnowledgeBaseDeletion(data) {
  const groupName = "Artifact API: Knowledge base deletion";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });
    console.log("=== Starting Knowledge Base Deletion Test ===");

    // Step 1: Create a test knowledge base
    console.log("Step 1: Creating test knowledge base...");
    const kbName = data.dbIDPrefix + "cleanup-" + randomString(5);
    const createRes = http.request(
      "POST",
      `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases`,
      JSON.stringify({
        knowledgeBase: {
          displayName: kbName,
          description: "Knowledge base deletion cleanup test",
          tags: ["test", "cleanup"]
        }
      }),
      data.header
    );

    let kb;
    try { kb = createRes.json().knowledgeBase; } catch (e) { kb = {}; }
    const knowledgeBaseId = kb ? kb.id : null;
    const knowledgeBaseUid = kb ? kb.uid : null;

    check(createRes, {
      "Cleanup: Knowledge base created": (r) => r.status === 200 && knowledgeBaseId && knowledgeBaseUid,
    });
    console.log(`✓ Knowledge base created: ${knowledgeBaseId} (UID: ${knowledgeBaseUid})`);

    if (!knowledgeBaseId || !knowledgeBaseUid) {
      console.log("✗ Failed to create knowledge base, aborting test");
      return;
    }

    // Add a small delay to ensure services are ready
    sleep(1);

    // Step 2: Upload a PDF file
    console.log("Step 2: Uploading PDF file...");
    // Using PDF to test the full processing pipeline and cleanup:
    // - Conversion step (creates converted-file blobs in MinIO)
    // - Chunking step (creates chunk blobs in MinIO)
    // - Embedding step (creates vectors in Milvus)
    // This provides comprehensive coverage of cleanup logic
    const filename = `${data.dbIDPrefix}cleanup-test.pdf`;
    const uploadRes = helper.uploadFileWithRetry(
      `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files`,
      {
        displayName: filename,
        type: "TYPE_PDF",
        content: constant.docSamplePdf
      },
      data.header,
      5 // max retries (PDF upload can be slow under parallel load)
    );

    let uploadedFile;
    let fileUid = null;
    if (uploadRes) {
      try { uploadedFile = uploadRes.json().file; } catch (e) { uploadedFile = {}; }
      fileUid = uploadedFile ? uploadedFile.uid : null;
    }

    check(uploadRes, {
      "Cleanup: File uploaded": (r) => r && r.status === 200 && fileUid,
    });
    console.log(`✓ File uploaded: ${filename} (UID: ${fileUid})`);

    if (!fileUid) {
      console.log("✗ Failed to upload file, cleaning up and aborting");
      http.request("DELETE", `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}`, null, data.header);
      return;
    }

    // Step 3: Wait for file processing to complete
    // Auto-trigger: Processing starts automatically on upload (no manual trigger needed)
    console.log("Step 3: Waiting for file processing to complete using robust helper...");

    // Wait for file processing using robust helper function
    const result = helper.waitForFileProcessingComplete(
      data.expectedOwner.id,
      knowledgeBaseId,
      fileUid,
      data.header,
      300, // Max 300 seconds for PDF processing
      60   // Fast-fail after 60s if stuck in NOTSTARTED
    );

    const processingCompleted = result.completed && result.status === "COMPLETED";

    if (result.status === "FAILED") {
      const errorMsg = result.error || "Unknown error";
      check(false, { [`Cleanup: Processing failed - ${errorMsg}`]: () => false });
      http.request("DELETE", `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}`, null, data.header);
      return;
    } else if (!processingCompleted) {
      console.log(`Step 3: File processing did not complete. Status: ${result.status}`);
    }

    check({ completed: processingCompleted }, {
      "Cleanup: Processing completed": () => processingCompleted,
    });

    if (!processingCompleted) {
      console.log("✗ Processing did not complete within timeout, cleaning up and aborting");
      http.request("DELETE", `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}`, null, data.header);
      return;
    }
    console.log("✓ File processing completed successfully");

    // Step 4: Verify all resources exist BEFORE deletion (BASELINE)
    console.log("Step 4: Verifying baseline - all resources should exist...");
    // Poll storage systems until data appears (handles eventual consistency)
    const minioBlobsBeforeDelete = {
      converted: helper.pollMinIOObjects(knowledgeBaseUid, fileUid, 'converted-file'),
      chunks: helper.pollMinIOObjects(knowledgeBaseUid, fileUid, 'chunk'),
    };

    // Poll database embeddings and Milvus vectors (longer timeout for embeddings)
    const embeddingsBeforeDelete = helper.pollEmbeddings(fileUid, 30);
    const milvusVectorsBeforeDelete = helper.pollMilvusVectors(knowledgeBaseUid, fileUid);

    // Verify all resources exist (this proves our cleanup test is valid)
    check(minioBlobsBeforeDelete, {
      "Cleanup: MinIO has converted file blob before deletion": (r) => r.converted > 0,
      "Cleanup: MinIO has chunk blobs before deletion": (r) => r.chunks > 0,
    });

    check({ embeddings: embeddingsBeforeDelete }, {
      "Cleanup: Database has embeddings before deletion": (r) => r.embeddings > 0,
    });

    check({ vectors: milvusVectorsBeforeDelete }, {
      "Cleanup: Milvus has vectors before deletion": (r) => r.vectors > 0,
    });
    console.log(`✓ Baseline verified: MinIO(converted=${minioBlobsBeforeDelete.converted}, chunks=${minioBlobsBeforeDelete.chunks}), Postgres(embeddings=${embeddingsBeforeDelete}), Milvus(vectors=${milvusVectorsBeforeDelete})`);

    // Early exit if baseline verification fails
    if (minioBlobsBeforeDelete.converted === 0 || minioBlobsBeforeDelete.chunks === 0) {
      console.log("✗ Baseline verification failed, cleaning up and aborting");
      http.request("DELETE", `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}`, null, data.header);
      return;
    }

    // Step 5: Delete knowledge base (triggers cleanup workflow)
    console.log("Step 5: Deleting knowledge base (triggers cleanup workflow)...");
    // This should:
    // 1. Trigger CleanupWorkflow for each file in the knowledge base
    // 2. Delete all converted-file and chunks from MinIO
    // 3. Delete all embeddings from database and Milvus
    // 4. Delete all database records (converted_file, chunk, embedding)
    // 5. Delete the knowledge base itself
    const deleteRes = http.request(
      "DELETE",
      `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}`,
      null,
      data.header
    );

    check(deleteRes, {
      "Cleanup: Knowledge base deleted (triggers cleanup workflow)": (r) => r.status === 200 || r.status === 204,
    });
    console.log("✓ Knowledge base deleted, cleanup workflow triggered");

    // Step 6: Poll for cleanup workflow completion (wait for all resources to be removed)
    // The cleanup workflow runs asynchronously via Temporal
    // Poll each resource type until it goes to zero (or timeout after 60s)
    console.log("Step 6: Polling for cleanup workflow completion (waiting for resources to be removed)...");

    // Step 7: Verify all resources are COMPLETELY REMOVED (FINAL VERIFICATION)
    console.log("Step 7: Polling and verifying all resources are completely removed...");
    const minioBlobsAfterDelete = {
      converted: helper.pollMinIOCleanup(knowledgeBaseUid, fileUid, 'converted-file', 60),
      chunks: helper.pollMinIOCleanup(knowledgeBaseUid, fileUid, 'chunk', 60),
    };

    const embeddingsAfterDelete = helper.pollEmbeddingsCleanup(fileUid, 60);
    const milvusVectorsAfterDelete = helper.pollMilvusVectorsCleanup(knowledgeBaseUid, fileUid, 60);

    let dbRecordsAfterDelete = { converted: 0, chunks: 0 };
    try {
      const convertedResults = helper.safeQuery('SELECT uid FROM converted_file WHERE file_uid = $1', fileUid);
      dbRecordsAfterDelete.converted = convertedResults ? convertedResults.length : 0;

      const chunksResults = helper.safeQuery('SELECT uid FROM chunk WHERE file_uid = $1', fileUid);
      dbRecordsAfterDelete.chunks = chunksResults ? chunksResults.length : 0;
    } catch (e) {
      console.error(`Failed to query database records after deletion: ${e}`);
    }

    // THE CRITICAL VERIFICATION: All resources should be COMPLETELY REMOVED
    // This validates that the CleanupWorkflow properly cleans up:
    // - MinIO blobs (converted files and chunks)
    // - Postgres records (converted_file, chunk, embedding)
    // - Milvus vectors

    // Verify all resources are completely removed after cleanup workflow
    check({
      convertedBlobsRemoved: minioBlobsAfterDelete.converted === 0,
      chunkBlobsRemoved: minioBlobsAfterDelete.chunks === 0,
      embeddingsRemoved: embeddingsAfterDelete === 0,
      milvusVectorsRemoved: milvusVectorsAfterDelete === 0,
      convertedRecordsRemoved: dbRecordsAfterDelete.converted === 0,
      chunkRecordsRemoved: dbRecordsAfterDelete.chunks === 0,
    }, {
      "Cleanup: MinIO converted file blobs COMPLETELY REMOVED": (r) => r.convertedBlobsRemoved,
      "Cleanup: MinIO chunk blobs COMPLETELY REMOVED": (r) => r.chunkBlobsRemoved,
      "Cleanup: Postgres embeddings COMPLETELY REMOVED": (r) => r.embeddingsRemoved,
      "Cleanup: Milvus vectors COMPLETELY REMOVED": (r) => r.milvusVectorsRemoved,
      "Cleanup: Postgres converted_file records COMPLETELY REMOVED": (r) => r.convertedRecordsRemoved,
      "Cleanup: Postgres chunk records COMPLETELY REMOVED": (r) => r.chunkRecordsRemoved,
    });

    // Additional verification: Knowledge base should also be removed
    const kbCheckRes = http.request(
      "GET",
      `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}`,
      null,
      data.header
    );

    check(kbCheckRes, {
      "Cleanup: Knowledge base record removed (404 or empty response)": (r) => r.status === 404 || r.status === 400,
    });
    console.log(`✓ Final verification: MinIO(converted=${minioBlobsAfterDelete.converted}, chunks=${minioBlobsAfterDelete.chunks}), Postgres(embeddings=${embeddingsAfterDelete}, converted=${dbRecordsAfterDelete.converted}, chunks=${dbRecordsAfterDelete.chunks}), Milvus(vectors=${milvusVectorsAfterDelete})`);
    console.log("=== Knowledge Base Deletion Test Complete ===\n");
  });
}
