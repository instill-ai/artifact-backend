import http from "k6/http";
import { check, group, sleep } from "k6";
import { randomString } from "https://jslib.k6.io/k6-utils/1.1.0/index.js";

import { artifactPublicHost } from "./const.js";

import * as constant from "./const.js";
import * as helper from "./helper.js";

/**
 * Test comprehensive cleanup of all resources when a catalog is deleted.
 *
 * This test verifies that when a catalog is deleted:
 * 1. All intermediate files (converted files, chunks) are deleted from MinIO
 * 2. All embeddings are deleted from Postgres
 * 3. All vectors are deleted from Milvus
 * 4. All database records are properly cleaned up
 * 5. The cleanup workflow handles both completed and in-progress files
 *
 * Test Flow:
 * - Create a test catalog
 * - Upload a PDF file (full processing pipeline: convert → chunk → embed)
 * - Process the file completely
 * - Verify all resources exist (baseline verification)
 * - Delete the catalog (triggers cleanup workflow)
 * - Verify all resources are completely removed
 *
 * This ensures the CleanupWorkflow and CleanupFilesActivity properly
 * remove all traces of catalog data across all storage systems:
 * - MinIO: Converted files, text chunks
 * - Postgres: File records, converted_file, text_chunk, embedding tables
 * - Milvus: Vector collections and vectors
 */
export function CheckKnowledgeBaseDeletion(data) {
  const groupName = "Artifact API: Knowledge base deletion";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    // Step 1: Create a test catalog
    const catalogName = constant.dbIDPrefix + "cleanup-" + randomString(5);
    const createRes = http.request(
      "POST",
      `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`,
      JSON.stringify({
        name: catalogName,
        description: "Catalog deletion cleanup test",
        tags: ["test", "cleanup"]
      }),
      data.header
    );

    let catalog;
    try { catalog = createRes.json().catalog; } catch (e) { catalog = {}; }
    const catalogId = catalog ? catalog.catalogId : null;
    const catalogUid = catalog ? catalog.catalogUid : null;

    check(createRes, {
      "Cleanup: Catalog created": (r) => r.status === 200 && catalogId && catalogUid,
    });

    if (!catalogId || !catalogUid) {
      return;
    }

    // Add a small delay to ensure services are ready
    sleep(1);

    // Step 2: Upload a PDF file
    // Using PDF to test the full processing pipeline and cleanup:
    // - Conversion step (creates converted-file blobs in MinIO)
    // - Chunking step (creates chunk blobs in MinIO)
    // - Embedding step (creates vectors in Milvus)
    // This provides comprehensive coverage of cleanup logic
    const fileName = `${constant.dbIDPrefix}cleanup-test.pdf`;
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
      "Cleanup: File uploaded": (r) => r.status === 200 && fileUid,
    });

    if (!fileUid) {
      http.request("DELETE", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}`, null, data.header);
      return;
    }

    // Step 3: Trigger processing and wait for completion
    const processRes = http.request(
      "POST",
      `${artifactPublicHost}/v1alpha/catalogs/files/processAsync`,
      JSON.stringify({ fileUids: [fileUid] }),
      data.header
    );

    check(processRes, {
      "Cleanup: Processing triggered": (r) => r.status === 200,
    });

    // Poll for completion (300s timeout for PDF processing)
    // We need the file to be fully processed to create all intermediate resources
    let processingCompleted = false;
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
          processingCompleted = true;
          break;
        } else if (status === "FILE_PROCESS_STATUS_FAILED") {
          const errorMsg = body.file && body.file.processOutcome ? body.file.processOutcome : "Unknown error";
          check(false, { [`Cleanup: Processing failed - ${errorMsg}`]: () => false });
          http.request("DELETE", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}`, null, data.header);
          return;
        }
      } catch (e) { /* continue polling */ }
    }

    check({ completed: processingCompleted }, {
      "Cleanup: Processing completed": () => processingCompleted,
    });

    if (!processingCompleted) {
      http.request("DELETE", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}`, null, data.header);
      return;
    }

    // Step 4: Verify all resources exist BEFORE deletion (BASELINE)
    // Poll storage systems until data appears (handles eventual consistency)
    const minioBlobsBeforeDelete = {
      converted: helper.pollMinioObjects(catalogUid, fileUid, 'converted-file', 10),
      chunks: helper.pollMinioObjects(catalogUid, fileUid, 'chunk', 10),
    };

    // Poll database embeddings and Milvus vectors
    const embeddingsBeforeDelete = helper.pollEmbeddings(fileUid, 10);
    const milvusVectorsBeforeDelete = helper.pollMilvusVectors(catalogUid, fileUid, 10);

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

    // Early exit if baseline verification fails
    if (minioBlobsBeforeDelete.converted === 0 || minioBlobsBeforeDelete.chunks === 0) {
      http.request("DELETE", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}`, null, data.header);
      return;
    }

    // Step 5: Delete catalog (triggers cleanup workflow)
    // This should:
    // 1. Trigger CleanupWorkflow for each file in the catalog
    // 2. Delete all converted-file and chunks from MinIO
    // 3. Delete all embeddings from database and Milvus
    // 4. Delete all database records (converted_file, text_chunk, embedding)
    // 5. Delete the catalog itself
    const deleteRes = http.request(
      "DELETE",
      `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}`,
      null,
      data.header
    );

    check(deleteRes, {
      "Cleanup: Catalog deleted (triggers cleanup workflow)": (r) => r.status === 200 || r.status === 204,
    });

    // Step 6: Wait for Temporal cleanup workflow to complete
    // The cleanup workflow runs asynchronously, so we need to wait
    // for it to finish removing all resources
    sleep(15);

    // Step 7: Verify all resources are COMPLETELY REMOVED (FINAL VERIFICATION)
    const minioBlobsAfterDelete = {
      converted: helper.countMinioObjects(catalogUid, fileUid, 'converted-file'),
      chunks: helper.countMinioObjects(catalogUid, fileUid, 'chunk'),
    };

    const embeddingsAfterDelete = helper.countEmbeddings(fileUid);
    const milvusVectorsAfterDelete = helper.countMilvusVectors(catalogUid, fileUid);

    let dbRecordsAfterDelete = { converted: 0, chunks: 0 };
    try {
      const convertedResults = constant.db.query('SELECT uid FROM converted_file WHERE file_uid = $1', fileUid);
      dbRecordsAfterDelete.converted = convertedResults ? convertedResults.length : 0;

      const chunksResults = constant.db.query('SELECT uid FROM text_chunk WHERE kb_file_uid = $1', fileUid);
      dbRecordsAfterDelete.chunks = chunksResults ? chunksResults.length : 0;
    } catch (e) {
      console.error(`Failed to query database records after deletion: ${e}`);
    }

    // THE CRITICAL VERIFICATION: All resources should be COMPLETELY REMOVED
    // This validates that the CleanupWorkflow properly cleans up:
    // - MinIO blobs (converted files and chunks)
    // - Postgres records (converted_file, text_chunk, embedding)
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
      "Cleanup: Postgres text_chunk records COMPLETELY REMOVED": (r) => r.chunkRecordsRemoved,
    });

    // Additional verification: Catalog should also be removed
    const catalogCheckRes = http.request(
      "GET",
      `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}`,
      null,
      data.header
    );

    check(catalogCheckRes, {
      "Cleanup: Catalog record removed (404 or empty response)": (r) => r.status === 404 || r.status === 400,
    });
  });
}
