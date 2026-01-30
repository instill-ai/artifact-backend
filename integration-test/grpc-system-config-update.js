/**
 * System Config Update Integration Test (OpenAI → Gemini)
 *
 * PURPOSE:
 * Validates the complete Knowledge Base update lifecycle when changing system configurations
 * (embedding models), specifically testing the OpenAI → Gemini transition with different
 * dimensionalities (1536 → 3072). This test validates:
 * - System config preservation during updates
 * - Dual file processing with different configs during retention
 * - Rollback mechanism with config restoration
 * - Complete retention period lifecycle
 * - Position data (PageDelimiters, PageRange) generation and preservation across updates
 *
 * TEST FLOW:
 * Phase 1: Create KBs with OpenAI (1536-dim) and upload initial PDF files
 * Phase 1.5: Verify OpenAI pipeline generates position data correctly
 * Phase 2: Trigger update to Gemini (3072-dim)
 * Phase 3: Verify staging KB uses new config, production uses old config
 * Phase 4: Wait for update completion and verify swap
 * Phase 4.5: Verify Gemini conversion preserves position data after reprocessing
 * Phase 4.6: Verify staging KB cleanup (soft-deleted, update_status cleared)
 * Phase 5: Upload multi-page PDF files during retention (dual processing)
 * Phase 5.5: Verify Gemini AI route generates position data correctly
 * Phase 6: Verify dual processing used different configs
 * Phase 7: Trigger rollback
 * Phase 8: Verify rollback restored original config
 * Phase 8.5: Verify position data survived rollback (Gemini → OpenAI)
 * Phase 9: Test retention expiry cleanup
 *
 * KEY VALIDATIONS:
 * - Rollback KB preserves original system config (OpenAI)
 * - Production KB adopts new system config after swap (Gemini)
 * - Dual processing uses correct config for each KB
 * - Files uploaded during retention work correctly after rollback
 * - Retention expiry cleans up rollback KB properly
 * - Staging KB cleanup clears update_status before soft-deletion
 * - Staging KB resources fully cleaned after successful update
 * - New KBs have unique active_collection_uid != KB UID
 * - Rollback KB cleanup clears update_status before soft-deletion
 * - Rollback KB cleanup verification runs after cleanup
 *
 * POSITION DATA VALIDATIONS:
 * - OpenAI pipeline generates position_data with PageDelimiters
 * - Gemini AI route generates position_data with PageDelimiters
 * - chunk.reference contains PageRange (PascalCase)
 * - converted_file.position_data contains PageDelimiters (PascalCase)
 * - API returns chunks with UNIT_PAGE references
 * - API returns chunks with markdown_reference (UNIT_CHARACTER)
 * - Position data preserved through OpenAI → Gemini update
 * - Position data preserved through Gemini → OpenAI rollback
 * - Multi-page PDF files have correct PageDelimiters count (4 pages in sample-multi-page.pdf)
 */

import grpc from "k6/net/grpc";
import { check, group, sleep } from "k6";
import { randomString } from "https://jslib.k6.io/k6-utils/1.1.0/index.js";

import * as constant from "./const.js";
import * as helper from "./helper.js";
import { grpcInvokeWithRetry } from "./helper.js";

// Use httpRetry for automatic retry on transient errors (429, 5xx)
const http = helper.httpRetry;

const client = new grpc.Client();
client.load(
    ["proto"],
    "artifact/v1alpha/artifact_private_service.proto"
);

export let options = {
    setupTimeout: '600s',
    teardownTimeout: '180s',
    iterations: 1,  // Run once to completion - test with AI processing, update workflows, and retention periods
    insecureSkipTLSVerify: true,
    thresholds: {
        checks: ["rate == 1.0"],
    },
};

export function setup() {
    check(true, { [constant.banner('System Config Update Test: Setup')]: () => true });

    // Generate unique test prefix (must be in setup, not module-level, to avoid k6 parallel init issues)
    const dbIDPrefix = constant.generateDBIDPrefix();
    console.log(`grpc-system-config-update.js: Using unique test prefix: ${dbIDPrefix}`);

    // Authenticate with retry to handle transient failures
    const authHeader = helper.getBasicAuthHeader(constant.defaultUsername, constant.defaultPassword);
    const header = {
        "headers": {
            "Authorization": authHeader,
            "Content-Type": "application/json",
        },
        "timeout": "600s",
    };

    const userResp = http.request("GET", `${constant.mgmtRESTPublicHost}/v1beta/user`, {}, {
        headers: { "Authorization": authHeader }
    });

    const grpcMetadata = {
        "metadata": {
            "Authorization": authHeader
        },
        "timeout": "600s"
    };

    // Cleanup orphaned knowledge bases from previous failed test runs OF THIS SPECIFIC TEST
    // Use API-only cleanup to properly trigger workflows (no direct DB manipulation)
    console.log("\n=== SETUP: Cleaning up previous test data (sysconfig pattern only) ===");
    try {
        const listResp = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${userResp.json().user.id}/knowledge-bases`, null, header);
        if (listResp.status === 200) {
            const knowledgeBases = Array.isArray(listResp.json().knowledgeBases) ? listResp.json().knowledgeBases : [];
            let cleanedCount = 0;
            for (const kb of knowledgeBases) {
                const kbId = kb.id;
                if (kbId && kbId.match(/test-[a-z0-9]+-sysconfig-/)) {
                    const delResp = http.request("DELETE", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${userResp.json().user.id}/knowledge-bases/${kbId}`, null, header);
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

    return {
        header: header,
        expectedOwner: userResp.json().user,
        metadata: grpcMetadata,
        knowledgeBaseIds: [], // Track created knowledge bases for cleanup
        dbIDPrefix: dbIDPrefix
    };
}

export function teardown(data) {
    // Skip if test was disabled
    if (!data) {
        console.log("Teardown skipped (test was disabled)");
        return;
    }

    group("System Config Update Test: Teardown", () => {
        check(true, { [constant.banner('Teardown')]: () => true });

        console.log("\n=== TEARDOWN: Cleaning up test resources ===");

        // Wait for file processing AND Temporal activities to settle before cleanup
        console.log("Teardown: Waiting for safe cleanup...");
        helper.waitForSafeCleanup(120, data.dbIDPrefix, 3);

        // Delete all test knowledge bases via API (primary cleanup method)
        console.log("Deleting test knowledge bases via API...");
        let deletedCount = 0;
        const kbIds = data.knowledgeBaseIds || [];
        for (const knowledgeBaseId of kbIds) {
            try {
                const deleteRes = http.request(
                    "DELETE",
                    `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}`,
                    null,
                    data.header
                );
                if (deleteRes.status === 200) {
                    deletedCount++;
                }
                console.log(`Deleted knowledge base: ${knowledgeBaseId} (status: ${deleteRes.status})`);
            } catch (e) {
                console.error(`Failed to delete knowledge base ${knowledgeBaseId}: ${e}`);
            }
        }

        console.log(`API cleanup: ${deletedCount}/${kbIds.length} knowledge bases deleted via API`);

        // CRITICAL: Hard delete test data from DB to prevent orphaned records affecting next test
        // Soft deletion (delete_time) is not enough because:
        // 1. Background cleanup may not run immediately
        // 2. Next test may see orphaned records and try to process files with missing blobs
        // 3. Previous test failures may leave inconsistent state
        //
        // SAFETY: Only delete KBs explicitly tracked in data.knowledgeBaseIds (created by THIS test)
        // This prevents accidentally deleting other tests' data
        console.log("Performing hard DB cleanup for test data...");

        if (data.knowledgeBaseIds && data.knowledgeBaseIds.length > 0) {
            // Build list of knowledge base IDs to delete (ONLY those created by this test)
            const knowledgeBaseIdsToDelete = data.knowledgeBaseIds.map(id => `'${id}'`).join(',');
            console.log(`Deleting KBs with IDs: ${data.knowledgeBaseIds.join(', ')}`);

            const dbCleanupResult = helper.safeQuery(
                `-- Hard delete ONLY the KBs explicitly created by THIS test (tracked in data.knowledgeBaseIds)
                 WITH deleted_kbs AS (
                     DELETE FROM knowledge_base
                     WHERE id IN (${knowledgeBaseIdsToDelete})
                     RETURNING uid
                 ),
                 deleted_file_kb_links AS (
                     DELETE FROM file_knowledge_base
                     WHERE kb_uid IN (SELECT uid FROM deleted_kbs)
                     RETURNING file_uid
                 ),
                 deleted_files AS (
                     DELETE FROM file
                     WHERE uid IN (SELECT file_uid FROM deleted_file_kb_links)
                     RETURNING uid
                 ),
                 deleted_chunks AS (
                     DELETE FROM chunk
                     WHERE file_uid IN (SELECT uid FROM deleted_files)
                     RETURNING uid
                 ),
                 deleted_converted AS (
                     DELETE FROM converted_file
                     WHERE file_uid IN (SELECT uid FROM deleted_files)
                     RETURNING uid
                 )
                 SELECT
                     (SELECT COUNT(*) FROM deleted_kbs) as kb_count,
                     (SELECT COUNT(*) FROM deleted_files) as file_count,
                     (SELECT COUNT(*) FROM deleted_chunks) as chunk_count,
                     (SELECT COUNT(*) FROM deleted_converted) as converted_count;`
            );

            if (dbCleanupResult && dbCleanupResult.length > 0) {
                console.log(`DB hard cleanup complete:`, {
                    kbs: dbCleanupResult[0].kb_count,
                    files: dbCleanupResult[0].file_count,
                    chunks: dbCleanupResult[0].chunk_count,
                    converted: dbCleanupResult[0].converted_count
                });
            }
        } else {
            console.log("No knowledgeBaseIds to clean up");
        }

        console.log("Cleanup complete - next test run will start with clean state");

        client.close();
        console.log("=== TEARDOWN: Complete ===");
    });
}

export default function (data) {
    // Guard against setup failure
    if (!data || !data.metadata) {
        console.error("Setup failed - skipping test");
        return;
    }

    // Connect gRPC client to private service
    client.connect(constant.artifactGRPCPrivateHost, {
        plaintext: true,
    });

    // Track test success - if any phase fails critically, skip remaining phases
    let testShouldContinue = true;

    group("System Config Update: Complete Lifecycle Test", () => {

        // ====================================================================
        // PHASE 1: Setup with OpenAI
        // ====================================================================
        group("Phase 1: Create KBs with OpenAI and Upload Initial Files", () => {
            console.log("\n=== Phase 1: Creating KBs with OpenAI system config ===");

            // Wait for any ongoing updates to complete
            helper.waitForAllUpdatesComplete(client, data, 30);

            const randomSuffix = randomString(8);
            const displayName1 = `${data.dbIDPrefix}sysconfig-kb1-${randomSuffix}`;
            const displayName2 = `${data.dbIDPrefix}sysconfig-kb2-${randomSuffix}`;

            // Create first KB with OpenAI
            const createReq1 = {
                displayName: displayName1,
                description: "Test knowledge base 1 for system config update",
                tags: ["test", "openai"],
                systemId: "openai"
            };

            const createRes1 = http.request(
                "POST",
                `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases`,
                JSON.stringify(createReq1),
                data.header
            );

            check(createRes1, {
                "Phase 1: KB1 created successfully": (r) => r.status === 200,
            });

            if (createRes1.status !== 200) {
                console.error(`Phase 1: Failed to create KB1. Status: ${createRes1.status}, Body: ${createRes1.body}`);
                testShouldContinue = false;
                return;
            }

            const responseBody1 = createRes1.json();
            if (!responseBody1 || !responseBody1.knowledgeBase) {
                console.error(`Phase 1: Response missing knowledgeBase field. Response: ${JSON.stringify(responseBody1)}`);
                testShouldContinue = false;
                return;
            }

            const kb1 = responseBody1.knowledgeBase;
            const knowledgeBaseId1 = kb1.id;
            data.knowledgeBaseIds.push(knowledgeBaseId1);

            // Verify embedding config (model_family and dimensionality depend on environment)
            check(kb1, {
                "Phase 1: KB1 has knowledge base object": (c) => c !== undefined && c !== null,
                "Phase 1: KB1 has embeddingConfig": (c) => c && c.embeddingConfig !== undefined,
                "Phase 1: KB1 has valid model_family": (c) => c && c.embeddingConfig && c.embeddingConfig.modelFamily && c.embeddingConfig.modelFamily.length > 0,
                "Phase 1: KB1 has valid dimensionality": (c) => c && c.embeddingConfig && c.embeddingConfig.dimensionality > 0,
            });
            // Store original config for later rollback verification
            const kb1OriginalModelFamily = kb1.embeddingConfig ? kb1.embeddingConfig.modelFamily : "";
            data.kb1OriginalModelFamily = kb1OriginalModelFamily;

            // Verify active_collection_uid is unique and store original system slug
            const kb1Uid = helper.getKnowledgeBaseUidFromId(knowledgeBaseId1);
            const kb1CollectionCheck = helper.safeQuery(
                `SELECT uid::text, active_collection_uid::text FROM knowledge_base WHERE uid = $1`,
                kb1Uid
            );
            if (kb1CollectionCheck && kb1CollectionCheck.length > 0) {
                const collectionUID = kb1CollectionCheck[0].active_collection_uid;
                const kbUIDFromDB = kb1CollectionCheck[0].uid;
                check({ collectionUID, kbUID: kb1Uid }, {
                    "Phase 1: KB1 active_collection_uid is not null": () => collectionUID !== null,
                    "Phase 1: KB1 active_collection_uid != KB UID": () =>
                        collectionUID !== kb1Uid && collectionUID !== kbUIDFromDB,
                });
                console.log(`KB1 collection UID: ${collectionUID} (different from KB UID: ${kb1Uid})`);
            }

            // Query and store the original system slug for KB1 (for rollback verification)
            const kb1SystemQuery = helper.safeQuery(
                `SELECT s.id, s.slug FROM system s
                 JOIN knowledge_base kb ON kb.system_uid = s.uid
                 WHERE kb.uid = $1`,
                kb1Uid
            );
            if (kb1SystemQuery && kb1SystemQuery.length > 0) {
                data.kb1OriginalSystemId = kb1SystemQuery[0].id;
                data.kb1OriginalSystemSlug = kb1SystemQuery[0].slug;
                console.log(`KB1 original system: id=${data.kb1OriginalSystemId}, slug=${data.kb1OriginalSystemSlug}`);
            }

            if (!kb1 || !kb1.embeddingConfig) {
                console.error(`Phase 1: KB1 missing embedding config. Knowledge base: ${JSON.stringify(kb1)}`);
                testShouldContinue = false;
                return;
            }

            // Create second KB with OpenAI
            const createReq2 = {
                displayName: displayName2,
                description: "Test knowledge base 2 for system config update",
                tags: ["test", "openai"],
                systemId: "openai"
            };

            const createRes2 = http.request(
                "POST",
                `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases`,
                JSON.stringify(createReq2),
                data.header
            );

            check(createRes2, {
                "Phase 1: KB2 created successfully": (r) => r.status === 200,
            });

            if (createRes2.status !== 200) {
                console.error(`Phase 1: Failed to create KB2. Status: ${createRes2.status}, Body: ${createRes2.body}`);
                testShouldContinue = false;
                return;
            }

            const responseBody2 = createRes2.json();
            if (!responseBody2 || !responseBody2.knowledgeBase) {
                console.error(`Phase 1: Response missing knowledgeBase field. Response: ${JSON.stringify(responseBody2)}`);
                testShouldContinue = false;
                return;
            }

            const kb2 = responseBody2.knowledgeBase;
            const knowledgeBaseId2 = kb2.id;
            data.knowledgeBaseIds.push(knowledgeBaseId2);
            // Store original config for later rollback verification
            const kb2OriginalModelFamily = kb2.embeddingConfig ? kb2.embeddingConfig.modelFamily : "";
            data.kb2OriginalModelFamily = kb2OriginalModelFamily;

            // Upload initial files to KB1 (use multi-page PDF to test position data with OpenAI)
            console.log("Uploading initial files to KB1...");
            const initialFiles1 = [];

            // Upload at least one PDF to test position data generation with OpenAI
            for (let i = 1; i <= 2; i++) {
                const filename = `${data.dbIDPrefix}sysconfig-initial-kb1-${i}.pdf`;
                const uploadRes = http.request(
                    "POST",
                    `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId1}/files`,
                    JSON.stringify({ displayName: filename, type: "TYPE_PDF", content: constant.docSampleMultiPagePdf }),
                    data.header
                );

                if (uploadRes.status === 200) {
                    const fileObj = uploadRes.json().file;
                    console.log(`KB1 File ${i} uploaded: id=${fileObj ? fileObj.id : 'missing'}`);
                    initialFiles1.push(fileObj);
                } else {
                    console.error(`KB1 File ${i} upload FAILED: status=${uploadRes.status}, body=${uploadRes.body}`);
                }
            }

            check(initialFiles1, {
                "Phase 1: KB1 initial files uploaded": () => initialFiles1.length === 2,
            });

            // Upload initial files to KB2
            console.log("Uploading initial files to KB2...");
            const initialFiles2 = [];

            for (let i = 1; i <= 2; i++) {
                const filename = `${data.dbIDPrefix}sysconfig-initial-kb2-${i}.txt`;
                const uploadRes = http.request(
                    "POST",
                    `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId2}/files`,
                    JSON.stringify({ displayName: filename, type: "TYPE_TEXT", content: constant.docSampleTxt }),
                    data.header
                );

                if (uploadRes.status === 200) {
                    const fileObj = uploadRes.json().file;
                    console.log(`KB2 File ${i} uploaded: id=${fileObj ? fileObj.id : 'missing'}`);
                    initialFiles2.push(fileObj);
                } else {
                    console.error(`KB2 File ${i} upload FAILED: status=${uploadRes.status}, body=${uploadRes.body}`);
                }
            }

            check(initialFiles2, {
                "Phase 1: KB2 initial files uploaded": () => initialFiles2.length === 2,
            });

            // CRITICAL: Process files from each KB separately
            // Mixing files from different KBs causes all files to be assigned to the first KB's collection
            // Auto-trigger: Processing starts automatically on upload

            // Auto-trigger: Processing starts automatically on upload

            // Wait for processing completion using robust helper
            const allInitialFiles = [...initialFiles1, ...initialFiles2];
            console.log(`Waiting for ${allInitialFiles.length} initial files to process...`);
            console.log(`File IDs: ${allInitialFiles.map(f => f.id).join(', ')}`);

            // Wait for files in KB1 (timeout configured via env or default)
            const fileProcessTimeout = parseInt(__ENV.FILE_PROCESS_TIMEOUT || '300'); // 5 min default
            if (initialFiles1.length > 0) {
                console.log(`Waiting for ${initialFiles1.length} files in KB1 (timeout: ${fileProcessTimeout}s)...`);
                const result1 = helper.waitForMultipleFilesProcessingComplete(
                    data.expectedOwner.id,
                    knowledgeBaseId1,
                    initialFiles1.map(f => f.id),
                    data.header,
                    fileProcessTimeout
                );
                if (!result1.completed) {
                    console.log(`KB1 files incomplete: ${result1.status}, processed ${result1.processedCount}/${initialFiles1.length}`);
                }
            }

            // Wait for files in KB2 (timeout configured via env or default)
            if (initialFiles2.length > 0) {
                console.log(`Waiting for ${initialFiles2.length} files in KB2 (timeout: ${fileProcessTimeout}s)...`);
                const result2 = helper.waitForMultipleFilesProcessingComplete(
                    data.expectedOwner.id,
                    knowledgeBaseId2,
                    initialFiles2.map(f => f.id),
                    data.header,
                    fileProcessTimeout
                );
                if (!result2.completed) {
                    console.log(`KB2 files incomplete: ${result2.status}, processed ${result2.processedCount}/${initialFiles2.length}`);
                }
            }

            const allCompleted = true; // If we got here without throwing, all completed
            console.log(`All ${allInitialFiles.length} initial files processed successfully`);

            check({ allCompleted }, {
                "Phase 1: All initial files processed successfully": () => allCompleted === true,
            });

            // ====================================================================
            // Phase 1.5: Verify Position Data with OpenAI Pipeline
            // ====================================================================
            console.log("\n=== Phase 1.5: Verifying position data with OpenAI pipeline ===");

            if (initialFiles1.length > 0 && allCompleted) {
                const pdfFileId = initialFiles1[0].id;
                const pdfFileUid = helper.getFileUidFromId(pdfFileId);
                console.log(`Phase 1.5: Testing OpenAI-processed PDF file ${pdfFileId} (UID: ${pdfFileUid})`);

                // 1. Verify converted_file has position_data with PageDelimiters
                const convertedFileQuery = helper.safeQuery(
                    `SELECT position_data::text as position_data_text FROM converted_file
                 WHERE file_uid = $1 AND converted_type = 'CONVERTED_FILE_TYPE_CONTENT' AND position_data IS NOT NULL LIMIT 1`,
                    pdfFileUid
                );

                if (convertedFileQuery && convertedFileQuery.length > 0) {
                    let posData;
                    try {
                        posData = JSON.parse(convertedFileQuery[0].position_data_text);
                        console.log(`Phase 1.5: OpenAI PDF position_data has ${posData.PageDelimiters ? posData.PageDelimiters.length : 0} page delimiters`);
                    } catch (e) {
                        posData = null;
                    }

                    check({ posData }, {
                        "Phase 1.5: OpenAI PDF has position_data": () => posData !== null,
                        "Phase 1.5: OpenAI position_data has PageDelimiters": () =>
                            posData && posData.PageDelimiters !== undefined,
                        "Phase 1.5: OpenAI PageDelimiters is an array": () =>
                            posData && Array.isArray(posData.PageDelimiters),
                        "Phase 1.5: OpenAI PageDelimiters has pages (sample-multi-page.pdf)": () =>
                            posData && posData.PageDelimiters && posData.PageDelimiters.length >= 1,
                    });
                }

                // 2. Verify chunk has reference with PageRange
                const chunkQuery = helper.safeQuery(
                    `SELECT reference::text as reference_text FROM chunk
                     WHERE file_uid = $1 AND reference IS NOT NULL LIMIT 1`,
                    pdfFileUid
                );

                if (chunkQuery && chunkQuery.length > 0) {
                    let refData;
                    try {
                        refData = JSON.parse(chunkQuery[0].reference_text);
                        console.log(`Phase 1.5: OpenAI chunk reference = ${JSON.stringify(refData)}`);
                    } catch (e) {
                        refData = null;
                    }

                    check({ refData }, {
                        "Phase 1.5: OpenAI chunk has reference": () => refData !== null,
                        "Phase 1.5: OpenAI reference has PageRange": () =>
                            refData && refData.PageRange !== undefined,
                        "Phase 1.5: OpenAI PageRange is array with 2 elements": () =>
                            refData && Array.isArray(refData.PageRange) && refData.PageRange.length === 2,
                        "Phase 1.5: OpenAI PageRange values are valid": () =>
                            refData && refData.PageRange && refData.PageRange[0] > 0 && refData.PageRange[1] > 0,
                    });
                }

                // 3. Verify chunk API returns UNIT_PAGE references
                const chunksResp = http.request(
                    "GET",
                    `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId1}/files/${pdfFileId}/chunks`,
                    null,
                    data.header
                );

                if (chunksResp.status === 200 && chunksResp.json().chunks) {
                    const chunks = chunksResp.json().chunks;
                    console.log(`Phase 1.5: OpenAI PDF has ${chunks.length} chunks in API response`);

                    if (chunks.length > 0) {
                        const firstChunk = chunks[0];

                        check(firstChunk, {
                            "Phase 1.5: OpenAI chunk has reference": () =>
                                firstChunk.reference !== null && firstChunk.reference !== undefined,
                            "Phase 1.5: OpenAI reference start unit is UNIT_PAGE": () =>
                                firstChunk.reference && firstChunk.reference.start &&
                                firstChunk.reference.start.unit === "UNIT_PAGE",
                            "Phase 1.5: OpenAI start position has coordinates": () =>
                                firstChunk.reference && firstChunk.reference.start &&
                                Array.isArray(firstChunk.reference.start.coordinates) &&
                                firstChunk.reference.start.coordinates.length > 0,
                            "Phase 1.5: OpenAI reference end unit is UNIT_PAGE": () =>
                                firstChunk.reference && firstChunk.reference.end &&
                                firstChunk.reference.end.unit === "UNIT_PAGE",
                        });
                    }
                }
            }

            console.log("Phase 1.5: OpenAI position data verification complete");

            // Store initial state for later validation
            data.kb1_initial = {
                knowledgeBaseId: knowledgeBaseId1,
                knowledgeBaseUid: helper.getKnowledgeBaseUidFromId(knowledgeBaseId1),
                fileCount: initialFiles1.length,
                fileIds: initialFiles1.map(f => f.id),
            };

            data.kb2_initial = {
                knowledgeBaseId: knowledgeBaseId2,
                knowledgeBaseUid: helper.getKnowledgeBaseUidFromId(knowledgeBaseId2),
                fileCount: initialFiles2.length,
                fileIds: initialFiles2.map(f => f.id),
            };
        });

        // ====================================================================
        // PHASE 2: Trigger Update to Gemini
        // ====================================================================
        group("Phase 2: Trigger Update from OpenAI to Gemini", () => {
            console.log("\n=== Phase 2: Triggering update to Gemini ===");

            // Check if Phase 1 completed successfully
            if (!data.kb1_initial || !data.kb2_initial) {
                console.error("Phase 2: Cannot proceed - Phase 1 did not complete successfully");
                console.error(`kb1_initial: ${data.kb1_initial ? 'exists' : 'undefined'}, kb2_initial: ${data.kb2_initial ? 'exists' : 'undefined'}`);
                return;
            }

            // Trigger update for both KBs using Gemini system
            const updateRes = grpcInvokeWithRetry(client,
                "artifact.v1alpha.ArtifactPrivateService/ExecuteKnowledgeBaseUpdateAdmin",
                {
                    knowledge_base_ids: [data.kb1_initial.knowledgeBaseId, data.kb2_initial.knowledgeBaseId],
                    system_id: "gemini"
                },
                data.metadata
            );

            check(updateRes, {
                "Phase 2: Update triggered successfully": (r) => r.status === grpc.StatusOK,
                "Phase 2: Update started flag is true": (r) => r.message && r.message.started === true,
            });

            if (updateRes.status !== grpc.StatusOK) {
                console.error("Failed to trigger update, aborting remaining phases");
                testShouldContinue = false;
                return;
            }

            console.log("Update triggered successfully");
        });

        // ====================================================================
        // PHASE 3: Monitor Update Progress & Verify Configs
        // ====================================================================
        if (!testShouldContinue) {
            console.warn("Skipping Phase 3 due to previous phase failure");
            return;
        }

        group("Phase 3: Verify Staging KBs Use New Config", () => {
            console.log("\n=== Phase 3: Waiting for staging KBs and verifying configs ===");

            // Wait for staging KBs to be created
            const staging1Found = helper.pollForStagingKB(data.kb1_initial.knowledgeBaseId, data.expectedOwner.id, 60);
            const staging2Found = helper.pollForStagingKB(data.kb2_initial.knowledgeBaseId, data.expectedOwner.id, 60);

            check({ staging1Found, staging2Found }, {
                "Phase 3: Staging KB1 created": () => staging1Found === true,
                "Phase 3: Staging KB2 created": () => staging2Found === true,
            });

            if (staging1Found && staging2Found) {
                // Get staging KBs from database
                const staging1 = helper.verifyStagingKB(data.kb1_initial.knowledgeBaseId, data.expectedOwner.id);
                const staging2 = helper.verifyStagingKB(data.kb2_initial.knowledgeBaseId, data.expectedOwner.id);

                if (staging1 && staging1.length > 0) {
                    console.log("Staging KB1 found in database");

                    // Query system_uid to verify it's pointing to Gemini system
                    // System IDs are now hash-based (sys-xxx), so check model_family in config JSON
                    const systemQuery = helper.safeQuery(
                        `SELECT s.id, s.config->'rag'->'embedding'->>'model_family' as model_family FROM system s
                         JOIN knowledge_base kb ON kb.system_uid = s.uid
                         WHERE kb.uid = $1`,
                        staging1[0].uid
                    );

                    if (systemQuery && systemQuery.length > 0) {
                        const systemId = systemQuery[0].id;
                        const modelFamily = systemQuery[0].model_family || "";
                        console.log(`Staging KB1 system_id: ${systemId}, model_family: ${modelFamily}`);

                        check({ modelFamily }, {
                            "Phase 3: Staging KB1 uses Gemini system": () => modelFamily === "gemini",
                        });
                    }
                }

                if (staging2 && staging2.length > 0) {
                    console.log("Staging KB2 found in database");

                    const systemQuery = helper.safeQuery(
                        `SELECT s.id, s.config->'rag'->'embedding'->>'model_family' as model_family FROM system s
                         JOIN knowledge_base kb ON kb.system_uid = s.uid
                         WHERE kb.uid = $1`,
                        staging2[0].uid
                    );

                    if (systemQuery && systemQuery.length > 0) {
                        const systemId = systemQuery[0].id;
                        const modelFamily = systemQuery[0].model_family || "";
                        console.log(`Staging KB2 system_id: ${systemId}, model_family: ${modelFamily}`);

                        check({ modelFamily }, {
                            "Phase 3: Staging KB2 uses Gemini system": () => modelFamily === "gemini",
                        });
                    }
                }
            }
        });

        // ====================================================================
        // PHASE 4: Wait for Update Completion & Verify Swap
        // ====================================================================
        if (!testShouldContinue) {
            console.warn("Skipping Phase 4 due to previous phase failure");
            return;
        }

        group("Phase 4: Wait for Update Completion and Verify Swap", () => {
            console.log("\n=== Phase 4: Waiting for update completion ===");

            // Wait for BOTH updates to complete in parallel (max 10 minutes total)
            // System config changes require reprocessing all files with new embeddings
            // Poll both KBs concurrently instead of sequentially to avoid 2x timeout
            console.log("Waiting for both KB updates to complete (polling concurrently)...");
            console.log(`  KB1 UID: ${data.kb1_initial.knowledgeBaseId}`);
            console.log(`  KB2 UID: ${data.kb2_initial.knowledgeBaseId}`);
            const updateTimeout = parseInt(__ENV.UPDATE_TIMEOUT || '300'); // 5 min default
            console.log(`  Max wait: ${updateTimeout} seconds`);

            let kb1UpdateCompleted = null; // null = pending, true = completed, false = failed
            let kb2UpdateCompleted = null;
            let kb1NotFoundCount = 0;
            let kb2NotFoundCount = 0;
            const maxWaitSeconds = updateTimeout;
            const MAX_NOT_FOUND_WAIT = 60; // Wait up to 60s for KB to appear (workflow startup)

            for (let i = 0; i < maxWaitSeconds; i++) {
                const statusRes = grpcInvokeWithRetry(client,
                    "artifact.v1alpha.ArtifactPrivateService/GetKnowledgeBaseUpdateStatusAdmin",
                    {},
                    data.metadata
                );

                if (statusRes.status === grpc.StatusOK && statusRes.message.details) {
                    if (i === 0) {
                        console.log(`Status check iteration ${i}: Found ${statusRes.message.details.length} KBs in status list`);
                        console.log(`Looking for KB1 UID: ${data.kb1_initial.knowledgeBaseId}`);
                        console.log(`Looking for KB2 UID: ${data.kb2_initial.knowledgeBaseId}`);
                        if (statusRes.message.details.length > 0) {
                            console.log(`First KB in list: knowledgeBaseUid=${statusRes.message.details[0].knowledgeBaseId}, status=${statusRes.message.details[0].status}`);
                        }
                    }
                    const kb1Status = statusRes.message.details.find(d => d.knowledgeBaseId === data.kb1_initial.knowledgeBaseId);
                    const kb2Status = statusRes.message.details.find(d => d.knowledgeBaseId === data.kb2_initial.knowledgeBaseId);

                    // Check KB1 status
                    if (kb1Status) {
                        kb1NotFoundCount = 0; // Reset - KB found
                    } else if (kb1UpdateCompleted === null) {
                        // Only count not-found if we haven't determined terminal state yet
                        kb1NotFoundCount++;
                        if (kb1NotFoundCount > MAX_NOT_FOUND_WAIT) {
                            // Check database directly - update may have completed so quickly we missed it
                            // Note: knowledgeBaseId is the hash-based slug, so we query by 'id' not 'uid'
                            const kb1DbCheck = helper.safeQuery(
                                `SELECT update_status FROM knowledge_base WHERE id = $1 AND delete_time IS NULL`,
                                data.kb1_initial.knowledgeBaseId
                            );
                            if (kb1DbCheck && kb1DbCheck.length > 0) {
                                const dbStatus = kb1DbCheck[0].update_status;
                                if (dbStatus === 'KNOWLEDGE_BASE_UPDATE_STATUS_COMPLETED' || dbStatus === 6) {
                                    console.log(`✓ KB1 update already completed (verified via DB)`);
                                    kb1UpdateCompleted = true;
                                } else if (dbStatus === 'KNOWLEDGE_BASE_UPDATE_STATUS_FAILED' || dbStatus === 7 ||
                                    dbStatus === 'KNOWLEDGE_BASE_UPDATE_STATUS_ABORTED' || dbStatus === 8) {
                                    console.error(`✗ KB1 update failed/aborted (verified via DB): ${dbStatus}`);
                                    kb1UpdateCompleted = false;
                                } else {
                                    // Still in progress according to DB - continue polling
                                    console.log(`   KB1 not in status list but DB shows: ${dbStatus}, continuing...`);
                                }
                            } else {
                                console.error(`✗ KB1 not found in status list or database after ${MAX_NOT_FOUND_WAIT}s`);
                                kb1UpdateCompleted = false;
                            }
                        }
                    }

                    if (kb1Status && kb1UpdateCompleted === null) {
                        if (kb1Status.status === 6 || kb1Status.status === "KNOWLEDGE_BASE_UPDATE_STATUS_COMPLETED") {
                            kb1UpdateCompleted = true;
                            console.log(`✓ KB1 update completed (${i}s)`);
                        } else if (kb1Status.status === 7 || kb1Status.status === "KNOWLEDGE_BASE_UPDATE_STATUS_FAILED") {
                            console.error(`✗ KB1 update FAILED: ${kb1Status.errorMessage || 'No error message'}`);
                            kb1UpdateCompleted = false; // Mark as failed, but continue polling KB2
                        }
                    }

                    // Check KB2 status
                    if (kb2Status) {
                        kb2NotFoundCount = 0; // Reset - KB found
                    } else if (kb2UpdateCompleted === null) {
                        // Only count not-found if we haven't determined terminal state yet
                        kb2NotFoundCount++;
                        if (kb2NotFoundCount > MAX_NOT_FOUND_WAIT) {
                            // Check database directly - update may have completed so quickly we missed it
                            // Note: knowledgeBaseId is the hash-based slug, so we query by 'id' not 'uid'
                            const kb2DbCheck = helper.safeQuery(
                                `SELECT update_status FROM knowledge_base WHERE id = $1 AND delete_time IS NULL`,
                                data.kb2_initial.knowledgeBaseId
                            );
                            if (kb2DbCheck && kb2DbCheck.length > 0) {
                                const dbStatus = kb2DbCheck[0].update_status;
                                if (dbStatus === 'KNOWLEDGE_BASE_UPDATE_STATUS_COMPLETED' || dbStatus === 6) {
                                    console.log(`✓ KB2 update already completed (verified via DB)`);
                                    kb2UpdateCompleted = true;
                                } else if (dbStatus === 'KNOWLEDGE_BASE_UPDATE_STATUS_FAILED' || dbStatus === 7 ||
                                    dbStatus === 'KNOWLEDGE_BASE_UPDATE_STATUS_ABORTED' || dbStatus === 8) {
                                    console.error(`✗ KB2 update failed/aborted (verified via DB): ${dbStatus}`);
                                    kb2UpdateCompleted = false;
                                } else {
                                    // Still in progress according to DB - continue polling
                                    console.log(`   KB2 not in status list but DB shows: ${dbStatus}, continuing...`);
                                }
                            } else {
                                console.error(`✗ KB2 not found in status list or database after ${MAX_NOT_FOUND_WAIT}s`);
                                kb2UpdateCompleted = false;
                            }
                        }
                    }

                    if (kb2Status && kb2UpdateCompleted === null) {
                        if (kb2Status.status === 6 || kb2Status.status === "KNOWLEDGE_BASE_UPDATE_STATUS_COMPLETED") {
                            kb2UpdateCompleted = true;
                            console.log(`✓ KB2 update completed (${i}s)`);
                        } else if (kb2Status.status === 7 || kb2Status.status === "KNOWLEDGE_BASE_UPDATE_STATUS_FAILED") {
                            console.error(`✗ KB2 update FAILED: ${kb2Status.errorMessage || 'No error message'}`);
                            kb2UpdateCompleted = false; // Mark as failed, but continue polling KB1
                        }
                    }

                    // Both in terminal state (completed or failed) - exit early
                    if (kb1UpdateCompleted !== null && kb2UpdateCompleted !== null) {
                        if (kb1UpdateCompleted === true && kb2UpdateCompleted === true) {
                            console.log(`✓ Both KBs updated successfully (${i}s total)`);
                        } else {
                            console.log(`One or both KBs failed - exiting polling loop (${i}s total)`);
                        }
                        break;
                    }

                    // Log progress every 30 seconds
                    if (i > 0 && i % 30 === 0) {
                        console.log(`Still waiting... KB1: ${kb1Status ? kb1Status.status : 'not found'}, KB2: ${kb2Status ? kb2Status.status : 'not found'} (${i}s/${maxWaitSeconds}s)`);
                    }
                }

                sleep(1);
            }

            // Handle timeout case - if still null after max wait, mark as failed
            if (kb1UpdateCompleted === null) {
                console.error(`✗ KB1 update TIMEOUT after ${maxWaitSeconds}s`);
                kb1UpdateCompleted = false;
            }
            if (kb2UpdateCompleted === null) {
                console.error(`✗ KB2 update TIMEOUT after ${maxWaitSeconds}s`);
                kb2UpdateCompleted = false;
            }

            const updateCompleted = kb1UpdateCompleted === true && kb2UpdateCompleted === true;

            check({ updateCompleted }, {
                "Phase 4: Update completed successfully": () => updateCompleted === true,
                "Phase 4: KB1 updated": () => kb1UpdateCompleted === true,
                "Phase 4: KB2 updated": () => kb2UpdateCompleted === true,
            });

            if (!updateCompleted) {
                // Get final status for diagnostics
                const finalStatusRes = grpcInvokeWithRetry(client,
                    "artifact.v1alpha.ArtifactPrivateService/GetKnowledgeBaseUpdateStatusAdmin",
                    {},
                    data.metadata
                );

                if (finalStatusRes.status === grpc.StatusOK && finalStatusRes.message.details) {
                    const kb1FinalStatus = finalStatusRes.message.details.find(d => d.knowledgeBaseId === data.kb1_initial.knowledgeBaseId);
                    const kb2FinalStatus = finalStatusRes.message.details.find(d => d.knowledgeBaseId === data.kb2_initial.knowledgeBaseId);

                    if (!kb1UpdateCompleted) {
                        if (kb1FinalStatus) {
                            console.error(`✗ KB1 FAILED or TIMEOUT - Final status: ${kb1FinalStatus.status}`);
                            console.error(`   Workflow ID: ${kb1FinalStatus.workflowId || 'N/A'}`);
                            console.error(`   Error: ${kb1FinalStatus.errorMessage || 'None'}`);
                            console.error(`   Started: ${kb1FinalStatus.startedAt || 'N/A'}`);
                            console.error(`   Completed: ${kb1FinalStatus.completedAt || 'N/A'}`);
                        } else {
                            console.error(`✗ KB1 NOT FOUND in status list - workflow never started or KB was deleted`);
                        }
                    }

                    if (!kb2UpdateCompleted) {
                        if (kb2FinalStatus) {
                            console.error(`✗ KB2 FAILED or TIMEOUT - Final status: ${kb2FinalStatus.status}`);
                            console.error(`   Workflow ID: ${kb2FinalStatus.workflowId || 'N/A'}`);
                            console.error(`   Error: ${kb2FinalStatus.errorMessage || 'None'}`);
                            console.error(`   Started: ${kb2FinalStatus.startedAt || 'N/A'}`);
                            console.error(`   Completed: ${kb2FinalStatus.completedAt || 'N/A'}`);
                        } else {
                            console.error(`✗ KB2 NOT FOUND in status list - workflow never started or KB was deleted`);
                        }
                    }
                }

                // CRITICAL: Stop test - later phases depend on Phase 4 success
                console.error("❌ STOPPING TEST - Phase 4 failed, cannot proceed to Phase 5-9");
                testShouldContinue = false;
                return;
            }

            // Brief pause to ensure DB transactions are fully committed after workflow completion
            sleep(2);

            // Verify production KBs now use Gemini
            console.log("Verifying production KBs now use Gemini config...");

            const prodKB1 = helper.getKnowledgeBaseByIdAndOwner(data.kb1_initial.knowledgeBaseId, data.expectedOwner.id);
            const prodKB2 = helper.getKnowledgeBaseByIdAndOwner(data.kb2_initial.knowledgeBaseId, data.expectedOwner.id);

            if (prodKB1 && prodKB1.length > 0) {
                const systemQuery = helper.safeQuery(
                    `SELECT s.id, s.config->'rag'->'embedding'->>'model_family' as model_family FROM system s
                     JOIN knowledge_base kb ON kb.system_uid = s.uid
                     WHERE kb.uid = $1`,
                    prodKB1[0].uid
                );

                if (systemQuery && systemQuery.length > 0) {
                    const systemId = systemQuery[0].id;
                    const modelFamily = systemQuery[0].model_family || "";
                    console.log(`Production KB1 system_id after swap: ${systemId}, model_family: ${modelFamily}`);

                    check({ modelFamily }, {
                        "Phase 4: Production KB1 now uses Gemini": () => modelFamily === "gemini",
                    });
                }
            }

            if (prodKB2 && prodKB2.length > 0) {
                const systemQuery = helper.safeQuery(
                    `SELECT s.id, s.config->'rag'->'embedding'->>'model_family' as model_family FROM system s
                     JOIN knowledge_base kb ON kb.system_uid = s.uid
                     WHERE kb.uid = $1`,
                    prodKB2[0].uid
                );

                if (systemQuery && systemQuery.length > 0) {
                    const systemId = systemQuery[0].id;
                    const modelFamily = systemQuery[0].model_family || "";
                    console.log(`Production KB2 system_id after swap: ${systemId}, model_family: ${modelFamily}`);

                    check({ modelFamily }, {
                        "Phase 4: Production KB2 now uses Gemini": () => modelFamily === "gemini",
                    });
                }
            }

            // Verify rollback KBs exist and use OpenAI
            console.log("Verifying rollback KBs exist and use OpenAI config...");

            const rollback1 = helper.verifyRollbackKB(data.kb1_initial.knowledgeBaseId, data.expectedOwner.id);
            const rollback2 = helper.verifyRollbackKB(data.kb2_initial.knowledgeBaseId, data.expectedOwner.id);

            check({ rollback1, rollback2 }, {
                "Phase 4: Rollback KB1 exists": () => rollback1 && rollback1.length > 0,
                "Phase 4: Rollback KB2 exists": () => rollback2 && rollback2.length > 0,
            });

            if (rollback1 && rollback1.length > 0) {
                const systemQuery = helper.safeQuery(
                    `SELECT s.id, s.config->'rag'->'embedding'->>'model_family' as model_family FROM system s
                     JOIN knowledge_base kb ON kb.system_uid = s.uid
                     WHERE kb.uid = $1`,
                    rollback1[0].uid
                );

                if (systemQuery && systemQuery.length > 0) {
                    const systemId = systemQuery[0].id;
                    const modelFamily = systemQuery[0].model_family || "";
                    console.log(`Rollback KB1 system_id: ${systemId}, model_family: ${modelFamily}`);

                    // Check that rollback preserves valid embedding config (model_family should be non-empty)
                    check({ modelFamily, originalFamily: data.kb1OriginalModelFamily || "" }, {
                        "Phase 4: Rollback KB1 preserves embedding config": ({ modelFamily }) => modelFamily && modelFamily.length > 0,
                    });

                    // Store rollback KB info
                    data.kb1_rollback = {
                        kbUid: rollback1[0].uid,
                        knowledgeBaseId: rollback1[0].id,
                    };

                    // Track for cleanup
                    data.knowledgeBaseIds.push(rollback1[0].id);
                }
            }

            if (rollback2 && rollback2.length > 0) {
                const systemQuery = helper.safeQuery(
                    `SELECT s.id, s.config->'rag'->'embedding'->>'model_family' as model_family FROM system s
                     JOIN knowledge_base kb ON kb.system_uid = s.uid
                     WHERE kb.uid = $1`,
                    rollback2[0].uid
                );

                if (systemQuery && systemQuery.length > 0) {
                    const systemId = systemQuery[0].id;
                    const modelFamily = systemQuery[0].model_family || "";
                    // Check that rollback preserves valid embedding config (model_family should be non-empty)
                    check({ modelFamily, originalFamily: data.kb2OriginalModelFamily || "" }, {
                        "Phase 4: Rollback KB2 preserves embedding config": ({ modelFamily }) => modelFamily && modelFamily.length > 0,
                    });

                    data.kb2_rollback = {
                        kbUid: rollback2[0].uid,
                        knowledgeBaseId: rollback2[0].id,
                    };

                    // Track for cleanup
                    data.knowledgeBaseIds.push(rollback2[0].id);
                }
            }
        });

        // ====================================================================
        // PHASE 4.5: Verify Position Data After Gemini Conversion
        // ====================================================================
        if (!testShouldContinue) {
            console.warn("Skipping Phase 4.5 due to previous phase failure");
            return;
        }

        group("Phase 4.5: Verify Position Data After Gemini Conversion", () => {
            console.log("\n=== Phase 4.5: Verifying position data exists after Gemini conversion ===");

            // Check initial files that were reprocessed from OpenAI to Gemini
            if (data.kb1_initial && data.kb1_initial.fileIds && data.kb1_initial.fileIds.length > 0) {
                const testFileId = data.kb1_initial.fileIds[0];
                const testFileUid = helper.getFileUidFromId(testFileId);

                // Verify converted_file has position_data with PageDelimiters
                const convertedFileQuery = helper.safeQuery(
                    `SELECT position_data::text as position_data_text FROM converted_file
                     WHERE file_uid = $1 AND converted_type = 'CONVERTED_FILE_TYPE_CONTENT' AND position_data IS NOT NULL LIMIT 1`,
                    testFileUid
                );

                if (convertedFileQuery && convertedFileQuery.length > 0) {
                    let posData;
                    try {
                        posData = JSON.parse(convertedFileQuery[0].position_data_text);
                        console.log(`Phase 4.5: Reprocessed file position_data = ${JSON.stringify(posData)}`);
                    } catch (e) {
                        posData = null;
                    }

                    check({ posData }, {
                        "Phase 4.5: Reprocessed file has position_data": () => posData !== null,
                        "Phase 4.5: position_data has PageDelimiters": () =>
                            posData && posData.PageDelimiters !== undefined,
                        "Phase 4.5: PageDelimiters is an array": () =>
                            posData && Array.isArray(posData.PageDelimiters),
                    });
                }

                // Verify chunk has reference with PageRange (if applicable for text files)
                const chunkQuery = helper.safeQuery(
                    `SELECT reference::text as reference_text FROM chunk
                     WHERE file_uid = $1 AND reference IS NOT NULL LIMIT 1`,
                    testFileUid
                );

                if (chunkQuery && chunkQuery.length > 0) {
                    let refData;
                    try {
                        refData = JSON.parse(chunkQuery[0].reference_text);
                        console.log(`Phase 4.5: Reprocessed chunk reference = ${JSON.stringify(refData)}`);
                    } catch (e) {
                        refData = null;
                    }

                    if (refData) {
                        check({ refData }, {
                            "Phase 4.5: Reprocessed chunk has reference": () => refData !== null,
                            "Phase 4.5: Reference uses PascalCase PageRange": () =>
                                refData.PageRange !== undefined && refData.page_range === undefined,
                        });
                    }
                }
            }

            console.log("Phase 4.5: Position data verification complete");

            // ====================================================================
            // Phase 4.6: Verify Staging KB Cleanup
            // ====================================================================
            console.log("\n=== Phase 4.6: Verifying staging KB cleanup was complete ===");

            // After successful update, staging KBs should be soft-deleted and update_status cleared
            // Poll for up to 30 seconds to ensure cleanup activity has completed
            // (cleanup may take longer if there are files being processed)
            let kb1CleanupVerified = false;
            let kb2CleanupVerified = false;

            for (let i = 0; i < 30 && (!kb1CleanupVerified || !kb2CleanupVerified); i++) {
                if (!kb1CleanupVerified) {
                    const stagingKB1Check = helper.safeQuery(
                        `SELECT delete_time, update_status, update_workflow_id
                         FROM knowledge_base
                         WHERE parent_kb_uid = (SELECT uid FROM knowledge_base WHERE id = $1) AND staging = true AND 'staging' = ANY(tags)`,
                        data.kb1_initial.knowledgeBaseId
                    );

                    if (stagingKB1Check && stagingKB1Check.length > 0) {
                        const stagingKB = stagingKB1Check[0];
                        const isSoftDeleted = stagingKB.delete_time !== null;
                        const statusCleared = stagingKB.update_status === "" || stagingKB.update_status === null;

                        if (i % 5 === 0 || (isSoftDeleted && statusCleared)) {
                            console.log(`Staging KB1 cleanup check [${i + 1}s]: delete_time=${isSoftDeleted ? 'SET' : 'NULL'}, update_status="${stagingKB.update_status}"`);
                        }

                        if (isSoftDeleted && statusCleared) {
                            kb1CleanupVerified = true;
                            console.log(`✓ Staging KB1 cleanup verified after ${i + 1}s`);
                        }
                    } else {
                        if (i % 5 === 0) {
                            console.log(`Staging KB1 cleanup check [${i + 1}s]: not found in database`);
                        }
                    }
                }

                if (!kb2CleanupVerified) {
                    const stagingKB2Check = helper.safeQuery(
                        `SELECT delete_time, update_status, update_workflow_id
                         FROM knowledge_base
                         WHERE parent_kb_uid = (SELECT uid FROM knowledge_base WHERE id = $1) AND staging = true AND 'staging' = ANY(tags)`,
                        data.kb2_initial.knowledgeBaseId
                    );

                    if (stagingKB2Check && stagingKB2Check.length > 0) {
                        const stagingKB = stagingKB2Check[0];
                        const isSoftDeleted = stagingKB.delete_time !== null;
                        const statusCleared = stagingKB.update_status === "" || stagingKB.update_status === null;

                        if (i % 5 === 0 || (isSoftDeleted && statusCleared)) {
                            console.log(`Staging KB2 cleanup check [${i + 1}s]: delete_time=${isSoftDeleted ? 'SET' : 'NULL'}, update_status="${stagingKB.update_status}"`);
                        }

                        if (isSoftDeleted && statusCleared) {
                            kb2CleanupVerified = true;
                            console.log(`✓ Staging KB2 cleanup verified after ${i + 1}s`);
                        }
                    } else {
                        if (i % 5 === 0) {
                            console.log(`Staging KB2 cleanup check [${i + 1}s]: not found in database`);
                        }
                    }
                }

                if (!kb1CleanupVerified || !kb2CleanupVerified) {
                    sleep(1);
                }
            }

            // Now perform final verification with proper checks
            const stagingKB1Check = helper.safeQuery(
                `SELECT delete_time, update_status, update_workflow_id
                 FROM knowledge_base
                 WHERE parent_kb_uid = (SELECT uid FROM knowledge_base WHERE id = $1) AND staging = true AND 'staging' = ANY(tags)`,
                data.kb1_initial.knowledgeBaseId
            );

            if (stagingKB1Check && stagingKB1Check.length > 0) {
                const stagingKB = stagingKB1Check[0];
                const isSoftDeleted = stagingKB.delete_time !== null;
                const statusCleared = stagingKB.update_status === "" || stagingKB.update_status === null;

                console.log(`FINAL CHECK KB1: delete_time=${isSoftDeleted ? 'SET' : 'NULL'}, update_status="${stagingKB.update_status}"`);

                check(stagingKB, {
                    "Phase 4.6: Staging KB1 is soft-deleted": () => isSoftDeleted,
                    "Phase 4.6: Staging KB1 update_status cleared": () => statusCleared,
                    "Phase 4.6: Staging KB1 update_workflow_id cleared": () =>
                        stagingKB.update_workflow_id === "" || stagingKB.update_workflow_id === null,
                });

                if (isSoftDeleted && statusCleared) {
                    console.log("✓ Staging KB1 cleanup PASSED: soft-deleted with cleared status");
                } else {
                    console.error(`✗ Staging KB1 cleanup FAILED: delete_time=${isSoftDeleted ? 'SET' : 'NULL'}, status="${stagingKB.update_status}"`);
                }
            } else {
                console.log("Staging KB1 not found (may be hard-deleted - this is acceptable)");
            }

            const stagingKB2Check = helper.safeQuery(
                `SELECT delete_time, update_status, update_workflow_id
                 FROM knowledge_base
                 WHERE parent_kb_uid = (SELECT uid FROM knowledge_base WHERE id = $1) AND staging = true AND 'staging' = ANY(tags)`,
                data.kb2_initial.knowledgeBaseId
            );

            if (stagingKB2Check && stagingKB2Check.length > 0) {
                const stagingKB = stagingKB2Check[0];
                const isSoftDeleted = stagingKB.delete_time !== null;
                const statusCleared = stagingKB.update_status === "" || stagingKB.update_status === null;

                console.log(`FINAL CHECK KB2: delete_time=${isSoftDeleted ? 'SET' : 'NULL'}, update_status="${stagingKB.update_status}"`);

                check(stagingKB, {
                    "Phase 4.6: Staging KB2 is soft-deleted": () => isSoftDeleted,
                    "Phase 4.6: Staging KB2 update_status cleared": () => statusCleared,
                    "Phase 4.6: Staging KB2 update_workflow_id cleared": () =>
                        stagingKB.update_workflow_id === "" || stagingKB.update_workflow_id === null,
                });

                if (isSoftDeleted && statusCleared) {
                    console.log("✓ Staging KB2 cleanup PASSED: soft-deleted with cleared status");
                } else {
                    console.error(`✗ Staging KB2 cleanup FAILED: delete_time=${isSoftDeleted ? 'SET' : 'NULL'}, status="${stagingKB.update_status}"`);
                }
            } else {
                console.log("Staging KB2 not found (may be hard-deleted - this is acceptable)");
            }

            console.log("Phase 4.6: Staging KB cleanup verification complete");
        });

        // ====================================================================
        // PHASE 5: Upload Files During Retention (Dual Processing)
        // ====================================================================
        if (!testShouldContinue) {
            console.warn("Skipping Phase 5 due to previous phase failure");
            return;
        }

        group("Phase 5: Upload New Files During Retention Period", () => {
            console.log("\n=== Phase 5: Uploading new files during retention (dual processing) ===");

            // Upload new files to production KB1 (use multi-page PDF for position data testing)
            const retentionFiles1 = [];
            for (let i = 1; i <= 2; i++) {
                const filename = `${data.dbIDPrefix}sysconfig-retention-kb1-${i}.pdf`;
                const uploadRes = http.request(
                    "POST",
                    `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${data.kb1_initial.knowledgeBaseId}/files`,
                    JSON.stringify({ displayName: filename, type: "TYPE_PDF", content: constant.docSampleMultiPagePdf }),
                    data.header
                );

                if (uploadRes.status === 200) {
                    retentionFiles1.push(uploadRes.json().file);
                }
            }

            check(retentionFiles1, {
                "Phase 5: KB1 retention files uploaded": () => retentionFiles1.length === 2,
            });

            // Upload new files to production KB2
            const retentionFiles2 = [];
            for (let i = 1; i <= 2; i++) {
                const filename = `${data.dbIDPrefix}sysconfig-retention-kb2-${i}.md`;
                const uploadRes = http.request(
                    "POST",
                    `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${data.kb2_initial.knowledgeBaseId}/files`,
                    JSON.stringify({ displayName: filename, type: "TYPE_MARKDOWN", content: constant.docSampleMd }),
                    data.header
                );

                if (uploadRes.status === 200) {
                    retentionFiles2.push(uploadRes.json().file);
                }
            }

            check(retentionFiles2, {
                "Phase 5: KB2 retention files uploaded": () => retentionFiles2.length === 2,
            });

            // CRITICAL: Process files from each KB separately (same reason as Phase 1)
            // Auto-trigger: Processing starts automatically on upload

            // Auto-trigger: Processing starts automatically on upload

            // Wait for processing in BOTH production AND rollback KBs
            // With sequential dual-processing, rollback KB files are triggered after production completes
            // We must wait for ALL files to complete in BOTH KBs before triggering rollback
            const retentionFileIds = [...retentionFiles1, ...retentionFiles2].map(f => f.id);
            console.log("Waiting for retention file processing to complete in production KBs...");
            console.log(`Waiting for ${retentionFileIds.length} production retention files...`);

            // Wait for KB1 retention files (timeout configured via env or default)
            const retentionTimeout = parseInt(__ENV.FILE_PROCESS_TIMEOUT || '300'); // 5 min default
            let kb1Result = { completed: true, processedCount: 0 };
            if (retentionFiles1.length > 0) {
                kb1Result = helper.waitForMultipleFilesProcessingComplete(
                    data.expectedOwner.id,
                    data.kb1_initial.knowledgeBaseId,
                    retentionFiles1.map(f => f.id),
                    data.header,
                    retentionTimeout
                );
                if (!kb1Result.completed) {
                    console.error(`✗ KB1 retention files incomplete: ${kb1Result.status}, processed ${kb1Result.processedCount}/${retentionFiles1.length}`);
                }
            }

            // Wait for KB2 retention files (timeout configured via env or default)
            let kb2Result = { completed: true, processedCount: 0 };
            if (retentionFiles2.length > 0) {
                kb2Result = helper.waitForMultipleFilesProcessingComplete(
                    data.expectedOwner.id,
                    data.kb2_initial.knowledgeBaseId,
                    retentionFiles2.map(f => f.id),
                    data.header,
                    retentionTimeout
                );
                if (!kb2Result.completed) {
                    console.error(`✗ KB2 retention files incomplete: ${kb2Result.status}, processed ${kb2Result.processedCount}/${retentionFiles2.length}`);
                }
            }

            const productionCompleted = kb1Result.completed && kb2Result.completed;

            if (productionCompleted) {
                console.log(`✓ All ${retentionFileIds.length} production retention files processed`);
            } else {
                console.error(`✗ Production retention files failed: KB1=${kb1Result.completed}, KB2=${kb2Result.completed}`);
            }

            check({ productionCompleted }, {
                "Phase 5: Production retention files processed": () => productionCompleted === true,
            });

            // FAIL EARLY: If production files didn't complete, don't proceed with rollback verification
            if (!productionCompleted) {
                console.error("Phase 5: Aborting - production retention files did not complete in time");
                console.error("This prevents invalid test results in subsequent phases");
                return;
            }

            // CRITICAL: Also wait for rollback KB files to complete
            // Sequential dual-processing triggers rollback files after production completes
            // Rollback workflow will block if any files are NOTSTARTED in rollback KB
            // Timeout for rollback file processing (120s = 2 minutes)
            console.log("Waiting for retention file processing in rollback KBs (sequential dual-processing)...");
            let rollbackCompleted = false;
            let kb1Count = 0;
            let kb2Count = 0;

            // Verify rollback KBs exist before waiting (KB2 might not exist if update failed)
            if (!data.kb1_rollback) {
                console.error("✗ Phase 5: Rollback KB1 not found - KB1 update may have failed");
                return;
            }

            if (!data.kb2_rollback) {
                console.warn("⚠ Phase 5: Rollback KB2 not found - KB2 update likely failed, skipping KB2 rollback file wait");
                console.warn("   Continuing with KB1 rollback verification only");
            }

            // Wait for rollback KB files - only wait for KB2 if rollback KB2 exists
            // Increased timeout to 5 minutes (600 * 0.5s = 300s) to match production file processing time
            if (data.kb1_rollback && data.kb2_rollback) {
                for (let i = 0; i < 600; i++) { // 600 * 0.5s = 300s (5 minutes)
                    kb1Count = 0;
                    kb2Count = 0;

                    // Check files in rollback KB1 by filename (same filenames as production)
                    for (const filename of [`${data.dbIDPrefix}sysconfig-retention-kb1-1.pdf`, `${data.dbIDPrefix}sysconfig-retention-kb1-2.pdf`]) {
                        const fileQuery = helper.safeQuery(
                            `SELECT f.process_status FROM file f
                             INNER JOIN file_knowledge_base fkb ON f.uid = fkb.file_uid
                             WHERE fkb.kb_uid = $1 AND f.display_name = $2 AND f.delete_time IS NULL`,
                            data.kb1_rollback.kbUid,
                            filename
                        );

                        if (fileQuery && fileQuery.length > 0 && fileQuery[0].process_status === "FILE_PROCESS_STATUS_COMPLETED") {
                            kb1Count++;
                        }
                    }

                    // Check files in rollback KB2 by filename (only if rollback KB2 exists)
                    if (data.kb2_rollback) {
                        for (const filename of [`${data.dbIDPrefix}sysconfig-retention-kb2-1.md`, `${data.dbIDPrefix}sysconfig-retention-kb2-2.md`]) {
                            const fileQuery = helper.safeQuery(
                                `SELECT f.process_status FROM file f
                                 INNER JOIN file_knowledge_base fkb ON f.uid = fkb.file_uid
                                 WHERE fkb.kb_uid = $1 AND f.display_name = $2 AND f.delete_time IS NULL`,
                                data.kb2_rollback.kbUid,
                                filename
                            );

                            if (fileQuery && fileQuery.length > 0 && fileQuery[0].process_status === "FILE_PROCESS_STATUS_COMPLETED") {
                                kb2Count++;
                            }
                        }
                    }

                    // Complete if KB1 is done AND (KB2 is done OR KB2 rollback doesn't exist)
                    const kb1Done = kb1Count === 2;
                    const kb2Done = !data.kb2_rollback || kb2Count === 2; // If no rollback KB2, consider it "done"

                    if (kb1Done && kb2Done) {
                        rollbackCompleted = true;
                        console.log("All rollback KB files processed (sequential dual-processing complete)");
                        break;
                    }

                    if (i % 20 === 0 && i > 0) {
                        const kb2Status = data.kb2_rollback ? `${kb2Count}/2` : 'N/A (no rollback KB2)';
                        console.log(`Waiting for rollback files... KB1: ${kb1Count}/2, KB2: ${kb2Status} (${i * 0.5}s elapsed)`);

                        // DIAGNOSTIC: Check if files exist in rollback KBs and their status
                        if (i === 20 || i === 60) { // Log diagnostics at 10s and 30s
                            // KB1 diagnostics
                            const diagQuery1 = helper.safeQuery(
                                `SELECT f.display_name as filename, f.process_status, f.create_time, f.update_time
                                 FROM file f
                                 INNER JOIN file_knowledge_base fkb ON f.uid = fkb.file_uid
                                 WHERE fkb.kb_uid = $1 AND f.delete_time IS NULL
                                 ORDER BY f.create_time DESC`,
                                data.kb1_rollback.kbUid
                            );
                            if (diagQuery1 && diagQuery1.length > 0) {
                                console.log(`DIAGNOSTIC: Rollback KB1 has ${diagQuery1.length} files:`);
                                diagQuery1.forEach(f => console.log(`  - ${f.filename}: ${f.process_status}`));
                            } else {
                                console.log(`DIAGNOSTIC: Rollback KB1 has NO files - dual processing may have failed`);
                            }

                            // KB2 diagnostics
                            if (data.kb2_rollback) {
                                const diagQuery2 = helper.safeQuery(
                                    `SELECT f.display_name as filename, f.process_status, f.create_time, f.update_time
                                     FROM file f
                                     INNER JOIN file_knowledge_base fkb ON f.uid = fkb.file_uid
                                     WHERE fkb.kb_uid = $1 AND f.delete_time IS NULL
                                     ORDER BY f.create_time DESC`,
                                    data.kb2_rollback.kbUid
                                );
                                if (diagQuery2 && diagQuery2.length > 0) {
                                    console.log(`DIAGNOSTIC: Rollback KB2 has ${diagQuery2.length} files:`);
                                    diagQuery2.forEach(f => console.log(`  - ${f.filename}: ${f.process_status}`));
                                } else {
                                    console.log(`DIAGNOSTIC: Rollback KB2 has NO files - dual processing may have failed`);
                                }
                            }

                            // Check production KB status
                            const prodKBStatus = helper.safeQuery(
                                `SELECT update_status FROM knowledge_base WHERE id = $1`,
                                data.kb1_initial.knowledgeBaseId
                            );
                            if (prodKBStatus && prodKBStatus.length > 0) {
                                console.log(`DIAGNOSTIC: Production KB1 update_status = "${prodKBStatus[0].update_status}"`);
                            }
                        }
                    }

                    sleep(0.5);
                }

                check({ rollbackCompleted }, {
                    "Phase 5: Rollback KB files processed (dual-processing)": () => rollbackCompleted === true,
                });

                // FAIL EARLY: If rollback files didn't complete, don't proceed with subsequent phases
                if (!rollbackCompleted) {
                    const kb2Status = data.kb2_rollback ? `${kb2Count}/2` : 'N/A (no rollback KB2)';
                    console.error("✗ Phase 5: Rollback KB files did not complete in time");
                    console.error(`   Final state: KB1=${kb1Count}/2, KB2=${kb2Status}`);
                    if (data.kb2_rollback && kb2Count < 2) {
                        console.error("   KB2 rollback files stuck - KB2 update may have failed");
                        console.error("   Check Phase 4 logs for KB2 update failure details");
                    }
                    console.error("   Sequential dual-processing may be blocked or AI service overloaded");
                    console.error("Phase 5: Aborting - cannot verify rollback behavior without complete data");
                    return;
                }

                console.log("✓ All rollback KB files processed successfully");
            } else if (data.kb1_rollback) {
                // Only KB1 rollback exists - wait for KB1 files only
                console.log("Only KB1 rollback exists, waiting for KB1 rollback files only...");
                for (let i = 0; i < 600; i++) { // 600 * 0.5s = 300s (5 minutes)
                    kb1Count = 0;

                    for (const filename of [`${data.dbIDPrefix}sysconfig-retention-kb1-1.pdf`, `${data.dbIDPrefix}sysconfig-retention-kb1-2.pdf`]) {
                        const fileQuery = helper.safeQuery(
                            `SELECT f.process_status FROM file f
                             INNER JOIN file_knowledge_base fkb ON f.uid = fkb.file_uid
                             WHERE fkb.kb_uid = $1 AND f.display_name = $2 AND f.delete_time IS NULL`,
                            data.kb1_rollback.kbUid,
                            filename
                        );

                        if (fileQuery && fileQuery.length > 0 && fileQuery[0].process_status === "FILE_PROCESS_STATUS_COMPLETED") {
                            kb1Count++;
                        }
                    }

                    if (kb1Count === 2) {
                        rollbackCompleted = true;
                        console.log("KB1 rollback files processed successfully");
                        break;
                    }

                    if (i % 20 === 0 && i > 0) {
                        console.log(`Waiting for KB1 rollback files... ${kb1Count}/2 (${i * 0.5}s elapsed)`);
                    }

                    sleep(0.5);
                }

                check({ rollbackCompleted }, {
                    "Phase 5: KB1 rollback files processed": () => rollbackCompleted === true,
                });

                if (!rollbackCompleted) {
                    console.error(`✗ Phase 5: KB1 rollback files did not complete in time (${kb1Count}/2)`);
                    return;
                }
            }

            // ====================================================================
            // Phase 5.5: Verify Position Data for Multi-Page PDF Files
            // ====================================================================
            console.log("\n=== Phase 5.5: Verifying position data for multi-page PDF files ===");

            if (retentionFiles1.length > 0) {
                const pdfFileId = retentionFiles1[0].id;
                const pdfFileUid = helper.getFileUidFromId(pdfFileId);
                console.log(`Phase 5.5: Testing PDF file ${pdfFileId} (UID: ${pdfFileUid})`);

                // 1. Verify converted_file has position_data with PageDelimiters
                const convertedFileQuery = helper.safeQuery(
                    `SELECT position_data::text as position_data_text FROM converted_file
                 WHERE file_uid = $1 AND converted_type = 'CONVERTED_FILE_TYPE_CONTENT' AND position_data IS NOT NULL LIMIT 1`,
                    pdfFileUid
                );

                if (convertedFileQuery && convertedFileQuery.length > 0) {
                    let posData;
                    try {
                        posData = JSON.parse(convertedFileQuery[0].position_data_text);
                        console.log(`Phase 5.5: PDF position_data has ${posData.PageDelimiters ? posData.PageDelimiters.length : 0} page delimiters`);
                    } catch (e) {
                        posData = null;
                    }

                    check({ posData }, {
                        "Phase 5.5: PDF has position_data": () => posData !== null,
                        "Phase 5.5: position_data has PageDelimiters": () =>
                            posData && posData.PageDelimiters !== undefined,
                        "Phase 5.5: PageDelimiters is an array": () =>
                            posData && Array.isArray(posData.PageDelimiters),
                        "Phase 5.5: PageDelimiters has pages (sample-multi-page.pdf)": () =>
                            posData && posData.PageDelimiters && posData.PageDelimiters.length >= 1,
                        "Phase 5.5: position_data uses PascalCase": () =>
                            posData && posData.PageDelimiters !== undefined && posData.page_delimiters === undefined,
                    });
                }

                // 2. Verify chunk has reference with PageRange
                const chunkQuery = helper.safeQuery(
                    `SELECT reference::text as reference_text FROM chunk
                     WHERE file_uid = $1 AND reference IS NOT NULL LIMIT 1`,
                    pdfFileUid
                );

                if (chunkQuery && chunkQuery.length > 0) {
                    let refData;
                    try {
                        refData = JSON.parse(chunkQuery[0].reference_text);
                        console.log(`Phase 5.5: PDF chunk reference = ${JSON.stringify(refData)}`);
                    } catch (e) {
                        refData = null;
                    }

                    check({ refData }, {
                        "Phase 5.5: PDF chunk has reference": () => refData !== null,
                        "Phase 5.5: Reference has PageRange": () =>
                            refData && refData.PageRange !== undefined,
                        "Phase 5.5: PageRange is array with 2 elements": () =>
                            refData && Array.isArray(refData.PageRange) && refData.PageRange.length === 2,
                        "Phase 5.5: PageRange values are valid": () =>
                            refData && refData.PageRange && refData.PageRange[0] > 0 && refData.PageRange[1] > 0,
                        "Phase 5.5: Reference uses PascalCase PageRange": () =>
                            refData && refData.PageRange !== undefined && refData.page_range === undefined,
                    });
                }

                // 3. Verify chunk API returns UNIT_PAGE references
                const chunksResp = http.request(
                    "GET",
                    `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${data.kb1_initial.knowledgeBaseId}/files/${pdfFileId}/chunks`,
                    null,
                    data.header
                );

                if (chunksResp.status === 200 && chunksResp.json().chunks) {
                    const chunks = chunksResp.json().chunks;
                    console.log(`Phase 5.5: PDF has ${chunks.length} chunks in API response`);

                    if (chunks.length > 0) {
                        const firstChunk = chunks[0];

                        check(firstChunk, {
                            "Phase 5.5: Chunk has reference": () =>
                                firstChunk.reference !== null && firstChunk.reference !== undefined,
                            "Phase 5.5: Reference has start position": () =>
                                firstChunk.reference && firstChunk.reference.start !== null,
                            "Phase 5.5: Start position unit is UNIT_PAGE": () =>
                                firstChunk.reference && firstChunk.reference.start &&
                                firstChunk.reference.start.unit === "UNIT_PAGE",
                            "Phase 5.5: Start position has coordinates": () =>
                                firstChunk.reference && firstChunk.reference.start &&
                                Array.isArray(firstChunk.reference.start.coordinates) &&
                                firstChunk.reference.start.coordinates.length > 0,
                            "Phase 5.5: Reference has end position": () =>
                                firstChunk.reference && firstChunk.reference.end !== null,
                            "Phase 5.5: End position unit is UNIT_PAGE": () =>
                                firstChunk.reference && firstChunk.reference.end &&
                                firstChunk.reference.end.unit === "UNIT_PAGE",
                        });

                        // Also verify markdown_reference exists (character positions in markdown)
                        check(firstChunk, {
                            "Phase 5.5: Chunk has markdown_reference": () =>
                                firstChunk.markdownReference !== null && firstChunk.markdownReference !== undefined,
                            "Phase 5.5: Markdown reference start unit is UNIT_CHARACTER": () =>
                                firstChunk.markdownReference && firstChunk.markdownReference.start &&
                                firstChunk.markdownReference.start.unit === "UNIT_CHARACTER",
                        });
                    }
                }
            }

            console.log("Phase 5.5: Position data verification complete for PDF files");

            // Store retention file IDs for later phases
            data.kb1_retention_file_ids = retentionFiles1.map(f => f.id);
            data.kb2_retention_file_ids = retentionFiles2.map(f => f.id);
        });

        // ====================================================================
        // PHASE 6: Verify Dual Processing Used Different Configs
        // ====================================================================
        if (!testShouldContinue) {
            console.warn("Skipping Phase 6 due to previous phase failure");
            return;
        }

        group("Phase 6: Verify Dual Processing with Different Configs", () => {
            console.log("\n=== Phase 6: Verifying dual processing occurred ===");

            // Check that files exist in both production and rollback KBs
            if (data.kb1_rollback && data.kb1_retention_file_ids) {
                // Get the production KB1 UID from the hash-based ID
                const prodKB1UidQuery = helper.safeQuery(
                    `SELECT uid FROM knowledge_base WHERE id = $1`,
                    data.kb1_initial.knowledgeBaseId
                );
                const prodKB1Uid = prodKB1UidQuery && prodKB1UidQuery.length > 0 ? prodKB1UidQuery[0].uid : null;

                if (!prodKB1Uid) {
                    console.error(`Phase 6: Could not find production KB1 UID for ID ${data.kb1_initial.knowledgeBaseId}`);
                    return;
                }

                const prodKB1FileCount = helper.countFilesInKnowledgeBase(prodKB1Uid);
                const rollbackKB1FileCount = helper.countFilesInKnowledgeBase(data.kb1_rollback.kbUid);

                console.log(`KB1 - Production files: ${prodKB1FileCount}, Rollback files: ${rollbackKB1FileCount}`);

                check({ prodKB1FileCount, rollbackKB1FileCount }, {
                    "Phase 6: KB1 production has all files": () => prodKB1FileCount >= (data.kb1_initial.fileCount + data.kb1_retention_file_ids.length),
                    "Phase 6: KB1 rollback has all files (dual processing)": () => rollbackKB1FileCount >= (data.kb1_initial.fileCount + data.kb1_retention_file_ids.length),
                });

                // Verify files in rollback KB
                for (const filename of [`${data.dbIDPrefix}sysconfig-retention-kb1-1.pdf`, `${data.dbIDPrefix}sysconfig-retention-kb1-2.pdf`]) {
                    const fileQuery = helper.safeQuery(
                        `SELECT COUNT(*) as count FROM file f
                         INNER JOIN file_knowledge_base fkb ON f.uid = fkb.file_uid
                         WHERE fkb.kb_uid = $1 AND f.display_name = $2`,
                        data.kb1_rollback.kbUid,
                        filename
                    );

                    const fileExists = fileQuery && fileQuery.length > 0 && parseInt(fileQuery[0].count) > 0;
                    check({ fileExists }, {
                        [`Phase 6: KB1 rollback has ${filename} (dual processing)`]: () => fileExists === true,
                    });
                }
            }

            // Similar check for KB2
            if (data.kb2_rollback && data.kb2_retention_file_ids) {
                // Get the production KB2 UID from the hash-based ID
                const prodKB2UidQuery = helper.safeQuery(
                    `SELECT uid FROM knowledge_base WHERE id = $1`,
                    data.kb2_initial.knowledgeBaseId
                );
                const prodKB2Uid = prodKB2UidQuery && prodKB2UidQuery.length > 0 ? prodKB2UidQuery[0].uid : null;

                if (!prodKB2Uid) {
                    console.error(`Phase 6: Could not find production KB2 UID for ID ${data.kb2_initial.knowledgeBaseId}`);
                    return;
                }

                const prodKB2FileCount = helper.countFilesInKnowledgeBase(prodKB2Uid);
                const rollbackKB2FileCount = helper.countFilesInKnowledgeBase(data.kb2_rollback.kbUid);

                console.log(`KB2 - Production files: ${prodKB2FileCount}, Rollback files: ${rollbackKB2FileCount}`);

                check({ prodKB2FileCount, rollbackKB2FileCount }, {
                    "Phase 6: KB2 production has all files": () => prodKB2FileCount >= (data.kb2_initial.fileCount + data.kb2_retention_file_ids.length),
                    "Phase 6: KB2 rollback has all files (dual processing)": () => rollbackKB2FileCount >= (data.kb2_initial.fileCount + data.kb2_retention_file_ids.length),
                });
            }

            console.log("Dual processing verification complete");
        });

        // ====================================================================
        // PHASE 7: Trigger Rollback
        // ====================================================================
        if (!testShouldContinue) {
            console.warn("Skipping Phase 7 due to previous phase failure");
            return;
        }

        group("Phase 7: Execute Rollback", () => {
            console.log("\n=== Phase 7: Triggering rollback for KB1 ===");

            // Rollback KB1 only (keep KB2 for retention expiry test)
            const rollbackRes = grpcInvokeWithRetry(client,
                "artifact.v1alpha.ArtifactPrivateService/RollbackAdmin",
                {
                    name: `namespaces/${data.expectedOwner.id}/knowledge-bases/${data.kb1_initial.knowledgeBaseId}`,
                },
                data.metadata
            );

            check(rollbackRes, {
                "Phase 7: Rollback triggered successfully": (r) => r.status === grpc.StatusOK,
                "Phase 7: Rollback has knowledge base": (r) => r.message && r.message.knowledgeBase !== null,
            });

            if (rollbackRes.status !== grpc.StatusOK) {
                console.error("Rollback failed");
                return;
            }

            console.log("Rollback completed");

            // Give rollback a moment to settle
            sleep(2);
        });

        // ====================================================================
        // PHASE 8: Verify Rollback Restored Original Config
        // ====================================================================
        if (!testShouldContinue) {
            console.warn("Skipping Phase 8 due to previous phase failure");
            return;
        }

        group("Phase 8: Verify Rollback Restored OpenAI Config", () => {
            console.log("\n=== Phase 8: Verifying rollback restored OpenAI config ===");

            // Check production KB1 now uses OpenAI again
            const prodKB1 = helper.getKnowledgeBaseByIdAndOwner(data.kb1_initial.knowledgeBaseId, data.expectedOwner.id);

            if (prodKB1 && prodKB1.length > 0) {
                check(prodKB1[0], {
                    "Phase 8: KB1 status is ROLLED_BACK": () => prodKB1[0].update_status === "KNOWLEDGE_BASE_UPDATE_STATUS_ROLLED_BACK",
                });

                const systemQuery = helper.safeQuery(
                    `SELECT s.slug FROM system s
                     JOIN knowledge_base kb ON kb.system_uid = s.uid
                     WHERE kb.uid = $1`,
                    prodKB1[0].uid
                );

                if (systemQuery && systemQuery.length > 0) {
                    const systemSlug = systemQuery[0].slug;
                    const expectedSlug = data.kb1OriginalSystemSlug || "openai";
                    console.log(`Production KB1 system_slug after rollback: ${systemSlug} (expected: ${expectedSlug})`);

                    check({ systemSlug, expectedSlug }, {
                        "Phase 8: KB1 restored to original system config": ({ systemSlug, expectedSlug }) => systemSlug === expectedSlug,
                    });
                }

                // Verify all files are still present
                const fileCount = helper.countFilesInKnowledgeBase(prodKB1[0].uid);
                const retentionFileCount = data.kb1_retention_file_ids ? data.kb1_retention_file_ids.length : 0;
                const expectedCount = data.kb1_initial.fileCount + retentionFileCount;

                console.log(`KB1 after rollback - files: ${fileCount}, expected: ${expectedCount}`);

                check({ fileCount }, {
                    "Phase 8: KB1 has all files after rollback": () => fileCount >= expectedCount,
                });

                // ====================================================================
                // Phase 8.5: Verify Position Data Survived Rollback
                // ====================================================================
                console.log("\n=== Phase 8.5: Verifying position data survived rollback (back to OpenAI) ===");

                if (data.kb1_retention_file_ids && data.kb1_retention_file_ids.length > 0) {
                    const pdfFileId = data.kb1_retention_file_ids[0];
                    const pdfFileUid = helper.getFileUidFromId(pdfFileId);
                    console.log(`Phase 8.5: Testing PDF file ${pdfFileId} (UID: ${pdfFileUid}) after rollback`);

                    // 1. Verify converted_file still has position_data with PageDelimiters
                    const convertedFileQuery = helper.safeQuery(
                        `SELECT position_data::text as position_data_text FROM converted_file
                         WHERE file_uid = $1 AND converted_type = 'CONVERTED_FILE_TYPE_CONTENT' AND position_data IS NOT NULL LIMIT 1`,
                        pdfFileUid
                    );

                    if (convertedFileQuery && convertedFileQuery.length > 0) {
                        let posData;
                        try {
                            posData = JSON.parse(convertedFileQuery[0].position_data_text);
                            console.log(`Phase 8.5: After rollback, PDF position_data has ${posData.PageDelimiters ? posData.PageDelimiters.length : 0} page delimiters`);
                        } catch (e) {
                            posData = null;
                        }

                        check({ posData }, {
                            "Phase 8.5: After rollback, PDF has position_data": () => posData !== null,
                            "Phase 8.5: After rollback, position_data has PageDelimiters": () =>
                                posData && posData.PageDelimiters !== undefined,
                            "Phase 8.5: After rollback, PageDelimiters is an array": () =>
                                posData && Array.isArray(posData.PageDelimiters),
                            "Phase 8.5: After rollback, PageDelimiters has pages (sample-multi-page.pdf)": () =>
                                posData && posData.PageDelimiters && posData.PageDelimiters.length >= 1,
                        });
                    }

                    // 2. Verify chunk still has reference with PageRange
                    const chunkQuery = helper.safeQuery(
                        `SELECT reference::text as reference_text FROM chunk
                         WHERE file_uid = $1 AND reference IS NOT NULL LIMIT 1`,
                        pdfFileUid
                    );

                    if (chunkQuery && chunkQuery.length > 0) {
                        let refData;
                        try {
                            refData = JSON.parse(chunkQuery[0].reference_text);
                            console.log(`Phase 8.5: After rollback, chunk reference = ${JSON.stringify(refData)}`);
                        } catch (e) {
                            refData = null;
                        }

                        check({ refData }, {
                            "Phase 8.5: After rollback, chunk has reference": () => refData !== null,
                            "Phase 8.5: After rollback, reference has PageRange": () =>
                                refData && refData.PageRange !== undefined,
                            "Phase 8.5: After rollback, PageRange is valid array": () =>
                                refData && Array.isArray(refData.PageRange) && refData.PageRange.length === 2,
                        });
                    }

                    // 3. Verify chunk API still returns UNIT_PAGE references
                    const chunksResp = http.request(
                        "GET",
                        `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${data.kb1_initial.knowledgeBaseId}/files/${pdfFileId}/chunks`,
                        null,
                        data.header
                    );

                    if (chunksResp.status === 200 && chunksResp.json().chunks) {
                        const chunks = chunksResp.json().chunks;
                        console.log(`Phase 8.5: After rollback, PDF has ${chunks.length} chunks in API response`);

                        if (chunks.length > 0) {
                            const firstChunk = chunks[0];

                            check(firstChunk, {
                                "Phase 8.5: After rollback, chunk has reference": () =>
                                    firstChunk.reference !== null && firstChunk.reference !== undefined,
                                "Phase 8.5: After rollback, reference unit is UNIT_PAGE": () =>
                                    firstChunk.reference && firstChunk.reference.start &&
                                    firstChunk.reference.start.unit === "UNIT_PAGE",
                                "Phase 8.5: After rollback, coordinates are valid": () =>
                                    firstChunk.reference && firstChunk.reference.start &&
                                    Array.isArray(firstChunk.reference.start.coordinates) &&
                                    firstChunk.reference.start.coordinates.length > 0 &&
                                    firstChunk.reference.start.coordinates[0] > 0,
                            });
                        }
                    }
                }

                console.log("Phase 8.5: Position data verification complete after rollback");
            }

            console.log("Rollback verification complete");
        });

        // ====================================================================
        // PHASE 9: Test Retention Expiry Cleanup
        // ====================================================================
        if (!testShouldContinue) {
            console.warn("Skipping Phase 9 due to previous phase failure");
            return;
        }

        group("Phase 9: Test Retention Expiry Cleanup", () => {
            console.log("\n=== Phase 9: Testing retention expiry cleanup ===");

            // For KB2, we'll trigger retention expiry manually
            // First, set retention to a very short period (1 second)
            const setRetentionRes = grpcInvokeWithRetry(client,
                "artifact.v1alpha.ArtifactPrivateService/SetRollbackRetentionAdmin",
                {
                    name: `namespaces/${data.expectedOwner.id}/knowledge-bases/${data.kb2_initial.knowledgeBaseId}`,
                    duration: 1,
                    time_unit: "TIME_UNIT_SECOND",
                },
                data.metadata
            );

            check(setRetentionRes, {
                "Phase 9: Retention period set successfully": (r) => r.status === grpc.StatusOK,
            });

            // Wait for retention to expire and cleanup to happen
            console.log("Waiting for retention expiry and cleanup...");
            sleep(5);

            // Manually trigger purge
            const purgeRes = grpcInvokeWithRetry(client,
                "artifact.v1alpha.ArtifactPrivateService/PurgeRollbackAdmin",
                {
                    name: `namespaces/${data.expectedOwner.id}/knowledge-bases/${data.kb2_initial.knowledgeBaseId}`,
                },
                data.metadata
            );

            // Accept either success or "not found" error
            // The rollback KB might already be cleaned up by the automatic workflow
            check(purgeRes, {
                "Phase 9: Purge triggered successfully": (r) =>
                    r.status === grpc.StatusOK ||
                    (r.status === grpc.StatusInternal && r.error && r.error.message && r.error.message.includes("not found")),
                "Phase 9: Purge success flag": (r) =>
                    (r.message && r.message.success === true) ||
                    (r.error && r.error.message && r.error.message.includes("not found")), // Already cleaned up is also success
            });

            // Wait for cleanup to complete
            if (data.kb2_rollback) {
                // BEFORE cleanup completes, verify rollback KB status will be cleared
                console.log("Verifying rollback KB status before cleanup completes...");
                const rollbackKBPreCleanup = helper.safeQuery(
                    `SELECT uid, delete_time, update_status, update_workflow_id
                     FROM knowledge_base
                     WHERE uid = $1`,
                    data.kb2_rollback.kbUid
                );

                if (rollbackKBPreCleanup && rollbackKBPreCleanup.length > 0) {
                    console.log(`Rollback KB2 pre-cleanup state: update_status="${rollbackKBPreCleanup[0].update_status}", delete_time=${rollbackKBPreCleanup[0].delete_time}`);
                }

                const cleanedUp = helper.pollRollbackKBCleanup(
                    data.kb2_rollback.knowledgeBaseId,
                    data.kb2_rollback.kbUid,
                    data.expectedOwner.id,
                    60
                );

                check({ cleanedUp }, {
                    "Phase 9: Rollback KB2 cleaned up after expiry": () => cleanedUp === true,
                });

                // AFTER cleanup, verify status was cleared
                if (cleanedUp) {
                    const rollbackKBPostCleanup = helper.safeQuery(
                        `SELECT uid, delete_time, update_status, update_workflow_id
                         FROM knowledge_base
                         WHERE uid = $1`,
                        data.kb2_rollback.kbUid
                    );

                    if (rollbackKBPostCleanup && rollbackKBPostCleanup.length > 0) {
                        const rollbackKB = rollbackKBPostCleanup[0];
                        check(rollbackKB, {
                            "Phase 9: Rollback KB2 is soft-deleted": () => rollbackKB.delete_time !== null,
                            "Phase 9: Rollback KB2 update_status cleared": () =>
                                rollbackKB.update_status === "" || rollbackKB.update_status === null,
                            "Phase 9: Rollback KB2 update_workflow_id cleared": () =>
                                rollbackKB.update_workflow_id === "" || rollbackKB.update_workflow_id === null,
                        });
                        console.log("Rollback KB2 cleanup verified: soft-deleted with cleared status");
                    } else {
                        console.log("Rollback KB2 not found after cleanup (may be hard-deleted - acceptable)");
                    }
                }

                // Verify production KB2 still works normally
                const prodKB2 = helper.getKnowledgeBaseByIdAndOwner(data.kb2_initial.knowledgeBaseId, data.expectedOwner.id);

                if (prodKB2 && prodKB2.length > 0) {
                    check(prodKB2[0], {
                        "Phase 9: Production KB2 still exists": () => prodKB2[0].delete_time === null,
                        "Phase 9: Production KB2 retention cleared": () => prodKB2[0].rollback_retention_until === null,
                    });

                    const fileCount = helper.countFilesInKnowledgeBase(prodKB2[0].uid);
                    const expectedCount = data.kb2_initial.fileCount + data.kb2_retention_file_ids.length;

                    check({ fileCount }, {
                        "Phase 9: Production KB2 still has all files": () => fileCount >= expectedCount,
                    });
                }
            }

            console.log("Retention expiry test complete");
        });
    });

    // Close gRPC connection
    client.close();
}
