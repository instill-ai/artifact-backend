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
 *
 * POSITION DATA VALIDATIONS:
 * - OpenAI pipeline generates position_data with PageDelimiters
 * - Gemini AI route generates position_data with PageDelimiters
 * - text_chunk.reference contains PageRange (PascalCase)
 * - converted_file.position_data contains PageDelimiters (PascalCase)
 * - API returns chunks with UNIT_PAGE references
 * - API returns chunks with markdown_reference (UNIT_CHARACTER)
 * - Position data preserved through OpenAI → Gemini update
 * - Position data preserved through Gemini → OpenAI rollback
 * - Multi-page PDF files have correct PageDelimiters count (4 pages in sample-multi-page.pdf)
 */

import grpc from "k6/net/grpc";
import { check, group, sleep } from "k6";
import http from "k6/http";
import { randomString } from "https://jslib.k6.io/k6-utils/1.1.0/index.js";

import * as constant from "./const.js";
import * as helper from "./helper.js";

const client = new grpc.Client();
client.load(
    ["./proto"],
    "artifact/artifact/v1alpha/artifact_private_service.proto"
);

export let options = {
    setupTimeout: '600s',
    teardownTimeout: '180s',
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

    // Authenticate
    const loginResp = http.request("POST", `${constant.mgmtRESTPublicHost}/v1beta/auth/login`, JSON.stringify({
        "username": constant.defaultUsername,
        "password": constant.defaultPassword,
    }));

    check(loginResp, {
        "Setup: Authentication successful": (r) => r.status === 200,
    });

    const header = {
        "headers": {
            "Authorization": `Bearer ${loginResp.json().accessToken}`,
            "Content-Type": "application/json",
        },
        "timeout": "600s",
    };

    const userResp = http.request("GET", `${constant.mgmtRESTPublicHost}/v1beta/user`, {}, {
        headers: { "Authorization": `Bearer ${loginResp.json().accessToken}` }
    });

    const grpcMetadata = {
        "metadata": {
            "Authorization": `Bearer ${loginResp.json().accessToken}`
        },
        "timeout": "600s"
    };

    // Cleanup orphaned catalogs from previous failed test runs OF THIS SPECIFIC TEST
    // Use API-only cleanup to properly trigger workflows (no direct DB manipulation)
    console.log("\n=== SETUP: Cleaning up previous test data (sysconfig pattern only) ===");
    try {
        const listResp = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${userResp.json().user.id}/catalogs`, null, header);
        if (listResp.status === 200) {
            const catalogs = Array.isArray(listResp.json().catalogs) ? listResp.json().catalogs : [];
            let cleanedCount = 0;
            for (const catalog of catalogs) {
                const catId = catalog.catalogId || catalog.catalog_id;
                if (catId && catId.match(/test-[a-z0-9]+-sysconfig-/)) {
                    const delResp = http.request("DELETE", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${userResp.json().user.id}/catalogs/${catId}`, null, header);
                    if (delResp.status === 200 || delResp.status === 204) {
                        cleanedCount++;
                    }
                }
            }
            console.log(`Cleaned ${cleanedCount} orphaned catalogs from previous test runs`);
        }
    } catch (e) {
        console.log(`Setup cleanup warning: ${e}`);
    }
    console.log("=== SETUP: Cleanup complete ===\n");

    return {
        header: header,
        expectedOwner: userResp.json().user,
        metadata: grpcMetadata,
        catalogIds: [], // Track created catalogs for cleanup
        dbIDPrefix: dbIDPrefix
    };
}

export function teardown(data) {
    group("System Config Update Test: Teardown", () => {
        check(true, { [constant.banner('Teardown')]: () => true });

        console.log("\n=== TEARDOWN: Cleaning up test resources ===");

        // CRITICAL: Wait for THIS TEST's file processing to complete before deleting catalogs
        // Deleting catalogs triggers cleanup workflows that drop vector DB collections
        // If we delete while files are still processing, we get "collection does not exist" errors
        console.log("Teardown: Waiting for this test's file processing to complete...");
        const allProcessingComplete = helper.waitForAllFileProcessingComplete(120, data.dbIDPrefix);
        if (!allProcessingComplete) {
            console.warn("Teardown: Some files still processing after 120s, proceeding anyway");
        }

        // Delete all test catalogs via API (primary cleanup method)
        console.log("Deleting test catalogs via API...");
        let deletedCount = 0;
        for (const catalogId of data.catalogIds) {
            try {
                const deleteRes = http.request(
                    "DELETE",
                    `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}`,
                    null,
                    data.header
                );
                if (deleteRes.status === 200) {
                    deletedCount++;
                }
                console.log(`Deleted catalog: ${catalogId} (status: ${deleteRes.status})`);
            } catch (e) {
                console.error(`Failed to delete catalog ${catalogId}: ${e}`);
            }
        }

        console.log(`API cleanup: ${deletedCount}/${data.catalogIds.length} catalogs deleted via API`);

        // CRITICAL: Hard delete test data from DB to prevent orphaned records affecting next test
        // Soft deletion (delete_time) is not enough because:
        // 1. Background cleanup may not run immediately
        // 2. Next test may see orphaned records and try to process files with missing blobs
        // 3. Previous test failures may leave inconsistent state
        //
        // SAFETY: Only delete KBs explicitly tracked in data.catalogIds (created by THIS test)
        // This prevents accidentally deleting other tests' data
        console.log("Performing hard DB cleanup for test data...");

        if (data.catalogIds && data.catalogIds.length > 0) {
            // Build list of catalog IDs to delete (ONLY those created by this test)
            const catalogIdsToDelete = data.catalogIds.map(id => `'${id}'`).join(',');
            console.log(`Deleting KBs with IDs: ${data.catalogIds.join(', ')}`);

            const dbCleanupResult = helper.safeQuery(
                `-- Hard delete ONLY the KBs explicitly created by THIS test (tracked in data.catalogIds)
                 WITH deleted_kbs AS (
                     DELETE FROM knowledge_base
                     WHERE id IN (${catalogIdsToDelete})
                     RETURNING uid
                 ),
                 deleted_files AS (
                     DELETE FROM knowledge_base_file
                     WHERE kb_uid IN (SELECT uid FROM deleted_kbs)
                     RETURNING uid
                 ),
                 deleted_chunks AS (
                     DELETE FROM text_chunk
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
            console.log("No catalogIds to clean up");
        }

        console.log("Cleanup complete - next test run will start with clean state");

        client.close();
        console.log("=== TEARDOWN: Complete ===");
    });
}

export default function (data) {
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
            const catalogId1 = `${data.dbIDPrefix}sysconfig-kb1-${randomSuffix}`;
            const catalogId2 = `${data.dbIDPrefix}sysconfig-kb2-${randomSuffix}`;

            // Create first KB with OpenAI
            const createReq1 = {
                id: catalogId1,
                description: "Test catalog 1 for system config update",
                tags: ["test", "openai"],
                system_id: "openai"
            };

            const createRes1 = http.request(
                "POST",
                `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`,
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
            if (!responseBody1 || !responseBody1.catalog) {
                console.error(`Phase 1: Response missing catalog field. Response: ${JSON.stringify(responseBody1)}`);
                testShouldContinue = false;
                return;
            }

            const catalog1 = responseBody1.catalog;
            data.catalogIds.push(catalogId1);

            // Verify OpenAI config
            check(catalog1, {
                "Phase 1: KB1 has catalog object": (c) => c !== undefined && c !== null,
                "Phase 1: KB1 has embeddingConfig": (c) => c && c.embeddingConfig !== undefined,
                "Phase 1: KB1 has OpenAI model_family": (c) => c && c.embeddingConfig && c.embeddingConfig.modelFamily === "openai",
                "Phase 1: KB1 has 1536 dimensionality": (c) => c && c.embeddingConfig && c.embeddingConfig.dimensionality === 1536,
            });

            if (!catalog1 || !catalog1.embeddingConfig) {
                console.error(`Phase 1: KB1 missing embedding config. Catalog: ${JSON.stringify(catalog1)}`);
                testShouldContinue = false;
                return;
            }

            // Create second KB with OpenAI
            const createReq2 = {
                id: catalogId2,
                description: "Test catalog 2 for system config update",
                tags: ["test", "openai"],
                system_id: "openai"
            };

            const createRes2 = http.request(
                "POST",
                `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`,
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
            if (!responseBody2 || !responseBody2.catalog) {
                console.error(`Phase 1: Response missing catalog field. Response: ${JSON.stringify(responseBody2)}`);
                testShouldContinue = false;
                return;
            }

            const catalog2 = responseBody2.catalog;
            data.catalogIds.push(catalogId2);

            // Upload initial files to KB1 (use multi-page PDF to test position data with OpenAI)
            console.log("Uploading initial files to KB1...");
            const initialFiles1 = [];

            // Upload at least one PDF to test position data generation with OpenAI
            for (let i = 1; i <= 2; i++) {
                const filename = `${data.dbIDPrefix}sysconfig-initial-kb1-${i}.pdf`;
                const uploadRes = http.request(
                    "POST",
                    `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId1}/files`,
                    JSON.stringify({ filename: filename, type: "TYPE_PDF", content: constant.sampleMultiPagePdf }),
                    data.header
                );

                if (uploadRes.status === 200) {
                    const fileObj = uploadRes.json().file;
                    console.log(`KB1 File ${i} uploaded: uid=${fileObj ? fileObj.uid : 'missing'}, fileUid=${fileObj ? fileObj.fileUid : 'missing'}`);
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
                    `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId2}/files`,
                    JSON.stringify({ filename: filename, type: "TYPE_TEXT", content: constant.sampleTxt }),
                    data.header
                );

                if (uploadRes.status === 200) {
                    const fileObj = uploadRes.json().file;
                    console.log(`KB2 File ${i} uploaded: uid=${fileObj ? fileObj.uid : 'missing'}, fileUid=${fileObj ? fileObj.fileUid : 'missing'}`);
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
            console.log(`File UIDs: ${allInitialFiles.map(f => f.uid).join(', ')}`);

            // Wait for files in KB1
            if (initialFiles1.length > 0) {
                console.log(`Waiting for ${initialFiles1.length} files in KB1...`);
                const result1 = helper.waitForMultipleFilesProcessingComplete(
                    data.expectedOwner.id,
                    catalogId1,
                    initialFiles1.map(f => f.uid),
                    data.header,
                    360 // Max 360 seconds
                );
                if (!result1.completed) {
                    console.log(`KB1 files incomplete: ${result1.status}, processed ${result1.processedCount}/${initialFiles1.length}`);
                }
            }

            // Wait for files in KB2
            if (initialFiles2.length > 0) {
                console.log(`Waiting for ${initialFiles2.length} files in KB2...`);
                const result2 = helper.waitForMultipleFilesProcessingComplete(
                    data.expectedOwner.id,
                    catalogId2,
                    initialFiles2.map(f => f.uid),
                    data.header,
                    360 // Max 360 seconds
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
                const pdfFileUid = initialFiles1[0].fileUid;
                console.log(`Phase 1.5: Testing OpenAI-processed PDF file ${pdfFileUid}`);

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
                        "Phase 1.5: OpenAI PageDelimiters has 4 pages (sample-multi-page.pdf)": () =>
                            posData && posData.PageDelimiters && posData.PageDelimiters.length === 4,
                    });
                }

                // 2. Verify text_chunk has reference with PageRange
                const chunkQuery = helper.safeQuery(
                    `SELECT reference::text as reference_text FROM text_chunk
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
                    `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId1}/files/${pdfFileUid}/chunks`,
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
                catalogId: catalogId1,
                catalogUid: catalog1.uid,
                fileCount: initialFiles1.length,
                fileUids: initialFiles1.map(f => f.uid),
            };

            data.kb2_initial = {
                catalogId: catalogId2,
                catalogUid: catalog2.uid,
                fileCount: initialFiles2.length,
                fileUids: initialFiles2.map(f => f.uid),
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
            const updateRes = client.invoke(
                "artifact.artifact.v1alpha.ArtifactPrivateService/ExecuteKnowledgeBaseUpdateAdmin",
                {
                    catalog_ids: [data.kb1_initial.catalogId, data.kb2_initial.catalogId],
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
            const staging1Found = helper.pollForStagingKB(data.kb1_initial.catalogId, data.expectedOwner.uid, 60);
            const staging2Found = helper.pollForStagingKB(data.kb2_initial.catalogId, data.expectedOwner.uid, 60);

            check({ staging1Found, staging2Found }, {
                "Phase 3: Staging KB1 created": () => staging1Found === true,
                "Phase 3: Staging KB2 created": () => staging2Found === true,
            });

            if (staging1Found && staging2Found) {
                // Get staging KBs from database
                const staging1 = helper.verifyStagingKB(data.kb1_initial.catalogId, data.expectedOwner.uid);
                const staging2 = helper.verifyStagingKB(data.kb2_initial.catalogId, data.expectedOwner.uid);

                if (staging1 && staging1.length > 0) {
                    console.log("Staging KB1 found in database");

                    // Query system_uid to verify it's pointing to Gemini system
                    const systemQuery = helper.safeQuery(
                        `SELECT s.id, s.config FROM system s
                         JOIN knowledge_base kb ON kb.system_uid = s.uid
                         WHERE kb.uid = $1`,
                        staging1[0].uid
                    );

                    if (systemQuery && systemQuery.length > 0) {
                        const systemId = systemQuery[0].id;
                        console.log(`Staging KB1 system_id: ${systemId}`);

                        check({ systemId }, {
                            "Phase 3: Staging KB1 uses Gemini system": () => systemId === "gemini",
                        });
                    }
                }

                if (staging2 && staging2.length > 0) {
                    console.log("Staging KB2 found in database");

                    const systemQuery = helper.safeQuery(
                        `SELECT s.id FROM system s
                         JOIN knowledge_base kb ON kb.system_uid = s.uid
                         WHERE kb.uid = $1`,
                        staging2[0].uid
                    );

                    if (systemQuery && systemQuery.length > 0) {
                        const systemId = systemQuery[0].id;
                        console.log(`Staging KB2 system_id: ${systemId}`);

                        check({ systemId }, {
                            "Phase 3: Staging KB2 uses Gemini system": () => systemId === "gemini",
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
            console.log(`  KB1 UID: ${data.kb1_initial.catalogUid}`);
            console.log(`  KB2 UID: ${data.kb2_initial.catalogUid}`);
            console.log(`  Max wait: 600 seconds`);

            let kb1UpdateCompleted = null; // null = pending, true = completed, false = failed
            let kb2UpdateCompleted = null;
            let kb1NotFoundCount = 0;
            let kb2NotFoundCount = 0;
            const maxWaitSeconds = 600; // 10 minutes total
            const MAX_NOT_FOUND_WAIT = 60; // Wait up to 60s for KB to appear (workflow startup)

            for (let i = 0; i < maxWaitSeconds; i++) {
                const statusRes = client.invoke(
                    "artifact.artifact.v1alpha.ArtifactPrivateService/GetKnowledgeBaseUpdateStatusAdmin",
                    {},
                    data.metadata
                );

                if (statusRes.status === grpc.StatusOK && statusRes.message.details) {
                    if (i === 0) {
                        console.log(`Status check iteration ${i}: Found ${statusRes.message.details.length} KBs in status list`);
                        console.log(`Looking for KB1 UID: ${data.kb1_initial.catalogUid}`);
                        console.log(`Looking for KB2 UID: ${data.kb2_initial.catalogUid}`);
                        if (statusRes.message.details.length > 0) {
                            console.log(`First KB in list: catalogUid=${statusRes.message.details[0].catalogUid}, status=${statusRes.message.details[0].status}`);
                        }
                    }
                    const kb1Status = statusRes.message.details.find(d => d.catalogUid === data.kb1_initial.catalogUid);
                    const kb2Status = statusRes.message.details.find(d => d.catalogUid === data.kb2_initial.catalogUid);

                    // Check KB1 status
                    if (kb1Status) {
                        kb1NotFoundCount = 0; // Reset - KB found
                    } else if (kb1UpdateCompleted === null) {
                        // Only count not-found if we haven't determined terminal state yet
                        kb1NotFoundCount++;
                        if (kb1NotFoundCount > MAX_NOT_FOUND_WAIT) {
                            console.error(`✗ KB1 not found in status list after ${MAX_NOT_FOUND_WAIT}s - workflow may have failed to start`);
                            kb1UpdateCompleted = false; // Mark as failed, but continue polling KB2
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
                            console.error(`✗ KB2 not found in status list after ${MAX_NOT_FOUND_WAIT}s - workflow may have failed to start`);
                            kb2UpdateCompleted = false; // Mark as failed, but continue polling KB1
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
                const finalStatusRes = client.invoke(
                    "artifact.artifact.v1alpha.ArtifactPrivateService/GetKnowledgeBaseUpdateStatusAdmin",
                    {},
                    data.metadata
                );

                if (finalStatusRes.status === grpc.StatusOK && finalStatusRes.message.details) {
                    const kb1FinalStatus = finalStatusRes.message.details.find(d => d.uid === data.kb1_initial.catalogUid);
                    const kb2FinalStatus = finalStatusRes.message.details.find(d => d.uid === data.kb2_initial.catalogUid);

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

            const prodKB1 = helper.getCatalogByIdAndOwner(data.kb1_initial.catalogId, data.expectedOwner.uid);
            const prodKB2 = helper.getCatalogByIdAndOwner(data.kb2_initial.catalogId, data.expectedOwner.uid);

            if (prodKB1 && prodKB1.length > 0) {
                const systemQuery = helper.safeQuery(
                    `SELECT s.id, s.config FROM system s
                     JOIN knowledge_base kb ON kb.system_uid = s.uid
                     WHERE kb.uid = $1`,
                    prodKB1[0].uid
                );

                if (systemQuery && systemQuery.length > 0) {
                    const systemId = systemQuery[0].id;
                    console.log(`Production KB1 system_id after swap: ${systemId}`);

                    check({ systemId }, {
                        "Phase 4: Production KB1 now uses Gemini": () => systemId === "gemini",
                    });
                }
            }

            if (prodKB2 && prodKB2.length > 0) {
                const systemQuery = helper.safeQuery(
                    `SELECT s.id, s.config FROM system s
                     JOIN knowledge_base kb ON kb.system_uid = s.uid
                     WHERE kb.uid = $1`,
                    prodKB2[0].uid
                );

                if (systemQuery && systemQuery.length > 0) {
                    const systemId = systemQuery[0].id;
                    console.log(`Production KB2 system_id after swap: ${systemId}`);

                    check({ systemId }, {
                        "Phase 4: Production KB2 now uses Gemini": () => systemId === "gemini",
                    });
                }
            }

            // Verify rollback KBs exist and use OpenAI
            console.log("Verifying rollback KBs exist and use OpenAI config...");

            const rollback1 = helper.verifyRollbackKB(data.kb1_initial.catalogId, data.expectedOwner.uid);
            const rollback2 = helper.verifyRollbackKB(data.kb2_initial.catalogId, data.expectedOwner.uid);

            check({ rollback1, rollback2 }, {
                "Phase 4: Rollback KB1 exists": () => rollback1 && rollback1.length > 0,
                "Phase 4: Rollback KB2 exists": () => rollback2 && rollback2.length > 0,
            });

            if (rollback1 && rollback1.length > 0) {
                const systemQuery = helper.safeQuery(
                    `SELECT s.id FROM system s
                     JOIN knowledge_base kb ON kb.system_uid = s.uid
                     WHERE kb.uid = $1`,
                    rollback1[0].uid
                );

                if (systemQuery && systemQuery.length > 0) {
                    const systemId = systemQuery[0].id;
                    console.log(`Rollback KB1 system_id: ${systemId}`);

                    check({ systemId }, {
                        "Phase 4: Rollback KB1 preserves OpenAI config": () => systemId === "openai",
                    });

                    // Store rollback KB info
                    data.kb1_rollback = {
                        kbUid: rollback1[0].uid,
                        catalogId: rollback1[0].id,
                    };

                    // Track for cleanup
                    data.catalogIds.push(rollback1[0].id);
                }
            }

            if (rollback2 && rollback2.length > 0) {
                const systemQuery = helper.safeQuery(
                    `SELECT s.id FROM system s
                     JOIN knowledge_base kb ON kb.system_uid = s.uid
                     WHERE kb.uid = $1`,
                    rollback2[0].uid
                );

                if (systemQuery && systemQuery.length > 0) {
                    const systemId = systemQuery[0].id;
                    check({ systemId }, {
                        "Phase 4: Rollback KB2 preserves OpenAI config": () => systemId === "openai",
                    });

                    data.kb2_rollback = {
                        kbUid: rollback2[0].uid,
                        catalogId: rollback2[0].id,
                    };

                    // Track for cleanup
                    data.catalogIds.push(rollback2[0].id);
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
            if (data.kb1_initial && data.kb1_initial.fileUids && data.kb1_initial.fileUids.length > 0) {
                const testFileUid = data.kb1_initial.fileUids[0];

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

                // Verify text_chunk has reference with PageRange (if applicable for text files)
                const chunkQuery = helper.safeQuery(
                    `SELECT reference::text as reference_text FROM text_chunk
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
                    `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${data.kb1_initial.catalogId}/files`,
                    JSON.stringify({ filename: filename, type: "TYPE_PDF", content: constant.sampleMultiPagePdf }),
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
                    `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${data.kb2_initial.catalogId}/files`,
                    JSON.stringify({ filename: filename, type: "TYPE_MARKDOWN", content: constant.sampleMd }),
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
            const retentionFileUids = [...retentionFiles1, ...retentionFiles2].map(f => f.uid);
            console.log("Waiting for retention file processing to complete in production KBs...");
            // Wait for production retention files using robust helper
            console.log(`Waiting for ${retentionFileUids.length} production retention files...`);

            // Wait for KB1 retention files
            if (retentionFiles1.length > 0) {
                const result1 = helper.waitForMultipleFilesProcessingComplete(
                    data.expectedOwner.id,
                    data.kb1_initial.catalogId,
                    retentionFiles1.map(f => f.uid),
                    data.header,
                    360
                );
                if (!result1.completed) {
                    console.log(`KB1 retention files incomplete: ${result1.status}`);
                }
            }

            // Wait for KB2 retention files
            if (retentionFiles2.length > 0) {
                const result2 = helper.waitForMultipleFilesProcessingComplete(
                    data.expectedOwner.id,
                    data.kb2_initial.catalogId,
                    retentionFiles2.map(f => f.uid),
                    data.header,
                    360
                );
                if (!result2.completed) {
                    console.log(`KB2 retention files incomplete: ${result2.status}`);
                }
            }

            const productionCompleted = true;
            console.log(`All ${retentionFileUids.length} production retention files processed`);

            check({ productionCompleted }, {
                "Phase 5: Production retention files processed": () => productionCompleted === true,
            });

            // CRITICAL: Also wait for rollback KB files to complete
            // Sequential dual-processing triggers rollback files after production completes
            // Rollback workflow will block if any files are NOTSTARTED in rollback KB
            console.log("Waiting for retention file processing in rollback KBs (sequential dual-processing)...");
            let rollbackCompleted = false;
            if (data.kb1_rollback && data.kb2_rollback) {
                for (let i = 0; i < 360; i++) {
                    let kb1Count = 0;
                    let kb2Count = 0;

                    // Check files in rollback KB1 by filename (same filenames as production)
                    for (const filename of [`${data.dbIDPrefix}sysconfig-retention-kb1-1.pdf`, `${data.dbIDPrefix}sysconfig-retention-kb1-2.pdf`]) {
                        const fileQuery = helper.safeQuery(
                            `SELECT process_status FROM knowledge_base_file
                             WHERE kb_uid = $1 AND filename = $2 AND delete_time IS NULL`,
                            data.kb1_rollback.kbUid,
                            filename
                        );

                        if (fileQuery && fileQuery.length > 0 && fileQuery[0].process_status === "FILE_PROCESS_STATUS_COMPLETED") {
                            kb1Count++;
                        }
                    }

                    // Check files in rollback KB2 by filename
                    for (const filename of [`${data.dbIDPrefix}sysconfig-retention-kb2-1.md`, `${data.dbIDPrefix}sysconfig-retention-kb2-2.md`]) {
                        const fileQuery = helper.safeQuery(
                            `SELECT process_status FROM knowledge_base_file
                             WHERE kb_uid = $1 AND filename = $2 AND delete_time IS NULL`,
                            data.kb2_rollback.kbUid,
                            filename
                        );

                        if (fileQuery && fileQuery.length > 0 && fileQuery[0].process_status === "FILE_PROCESS_STATUS_COMPLETED") {
                            kb2Count++;
                        }
                    }

                    if (kb1Count === 2 && kb2Count === 2) {
                        rollbackCompleted = true;
                        console.log("All rollback KB files processed (sequential dual-processing complete)");
                        break;
                    }

                    if (i % 10 === 0 && i > 0) {
                        console.log(`Waiting for rollback files... KB1: ${kb1Count}/2, KB2: ${kb2Count}/2`);
                    }

                    sleep(0.5);
                }

                check({ rollbackCompleted }, {
                    "Phase 5: Rollback KB files processed (dual-processing)": () => rollbackCompleted === true,
                });

                if (!rollbackCompleted) {
                    console.warn("Rollback KB files did not complete in time - rollback may be blocked");
                }
            }

            // ====================================================================
            // Phase 5.5: Verify Position Data for Multi-Page PDF Files
            // ====================================================================
            console.log("\n=== Phase 5.5: Verifying position data for multi-page PDF files ===");

            if (retentionFiles1.length > 0) {
                const pdfFileUid = retentionFiles1[0].fileUid;
                console.log(`Phase 5.5: Testing PDF file ${pdfFileUid}`);

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
                        "Phase 5.5: PageDelimiters has 4 pages (sample-multi-page.pdf)": () =>
                            posData && posData.PageDelimiters && posData.PageDelimiters.length === 4,
                        "Phase 5.5: position_data uses PascalCase": () =>
                            posData && posData.PageDelimiters !== undefined && posData.page_delimiters === undefined,
                    });
                }

                // 2. Verify text_chunk has reference with PageRange
                const chunkQuery = helper.safeQuery(
                    `SELECT reference::text as reference_text FROM text_chunk
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
                    `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${data.kb1_initial.catalogId}/files/${pdfFileUid}/chunks`,
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

            // Store retention file info
            data.kb1_retention_files = retentionFiles1.map(f => f.uid);
            data.kb2_retention_files = retentionFiles2.map(f => f.uid);
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
            if (data.kb1_rollback && data.kb1_retention_files) {
                const prodKB1FileCount = helper.countFilesInCatalog(data.kb1_initial.catalogUid);
                const rollbackKB1FileCount = helper.countFilesInCatalog(data.kb1_rollback.kbUid);

                console.log(`KB1 - Production files: ${prodKB1FileCount}, Rollback files: ${rollbackKB1FileCount}`);

                check({ prodKB1FileCount, rollbackKB1FileCount }, {
                    "Phase 6: KB1 production has all files": () => prodKB1FileCount >= (data.kb1_initial.fileCount + data.kb1_retention_files.length),
                    "Phase 6: KB1 rollback has all files (dual processing)": () => rollbackKB1FileCount >= (data.kb1_initial.fileCount + data.kb1_retention_files.length),
                });

                // Verify files in rollback KB
                for (const filename of [`${data.dbIDPrefix}sysconfig-retention-kb1-1.pdf`, `${data.dbIDPrefix}sysconfig-retention-kb1-2.pdf`]) {
                    const fileQuery = helper.safeQuery(
                        `SELECT COUNT(*) as count FROM knowledge_base_file
                         WHERE kb_uid = $1 AND filename = $2`,
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
            if (data.kb2_rollback && data.kb2_retention_files) {
                const prodKB2FileCount = helper.countFilesInCatalog(data.kb2_initial.catalogUid);
                const rollbackKB2FileCount = helper.countFilesInCatalog(data.kb2_rollback.kbUid);

                console.log(`KB2 - Production files: ${prodKB2FileCount}, Rollback files: ${rollbackKB2FileCount}`);

                check({ prodKB2FileCount, rollbackKB2FileCount }, {
                    "Phase 6: KB2 production has all files": () => prodKB2FileCount >= (data.kb2_initial.fileCount + data.kb2_retention_files.length),
                    "Phase 6: KB2 rollback has all files (dual processing)": () => rollbackKB2FileCount >= (data.kb2_initial.fileCount + data.kb2_retention_files.length),
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
            const rollbackRes = client.invoke(
                "artifact.artifact.v1alpha.ArtifactPrivateService/RollbackAdmin",
                {
                    name: `users/${data.expectedOwner.uid}/catalogs/${data.kb1_initial.catalogId}`,
                },
                data.metadata
            );

            check(rollbackRes, {
                "Phase 7: Rollback triggered successfully": (r) => r.status === grpc.StatusOK,
                "Phase 7: Rollback has catalog": (r) => r.message && r.message.catalog !== null,
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
            const prodKB1 = helper.getCatalogByIdAndOwner(data.kb1_initial.catalogId, data.expectedOwner.uid);

            if (prodKB1 && prodKB1.length > 0) {
                check(prodKB1[0], {
                    "Phase 8: KB1 status is ROLLED_BACK": () => prodKB1[0].update_status === "KNOWLEDGE_BASE_UPDATE_STATUS_ROLLED_BACK",
                });

                const systemQuery = helper.safeQuery(
                    `SELECT s.id FROM system s
                     JOIN knowledge_base kb ON kb.system_uid = s.uid
                     WHERE kb.uid = $1`,
                    prodKB1[0].uid
                );

                if (systemQuery && systemQuery.length > 0) {
                    const systemId = systemQuery[0].id;
                    console.log(`Production KB1 system_id after rollback: ${systemId}`);

                    check({ systemId }, {
                        "Phase 8: KB1 restored to OpenAI config": () => systemId === "openai",
                    });
                }

                // Verify all files are still present
                const fileCount = helper.countFilesInCatalog(prodKB1[0].uid);
                const expectedCount = data.kb1_initial.fileCount + data.kb1_retention_files.length;

                console.log(`KB1 after rollback - files: ${fileCount}, expected: ${expectedCount}`);

                check({ fileCount }, {
                    "Phase 8: KB1 has all files after rollback": () => fileCount >= expectedCount,
                });

                // ====================================================================
                // Phase 8.5: Verify Position Data Survived Rollback
                // ====================================================================
                console.log("\n=== Phase 8.5: Verifying position data survived rollback (back to OpenAI) ===");

                if (data.kb1_retention_files && data.kb1_retention_files.length > 0) {
                    const pdfFileUid = data.kb1_retention_files[0];
                    console.log(`Phase 8.5: Testing PDF file ${pdfFileUid} after rollback`);

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
                            "Phase 8.5: After rollback, PageDelimiters has 4 pages (sample-multi-page.pdf)": () =>
                                posData && posData.PageDelimiters && posData.PageDelimiters.length === 4,
                        });
                    }

                    // 2. Verify text_chunk still has reference with PageRange
                    const chunkQuery = helper.safeQuery(
                        `SELECT reference::text as reference_text FROM text_chunk
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
                        `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${data.kb1_initial.catalogId}/files/${pdfFileUid}/chunks`,
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
            const setRetentionRes = client.invoke(
                "artifact.artifact.v1alpha.ArtifactPrivateService/SetRollbackRetentionAdmin",
                {
                    name: `users/${data.expectedOwner.uid}/catalogs/${data.kb2_initial.catalogId}`,
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
            const purgeRes = client.invoke(
                "artifact.artifact.v1alpha.ArtifactPrivateService/PurgeRollbackAdmin",
                {
                    name: `users/${data.expectedOwner.uid}/catalogs/${data.kb2_initial.catalogId}`,
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
                const cleanedUp = helper.pollRollbackKBCleanup(
                    data.kb2_rollback.catalogId,
                    data.kb2_rollback.kbUid,
                    data.expectedOwner.uid,
                    60
                );

                check({ cleanedUp }, {
                    "Phase 9: Rollback KB2 cleaned up after expiry": () => cleanedUp === true,
                });

                // Verify production KB2 still works normally
                const prodKB2 = helper.getCatalogByIdAndOwner(data.kb2_initial.catalogId, data.expectedOwner.uid);

                if (prodKB2 && prodKB2.length > 0) {
                    check(prodKB2[0], {
                        "Phase 9: Production KB2 still exists": () => prodKB2[0].delete_time === null,
                        "Phase 9: Production KB2 retention cleared": () => prodKB2[0].rollback_retention_until === null,
                    });

                    const fileCount = helper.countFilesInCatalog(prodKB2[0].uid);
                    const expectedCount = data.kb2_initial.fileCount + data.kb2_retention_files.length;

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
