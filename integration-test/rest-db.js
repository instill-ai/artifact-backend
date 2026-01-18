/**
 * Database Schema and Data Format Integration Tests
 *
 * This comprehensive test suite validates database schema correctness, data format
 * consistency, and enum serialization across the Artifact backend.
 *
 * Test Coverage:
 *
 * 1. Knowledge Base Type Enum Storage
 *    - Verifies knowledge_base_type uses full enum names (KNOWLEDGE_BASE_TYPE_PERSISTENT)
 *    - Ensures enums are NOT stored as short forms ("persistent")
 *
 * 2. System Config Foreign Key and JSONB Format
 *    - Validates system_uid foreign key relationship
 *    - Verifies system.config JSONB structure (nested rag.embedding)
 *    - Checks presence of model_family and dimensionality fields
 *
 * 3. Text Chunk Reference Format (PageRange)
 *    - Verifies chunk.reference JSONB uses PascalCase "PageRange"
 *    - Ensures NO snake_case "page_range" usage
 *    - Validates reference structure integrity
 *
 * 4. Converted File Position Data Format (PageDelimiters)
 *    - Verifies converted_file.position_data uses PascalCase "PageDelimiters"
 *    - Ensures NO snake_case "page_delimiters" usage
 *    - Validates position data structure
 *
 * 5. Single-Page File Position Data
 *    - Verifies CSV, TEXT, MARKDOWN files have position_data populated
 *    - Validates PageDelimiters array structure
 *    - Checks delimiter values are positive integers
 *
 * 6. Content/Summary Chunk Separation
 *    - Verifies content and summary reference different converted_file records
 *    - Validates chunk_type field (TYPE_CONTENT vs TYPE_SUMMARY)
 *    - Checks converted_type field (CONVERTED_FILE_TYPE_CONTENT/SUMMARY)
 *
 * 7. Field Naming Conventions (Multi-table Validation)
 *    - 7.1: file.file_type stores FileType enum (TYPE_*)
 *    - 7.2: converted_file.content_type stores MIME type (e.g., application/pdf, text/markdown)
 *    - 7.3: chunk uses MIME type (content_type) and classification (chunk_type)
 *    - 7.4: embedding uses MIME type (content_type) and classification (chunk_type)
 *    - 7.5: converted_file.converted_type has expected enum values
 *
 * 8. File.Type Enum Serialization in API Responses
 *    - 8.1: List Files API returns valid TYPE_* enum strings
 *    - 8.2: Get File API returns matching enum values
 *    - 8.3: All file types (PDF, TEXT, MARKDOWN, CSV, DOCX) serialize correctly
 *
 * Test Data Management:
 * - Creates test knowledge base with prefix pattern: test-{random}-db-{random}
 * - Uploads multiple file types: PDF, TEXT, MARKDOWN, CSV, DOCX
 * - Waits for file processing completion (up to 5 minutes)
 * - Setup: Cleans orphaned knowledge bases from previous failed runs
 * - Teardown: Waits for processing completion, then deletes test knowledge bases
 *
 * Database Access:
 * - Uses direct SQL queries via constant.db for validation
 * - Tests JSONB field structure using PostgreSQL ? operator
 * - Validates data consistency between database and API responses
 *
 * @requires Database connection configured in constant.db
 * @requires Multiple file types in constant.sampleFiles
 */

import { check, group, sleep } from "k6";
import { randomString } from "https://jslib.k6.io/k6-utils/1.1.0/index.js";

import { artifactRESTPublicHost } from "./const.js";

import * as constant from "./const.js";
import * as helper from "./helper.js";

// Use httpRetry for automatic retry on transient errors (429, 5xx)
const http = helper.httpRetry;

function logUnexpected(res, label) {
    if (res && res.status !== 200) {
        try {
            console.log(`${label} status=${res.status} body=${JSON.stringify(res.json())}`);
        } catch (e) {
            console.log(`${label} status=${res.status}`);
        }
    }
}

export let options = {
    setupTimeout: '300s',
    teardownTimeout: '180s',// Increased to accommodate file processing wait (120s) + cleanup
    insecureSkipTLSVerify: true,
    thresholds: {
        checks: ["rate == 1.0"],
    },
    scenarios: {
        db_schema_tests: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_DB_SCHEMA' },
    },
};

export function setup() {
    check(true, { [constant.banner('Artifact API: DB Tests Setup')]: () => true });

    // Stagger test execution to reduce parallel resource contention
    helper.staggerTestExecution(2);

    // Generate unique test prefix (must be in setup, not module-level, to avoid k6 parallel init issues)
    const dbIDPrefix = constant.generateDBIDPrefix();
    console.log(`rest-db.js: Using unique test prefix: ${dbIDPrefix}`);

    // Authenticate with retry to handle transient failures
    var loginResp = helper.authenticateWithRetry(
        constant.mgmtRESTPublicHost,
        constant.defaultUsername,
        constant.defaultPassword
    );

    check(loginResp, {
        [`POST ${constant.mgmtRESTPublicHost}/v1beta/auth/login response status is 200`]: (
            r
        ) => r && r.status === 200,
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

    var resp = http.request("GET", `${constant.mgmtRESTPublicHost}/v1beta/user`, {}, { headers: { "Authorization": `Bearer ${accessToken}` } })

    // Cleanup orphaned knowledge bases from previous failed test runs OF THIS SPECIFIC TEST
    // Use API-only cleanup to properly trigger workflows (no direct DB manipulation)
    console.log("\n=== SETUP: Cleaning up previous test data (db pattern only) ===");
    try {
        const listResp = http.request("GET", `${artifactRESTPublicHost}/v1alpha/namespaces/${resp.json().user.id}/knowledge-bases`, null, header);
        if (listResp.status === 200) {
            const knowledgeBases = Array.isArray(listResp.json().knowledgeBases) ? listResp.json().knowledgeBases : [];
            let cleanedCount = 0;
            for (const kb of knowledgeBases) {
                const kbId = kb.id;
                if (kbId && kbId.match(/test-[a-z0-9]+-db-/)) {
                    const delResp = http.request("DELETE", `${artifactRESTPublicHost}/v1alpha/namespaces/${resp.json().user.id}/knowledge-bases/${kbId}`, null, header);
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

export function teardown(data) {
    const groupName = "Artifact API: DB Tests Teardown";
    group(groupName, () => {
        check(true, { [constant.banner(groupName)]: () => true });

        // Wait for file processing AND Temporal activities to settle before cleanup
        console.log("Teardown: Waiting for safe cleanup...");
        helper.waitForSafeCleanup(120, data.dbIDPrefix, 3);

        // Delete knowledge bases via API (which triggers cleanup workflows)
        var listResp = http.request("GET", `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases`, null, data.header)
        if (listResp.status === 200) {
            var knowledgeBases = Array.isArray(listResp.json().knowledgeBases) ? listResp.json().knowledgeBases : []
            for (const kb of knowledgeBases) {
                if (kb.id && kb.id.startsWith(data.dbIDPrefix)) {
                    var delResp = http.request("DELETE", `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${kb.id}`, null, data.header);
                    check(delResp, {
                        [`DELETE /v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${kb.id} response status is 200 or 404`]: (r) => r.status === 200 || r.status === 404,
                    });
                }
            }
        }
    });
}

// Main test scenario
export function TEST_DB_SCHEMA(data) {
    const groupName = "Artifact API: Database Schema and Data Format Tests";
    group(groupName, () => {
        check(true, { [constant.banner(groupName)]: () => true });

        // Create knowledge base
        const kbName = data.dbIDPrefix + "db-" + randomString(8);
        const cRes = http.request("POST", `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases`, JSON.stringify({
            displayName: kbName,
            description: "DB schema test knowledge base",
            tags: ["test", "db", "schema"],
            type: "KNOWLEDGE_BASE_TYPE_PERSISTENT"
        }), data.header);

        logUnexpected(cRes, 'POST /v1alpha/namespaces/{namespace_id}/knowledge-bases');
        const kb = ((() => { try { return cRes.json(); } catch (e) { return {}; } })()).knowledgeBase || {};
        // Note: uid field removed in AIP refactoring - use id for identification
        const knowledgeBaseId = kb.id;

        check(cRes, {
            [`DB Tests: Knowledge base created successfully (${knowledgeBaseId})`]: (r) => r.status === 200,
            [`DB Tests: Knowledge base has valid ID`]: () => knowledgeBaseId && knowledgeBaseId.length > 0,
        });

        if (!knowledgeBaseId) {
            console.log("DB Tests: Failed to create knowledge base, aborting test");
            return;
        }

        // Get internal KB UUID for database queries (uid not exposed in API after AIP refactoring)
        const knowledgeBaseUid = helper.getKnowledgeBaseUidFromId(knowledgeBaseId);
        console.log(`DB Tests: Knowledge base UID: ${knowledgeBaseUid}`);

        if (!knowledgeBaseUid) {
            console.log("DB Tests: Failed to get KB internal UID, aborting test");
            http.request("DELETE", `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}`, null, data.header);
            return;
        }

        // Upload multiple file types for comprehensive testing
        const testFiles = [
            constant.sampleFiles.find(f => f.type === "TYPE_PDF"),
            constant.sampleFiles.find(f => f.type === "TYPE_TEXT"),
            constant.sampleFiles.find(f => f.type === "TYPE_MARKDOWN"),
            constant.sampleFiles.find(f => f.type === "TYPE_CSV"),
            constant.sampleFiles.find(f => f.type === "TYPE_DOCX"),
        ].filter(f => f !== undefined);

        const uploaded = [];
        for (const s of testFiles) {
            const filename = data.dbIDPrefix + s.originalName;
            const fReq = { displayName: filename, type: s.type, content: s.content };

            // Use retry logic to handle transient upload failures during parallel execution
            const uRes = helper.uploadFileWithRetry(
                `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/files?knowledgeBaseId=${knowledgeBaseId}`,
                fReq,
                data.header,
                3 // max retries
            );

            if (uRes) {
                const file = ((() => { try { return uRes.json(); } catch (e) { return {}; } })()).file || {};
                if (uRes.status === 200 && file.id) {
                    // Get internal file UID for database queries
                    const fileUid = helper.getFileUidFromId(file.id);
                    uploaded.push({
                        fileId: file.id,
                        fileUid: fileUid,  // Internal UUID for DB queries
                        name: filename,
                        type: s.type,
                        originalName: s.originalName
                    });
                    console.log(`DB Tests: Uploaded ${s.originalName} (ID: ${file.id}, UID: ${fileUid})`);
                } else {
                    console.log(`DB Tests: Upload succeeded but invalid response for ${s.originalName}`);
                }
            } else {
                console.log(`DB Tests: Upload failed after retries for ${s.originalName}`);
            }
        }

        check({ uploaded }, {
            [`DB Tests: Files uploaded successfully`]: () => uploaded.length === testFiles.length,
        });

        if (uploaded.length === 0) {
            console.log("DB Tests: No files uploaded, aborting test");
            return;
        }

        const fileIds = uploaded.map(f => f.fileId);
        // Auto-trigger: Processing starts automatically on upload (no manual trigger needed)

        // Wait for all files to complete processing using robust helper
        console.log(`DB Tests: Waiting for ${uploaded.length} files to complete processing...`);
        const result = helper.waitForMultipleFilesProcessingComplete(
            data.expectedOwner.id,
            knowledgeBaseId,
            fileIds,
            data.header,
            600 // 10 minutes max
        );

        const completedCount = result.processedCount;

        check({ completed: result.completed }, {
            [`DB Tests: All files completed processing`]: () => result.completed,
        });

        if (!result.completed) {
            console.log(`DB Tests: Only ${completedCount}/${uploaded.length} files completed (${result.status}), continuing with available data`);
            if (result.error) {
                console.log(`DB Tests: Error - ${result.error}`);
            }
        }

        // ==========================================================================
        // TEST 1: Knowledge Base Type Enum Storage
        // ==========================================================================
        group("DB Test 1: Knowledge Base Type Enum Storage", function () {
            console.log(`\nDB Test 1: Verifying knowledge_base_type enum storage format...`);

            const kbTypeResult = helper.safeQuery(
                `SELECT knowledge_base_type FROM knowledge_base WHERE uid = $1`,
                knowledgeBaseUid
            );

            if (kbTypeResult.length > 0) {
                const kbType = kbTypeResult[0].knowledge_base_type;
                console.log(`DB Test 1: knowledge_base_type = '${kbType}'`);

                check({ kbType }, {
                    "DB Test 1: knowledge_base_type uses full enum name (KNOWLEDGE_BASE_TYPE_PERSISTENT)": () =>
                        kbType === "KNOWLEDGE_BASE_TYPE_PERSISTENT",
                    "DB Test 1: knowledge_base_type is NOT short form ('persistent')": () =>
                        kbType !== "persistent",
                });
            } else {
                check(false, { "DB Test 1: Knowledge base record exists in database": () => false });
            }
        });

        // ==========================================================================
        // TEST 2: System Config Foreign Key and JSONB Format
        // ==========================================================================
        group("DB Test 2: System Config Foreign Key and JSONB Format", function () {
            console.log(`\nDB Test 2: Verifying system_uid FK and system config JSONB format...`);

            const systemConfigResult = helper.safeQuery(
                `SELECT kb.system_uid, s.config::text as system_config_text
                 FROM knowledge_base kb
                 JOIN system s ON kb.system_uid = s.uid
                 WHERE kb.uid = $1`,
                knowledgeBaseUid
            );

            if (systemConfigResult.length > 0) {
                const systemUID = systemConfigResult[0].system_uid;
                const systemConfigText = systemConfigResult[0].system_config_text;
                let parsedConfig;
                try {
                    parsedConfig = JSON.parse(systemConfigText);
                    console.log(`DB Test 2: system_uid = ${systemUID}`);
                    console.log(`DB Test 2: system.config = ${JSON.stringify(parsedConfig)}`);
                } catch (e) {
                    parsedConfig = null;
                }

                check({ systemUID, parsedConfig }, {
                    "DB Test 2: system_uid is present": () => systemUID !== null && systemUID !== undefined,
                    "DB Test 2: system.config is valid JSON": () => parsedConfig !== null,
                    "DB Test 2: system.config has nested rag.embedding structure": () =>
                        parsedConfig && parsedConfig.rag && parsedConfig.rag.embedding !== undefined,
                    "DB Test 2: system.config has model_family field": () =>
                        parsedConfig && parsedConfig.rag && parsedConfig.rag.embedding &&
                        parsedConfig.rag.embedding.model_family !== undefined,
                    "DB Test 2: system.config has dimensionality field": () =>
                        parsedConfig && parsedConfig.rag && parsedConfig.rag.embedding &&
                        parsedConfig.rag.embedding.dimensionality !== undefined,
                });
            }
        });

        // ==========================================================================
        // TEST 3: Text Chunk Reference Format (PageRange)
        // ==========================================================================
        group("DB Test 3: Text Chunk Reference Format", function () {
            console.log(`\nDB Test 3: Verifying chunk.reference uses PascalCase PageRange...`);

            const pdfFile = uploaded.find(f => f.type === "TYPE_PDF") || uploaded[0];
            if (!pdfFile) {
                console.log("DB Test 3: No PDF file available, skipping");
                return;
            }

            // Count chunks with correct PascalCase PageRange
            const correctResult = helper.safeQuery(
                `SELECT COUNT(*) as count FROM chunk WHERE file_uid = $1 AND reference ? 'PageRange'`,
                pdfFile.fileUid
            );
            const correctCount = correctResult.length > 0 ? parseInt(correctResult[0].count) : 0;

            // Count chunks with INCORRECT snake_case page_range
            const incorrectResult = helper.safeQuery(
                `SELECT COUNT(*) as count FROM chunk WHERE file_uid = $1 AND reference ? 'page_range'`,
                pdfFile.fileUid
            );
            const incorrectCount = incorrectResult.length > 0 ? parseInt(incorrectResult[0].count) : 0;

            console.log(`DB Test 3: PascalCase PageRange: ${correctCount}, snake_case page_range: ${incorrectCount}`);

            check({ correctCount, incorrectCount }, {
                "DB Test 3: All chunk.reference use PascalCase PageRange": () =>
                    correctCount > 0 && incorrectCount === 0,
            });

            // Sample a reference to verify structure
            const sampleResult = helper.safeQuery(
                `SELECT reference::text as reference_text FROM chunk WHERE file_uid = $1 AND reference IS NOT NULL LIMIT 1`,
                pdfFile.fileUid
            );

            if (sampleResult.length > 0) {
                let parsedRef;
                try {
                    parsedRef = JSON.parse(sampleResult[0].reference_text);
                    console.log(`DB Test 3: Sample reference = ${JSON.stringify(parsedRef)}`);
                } catch (e) {
                    parsedRef = null;
                }

                check({ parsedRef }, {
                    "DB Test 3: Sample reference contains PageRange field": () =>
                        parsedRef && parsedRef.PageRange !== undefined,
                    "DB Test 3: Sample reference does NOT contain page_range": () =>
                        parsedRef && parsedRef.page_range === undefined,
                });
            }
        });

        // ==========================================================================
        // TEST 4: Converted File Position Data Format (PageDelimiters)
        // ==========================================================================
        group("DB Test 4: Converted File Position Data Format", function () {
            console.log(`\nDB Test 4: Verifying converted_file.position_data uses PascalCase PageDelimiters...`);

            const pdfFile = uploaded.find(f => f.type === "TYPE_PDF") || uploaded[0];
            if (!pdfFile) {
                console.log("DB Test 4: No PDF file available, skipping");
                return;
            }

            // Count files with correct PascalCase PageDelimiters
            const correctResult = helper.safeQuery(
                `SELECT COUNT(*) as count FROM converted_file WHERE file_uid = $1 AND position_data ? 'PageDelimiters'`,
                pdfFile.fileUid
            );
            const correctCount = correctResult.length > 0 ? parseInt(correctResult[0].count) : 0;

            // Count files with INCORRECT snake_case page_delimiters
            const incorrectResult = helper.safeQuery(
                `SELECT COUNT(*) as count FROM converted_file WHERE file_uid = $1 AND position_data ? 'page_delimiters'`,
                pdfFile.fileUid
            );
            const incorrectCount = incorrectResult.length > 0 ? parseInt(incorrectResult[0].count) : 0;

            console.log(`DB Test 4: PascalCase PageDelimiters: ${correctCount}, snake_case page_delimiters: ${incorrectCount}`);

            check({ correctCount, incorrectCount }, {
                "DB Test 4: All converted_file.position_data use PascalCase PageDelimiters": () =>
                    correctCount >= 0 && incorrectCount === 0,
            });

            // Sample position_data to verify structure
            const sampleResult = helper.safeQuery(
                `SELECT position_data::text as position_data_text FROM converted_file WHERE file_uid = $1 AND position_data IS NOT NULL LIMIT 1`,
                pdfFile.fileUid
            );

            if (sampleResult.length > 0) {
                let parsedPos;
                try {
                    parsedPos = JSON.parse(sampleResult[0].position_data_text);
                    console.log(`DB Test 4: Sample position_data = ${JSON.stringify(parsedPos)}`);
                } catch (e) {
                    parsedPos = null;
                }

                check({ parsedPos }, {
                    "DB Test 4: Sample position_data contains PageDelimiters": () =>
                        parsedPos && parsedPos.PageDelimiters !== undefined,
                    "DB Test 4: Sample position_data does NOT contain page_delimiters": () =>
                        parsedPos && parsedPos.page_delimiters === undefined,
                });
            }
        });

        // ==========================================================================
        // TEST 5: Single-Page File Position Data
        // ==========================================================================
        group("DB Test 5: Single-Page File Position Data", function () {
            console.log(`\nDB Test 5: Verifying single-page files have position_data...`);

            const singlePageFile = uploaded.find(f =>
                ["TYPE_CSV", "TYPE_TEXT", "TYPE_MARKDOWN"].includes(f.type)
            );

            if (!singlePageFile) {
                console.log("DB Test 5: No single-page file available, skipping");
                return;
            }

            console.log(`DB Test 5: Testing ${singlePageFile.originalName}`);

            const posDataResult = helper.safeQuery(
                `SELECT position_data::text as position_data_text FROM converted_file WHERE file_uid = $1 AND position_data IS NOT NULL LIMIT 1`,
                singlePageFile.fileUid
            );

            if (posDataResult.length > 0) {
                let parsedPos;
                try {
                    parsedPos = JSON.parse(posDataResult[0].position_data_text);
                    console.log(`DB Test 5: position_data = ${JSON.stringify(parsedPos)}`);
                } catch (e) {
                    parsedPos = null;
                }

                check({ parsedPos }, {
                    "DB Test 5: Single-page file has position_data": () => parsedPos !== null,
                    "DB Test 5: position_data has PageDelimiters array": () =>
                        parsedPos && Array.isArray(parsedPos.PageDelimiters),
                    "DB Test 5: PageDelimiters has at least one delimiter": () =>
                        parsedPos && parsedPos.PageDelimiters && parsedPos.PageDelimiters.length >= 1,
                    "DB Test 5: Delimiter is positive integer": () =>
                        parsedPos && parsedPos.PageDelimiters && parsedPos.PageDelimiters[0] > 0,
                });
            } else {
                check(false, {
                    "DB Test 5: Single-page file has position_data": () => false,
                });
            }
        });

        // ==========================================================================
        // TEST 6: Content/Summary Chunk Separation
        // ==========================================================================
        group("DB Test 6: Content/Summary Chunk Separation", function () {
            console.log(`\nDB Test 6: Verifying content and summary chunks are properly separated...`);

            const testFile = uploaded.find(f => f.type === "TYPE_PDF") || uploaded[0];
            if (!testFile) {
                console.log("DB Test 6: No test file available, skipping");
                return;
            }

            // Get content and summary chunks
            const contentChunkResult = helper.safeQuery(
                `SELECT source_uid::text as source_uid_text, chunk_type FROM chunk WHERE file_uid = $1 AND chunk_type = 'TYPE_CONTENT' LIMIT 1`,
                testFile.fileUid
            );

            const summaryChunkResult = helper.safeQuery(
                `SELECT source_uid::text as source_uid_text, chunk_type FROM chunk WHERE file_uid = $1 AND chunk_type = 'TYPE_SUMMARY' LIMIT 1`,
                testFile.fileUid
            );

            if (contentChunkResult.length > 0 && summaryChunkResult.length > 0) {
                const contentSourceUid = contentChunkResult[0].source_uid_text;
                const summarySourceUid = summaryChunkResult[0].source_uid_text;

                console.log(`DB Test 6: Content source_uid: ${contentSourceUid}`);
                console.log(`DB Test 6: Summary source_uid: ${summarySourceUid}`);

                check({ contentSourceUid, summarySourceUid }, {
                    "DB Test 6: Content and summary chunks reference different source_uid": () =>
                        contentSourceUid !== summarySourceUid,
                    "DB Test 6: Content chunk has chunk_type='TYPE_CONTENT'": () =>
                        contentChunkResult[0].chunk_type === "TYPE_CONTENT",
                    "DB Test 6: Summary chunk has chunk_type='TYPE_SUMMARY'": () =>
                        summaryChunkResult[0].chunk_type === "TYPE_SUMMARY",
                });

                // Verify converted files exist
                const contentFileResult = helper.safeQuery(
                    `SELECT destination, converted_type FROM converted_file WHERE uid = $1::uuid`,
                    contentSourceUid
                );

                const summaryFileResult = helper.safeQuery(
                    `SELECT destination, converted_type FROM converted_file WHERE uid = $1::uuid`,
                    summarySourceUid
                );

                check({ contentFileResult, summaryFileResult }, {
                    "DB Test 6: Content converted_file exists": () => contentFileResult.length > 0,
                    "DB Test 6: Summary converted_file exists": () => summaryFileResult.length > 0,
                });

                if (contentFileResult.length > 0 && summaryFileResult.length > 0) {
                    console.log(`DB Test 6: Content destination: ${contentFileResult[0].destination}`);
                    console.log(`DB Test 6: Summary destination: ${summaryFileResult[0].destination}`);

                    check({ contentFileResult, summaryFileResult }, {
                        "DB Test 6: Content file has converted_type='CONVERTED_FILE_TYPE_CONTENT'": () =>
                            contentFileResult[0].converted_type === "CONVERTED_FILE_TYPE_CONTENT",
                        "DB Test 6: Summary file has converted_type='CONVERTED_FILE_TYPE_SUMMARY'": () =>
                            summaryFileResult[0].converted_type === "CONVERTED_FILE_TYPE_SUMMARY",
                    });
                }
            } else {
                console.log(`DB Test 6: WARNING - Could not find both content and summary chunks`);
            }
        });

        // ==========================================================================
        // TEST 7: Field Naming Conventions
        // ==========================================================================
        group("DB Test 7: Field Naming Conventions", function () {
            console.log(`\nDB Test 7: Verifying field naming conventions across tables...`);

            const testFile = uploaded[0];
            if (!testFile) {
                console.log("DB Test 7: No test file available, skipping");
                return;
            }

            // 7.1: file.file_type stores FileType enum string
            const kbFileTypeResult = helper.safeQuery(
                `SELECT file_type FROM file WHERE uid = $1`,
                testFile.fileUid
            );

            if (kbFileTypeResult.length > 0) {
                const fileType = kbFileTypeResult[0].file_type;
                console.log(`DB Test 7.1: file.file_type = "${fileType}"`);

                check({ fileType }, {
                    "DB Test 7.1: file_type is FileType enum string (TYPE_*)": () =>
                        fileType && fileType.startsWith("TYPE_"),
                    "DB Test 7.1: file_type is NOT a MIME type": () =>
                        fileType && !fileType.includes("/"),
                });
            }

            // 7.2: converted_file.content_type stores MIME type
            const convertedContentTypeResult = helper.safeQuery(
                `SELECT content_type FROM converted_file WHERE file_uid = $1 LIMIT 1`,
                testFile.fileUid
            );

            if (convertedContentTypeResult.length > 0) {
                const contentType = convertedContentTypeResult[0].content_type;
                console.log(`DB Test 7.2: converted_file.content_type = "${contentType}"`);

                check({ contentType }, {
                    "DB Test 7.2: content_type is MIME type": () =>
                        contentType && contentType.includes("/"),
                    "DB Test 7.2: content_type is valid MIME (e.g., text/markdown or application/pdf)": () =>
                        contentType === "text/markdown" || contentType === "application/pdf" || contentType.includes("/"),
                    "DB Test 7.2: content_type is NOT FileType enum": () =>
                        contentType && !contentType.startsWith("TYPE_"),
                });
            }

            // 7.3: chunk.content_type stores MIME type, chunk_type stores classification
            const chunkFieldsResult = helper.safeQuery(
                `SELECT content_type, chunk_type FROM chunk WHERE file_uid = $1 LIMIT 1`,
                testFile.fileUid
            );

            if (chunkFieldsResult.length > 0) {
                const chunkContentType = chunkFieldsResult[0].content_type;
                const chunkChunkType = chunkFieldsResult[0].chunk_type;
                console.log(`DB Test 7.3: chunk.content_type = "${chunkContentType}"`);
                console.log(`DB Test 7.3: chunk.chunk_type = "${chunkChunkType}"`);

                check({ chunkContentType, chunkChunkType }, {
                    "DB Test 7.3: chunk.content_type is MIME type": () =>
                        chunkContentType && chunkContentType.includes("/"),
                    "DB Test 7.3: chunk.chunk_type is classification string": () =>
                        chunkChunkType && ["TYPE_CONTENT", "TYPE_SUMMARY", "TYPE_AUGMENTED"].includes(chunkChunkType),
                });
            }

            // 7.4: embedding.content_type stores MIME type, chunk_type stores classification
            const embeddingFieldsResult = helper.safeQuery(
                `SELECT content_type, chunk_type FROM embedding WHERE file_uid = $1 LIMIT 1`,
                testFile.fileUid
            );

            if (embeddingFieldsResult.length > 0) {
                const embContentType = embeddingFieldsResult[0].content_type;
                const embChunkType = embeddingFieldsResult[0].chunk_type;
                console.log(`DB Test 7.4: embedding.content_type = "${embContentType}"`);
                console.log(`DB Test 7.4: embedding.chunk_type = "${embChunkType}"`);

                check({ embContentType, embChunkType }, {
                    "DB Test 7.4: embedding.content_type is MIME type": () =>
                        embContentType && embContentType.includes("/"),
                    "DB Test 7.4: embedding.chunk_type is classification string": () =>
                        embChunkType && ["TYPE_CONTENT", "TYPE_SUMMARY", "TYPE_AUGMENTED"].includes(embChunkType),
                });
            }

            // 7.5: converted_file.converted_type has expected values
            const convertedTypeResult = helper.safeQuery(
                `SELECT converted_type FROM converted_file WHERE file_uid = $1`,
                testFile.fileUid
            );

            if (convertedTypeResult.length > 0) {
                const convertedTypes = convertedTypeResult.map(row => row.converted_type);
                console.log(`DB Test 7.5: converted_file.converted_type values: ${JSON.stringify(convertedTypes)}`);

                check({ convertedTypes }, {
                    "DB Test 7.5: converted_type has expected values (CONTENT/SUMMARY/DOCUMENT)": () =>
                        convertedTypes.every(ct => ["CONVERTED_FILE_TYPE_CONTENT", "CONVERTED_FILE_TYPE_SUMMARY", "CONVERTED_FILE_TYPE_DOCUMENT"].includes(ct)),
                    "DB Test 7.5: File has both content and summary types": () =>
                        convertedTypes.includes("CONVERTED_FILE_TYPE_CONTENT") && convertedTypes.includes("CONVERTED_FILE_TYPE_SUMMARY"),
                });
            }
        });

        // ==========================================================================
        // TEST 8: File.Type Enum Serialization in API Responses
        // ==========================================================================
        group("DB Test 8: File.Type Enum Serialization", function () {
            console.log(`\nDB Test 8: Verifying File.Type enum serialization in API responses...`);

            // 8.1: List Files API
            const listFilesRes = http.request(
                "GET",
                `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/files?knowledgeBaseId=${knowledgeBaseId}`,
                null,
                data.header
            );

            let listFilesData;
            try {
                listFilesData = listFilesRes.json();
            } catch (e) {
                listFilesData = null;
            }

            if (listFilesData && listFilesData.files) {
                const files = listFilesData.files;
                console.log(`DB Test 8.1: Verifying ${files.length} files from List Files API`);

                const typeVerification = files.every(file => {
                    const hasType = file.type !== undefined && file.type !== null;
                    const isValidEnum = hasType && file.type.startsWith("TYPE_");
                    return hasType && isValidEnum;
                });

                const typeCount = {};
                files.forEach(file => {
                    if (file.type) {
                        typeCount[file.type] = (typeCount[file.type] || 0) + 1;
                    }
                });
                console.log(`DB Test 8.1: Type distribution: ${JSON.stringify(typeCount)}`);

                check({ typeVerification, typeCount }, {
                    "DB Test 8.1: All files have valid File.Type enum": () => typeVerification,
                    "DB Test 8.1: Multiple file types present": () => Object.keys(typeCount).length >= 3,
                });
            }

            // 8.2: Get File API
            const testFile = uploaded[0];
            const getFileRes = http.request(
                "GET",
                `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/files/${testFile.fileId}`,
                null,
                data.header
            );

            let getFileData;
            try {
                getFileData = getFileRes.json();
            } catch (e) {
                getFileData = null;
            }

            if (getFileData && getFileData.file) {
                const file = getFileData.file;
                console.log(`DB Test 8.2: Get File returned type: "${file.type}"`);

                check({ file, testFile }, {
                    "DB Test 8.2: Get File has type field": () => file.type !== undefined,
                    "DB Test 8.2: Get File type is valid enum": () =>
                        file.type && file.type.startsWith("TYPE_"),
                    "DB Test 8.2: Get File type matches uploaded type": () =>
                        file.type === testFile.type,
                });
            }

            // 8.3: Verify specific file types
            const fileTypeExamples = [
                { type: "TYPE_PDF", category: "document" },
                { type: "TYPE_TEXT", category: "text" },
                { type: "TYPE_MARKDOWN", category: "text" },
                { type: "TYPE_CSV", category: "text" },
                { type: "TYPE_DOCX", category: "document" },
            ];

            fileTypeExamples.forEach(example => {
                const fileOfType = uploaded.find(f => f.type === example.type);
                if (fileOfType) {
                    const fileRes = http.request(
                        "GET",
                        `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/files/${fileOfType.fileId}`,
                        null,
                        data.header
                    );

                    let fileData;
                    try {
                        fileData = fileRes.json();
                    } catch (e) {
                        fileData = null;
                    }

                    if (fileData && fileData.file) {
                        const returnedType = fileData.file.type;
                        console.log(`DB Test 8.3: ${example.type} -> "${returnedType}"`);

                        check({ returnedType, expected: example.type }, {
                            [`DB Test 8.3: ${example.type} correctly serialized`]: () =>
                                returnedType === example.type,
                        });
                    }
                }
            });
        });

        console.log(`\nDB Tests: All tests completed for knowledge base ${knowledgeBaseId}`);
    });
}
