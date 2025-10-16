import http from "k6/http";
import { check, group, sleep } from "k6";
import { randomString } from "https://jslib.k6.io/k6-utils/1.1.0/index.js";

import { artifactPublicHost } from "./const.js";

import * as constant from "./const.js";
import * as helper from "./helper.js";

/**
 * Database Schema and Data Format Integration Tests
 *
 * This test suite verifies that the database schema and data formats are correct:
 * 1. Enum Storage: Verify enum values are stored as full strings (e.g., "CATALOG_TYPE_PERSISTENT")
 * 2. JSONB Formats: Verify JSONB fields use PascalCase (e.g., PageRange, PageDelimiters)
 * 3. Field Naming: Verify consistent field naming conventions across tables
 * 4. Content/Summary Separation: Verify content and summary are stored in separate converted_file records
 * 5. File.Type Enum: Verify File.Type enum is correctly serialized in API responses
 * 6. Position Data: Verify single-page files (CSV, TXT, MD, HTML) have position_data populated
 */

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

    // Clean up any leftover test data from previous runs
    try {
        constant.db.exec(`DELETE FROM text_chunk WHERE kb_file_uid IN (SELECT uid FROM knowledge_base_file WHERE name LIKE '${constant.dbIDPrefix}%')`);
        constant.db.exec(`DELETE FROM embedding WHERE kb_file_uid IN (SELECT uid FROM knowledge_base_file WHERE name LIKE '${constant.dbIDPrefix}%')`);
        constant.db.exec(`DELETE FROM converted_file WHERE file_uid IN (SELECT uid FROM knowledge_base_file WHERE name LIKE '${constant.dbIDPrefix}%')`);
        constant.db.exec(`DELETE FROM knowledge_base_file WHERE name LIKE '${constant.dbIDPrefix}%'`);
        constant.db.exec(`DELETE FROM knowledge_base WHERE id LIKE '${constant.dbIDPrefix}%'`);
    } catch (e) {
        console.log(`Setup cleanup warning: ${e}`);
    }

    var loginResp = http.request("POST", `${constant.mgmtPublicHost}/v1beta/auth/login`, JSON.stringify({
        "username": constant.defaultUsername,
        "password": constant.defaultPassword,
    }))

    check(loginResp, {
        [`POST ${constant.mgmtPublicHost}/v1beta/auth/login response status is 200`]: (
            r
        ) => r.status === 200,
    });

    var header = {
        "headers": {
            "Authorization": `Bearer ${loginResp.json().accessToken}`,
            "Content-Type": "application/json",
        },
        "timeout": "600s",
    }

    var resp = http.request("GET", `${constant.mgmtPublicHost}/v1beta/user`, {}, { headers: { "Authorization": `Bearer ${loginResp.json().accessToken}` } })
    return { header: header, expectedOwner: resp.json().user }
}

export function teardown(data) {
    const groupName = "Artifact API: DB Tests Teardown";
    group(groupName, () => {
        check(true, { [constant.banner(groupName)]: () => true });

        // Delete catalogs via API (which triggers cleanup workflows)
        var listResp = http.request("GET", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`, null, data.header)
        if (listResp.status === 200) {
            var catalogs = Array.isArray(listResp.json().catalogs) ? listResp.json().catalogs : []
            for (const catalog of catalogs) {
                if (catalog.catalogId && catalog.catalogId.startsWith(constant.dbIDPrefix)) {
                    var delResp = http.request("DELETE", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalog.catalogId}`, null, data.header);
                    check(delResp, {
                        [`DELETE /v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalog.catalogId} response status is 200 or 404`]: (r) => r.status === 200 || r.status === 404,
                    });
                }
            }
        }

        // Final DB cleanup (defensive - in case workflows didn't complete)
        try {
            constant.db.exec(`DELETE FROM text_chunk WHERE kb_file_uid IN (SELECT uid FROM knowledge_base_file WHERE name LIKE '${constant.dbIDPrefix}%')`);
            constant.db.exec(`DELETE FROM embedding WHERE kb_file_uid IN (SELECT uid FROM knowledge_base_file WHERE name LIKE '${constant.dbIDPrefix}%')`);
            constant.db.exec(`DELETE FROM converted_file WHERE file_uid IN (SELECT uid FROM knowledge_base_file WHERE name LIKE '${constant.dbIDPrefix}%')`);
            constant.db.exec(`DELETE FROM knowledge_base_file WHERE name LIKE '${constant.dbIDPrefix}%'`);
            constant.db.exec(`DELETE FROM knowledge_base WHERE id LIKE '${constant.dbIDPrefix}%'`);
        } catch (e) {
            console.log(`Teardown DB cleanup warning: ${e}`);
        }

        constant.db.close();
    });
}

// Main test scenario
export function TEST_DB_SCHEMA(data) {
    const groupName = "Artifact API: Database Schema and Data Format Tests";
    group(groupName, () => {
        check(true, { [constant.banner(groupName)]: () => true });

        // Create catalog
        const catalogName = constant.dbIDPrefix + "db-" + randomString(8);
        const cRes = http.request("POST", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`, JSON.stringify({
            name: catalogName,
            description: "DB schema test catalog",
            tags: ["test", "db", "schema"],
            type: "CATALOG_TYPE_PERSISTENT"
        }), data.header);

        logUnexpected(cRes, 'POST /v1alpha/namespaces/{namespace_id}/catalogs');
        const catalog = ((() => { try { return cRes.json(); } catch (e) { return {}; } })()).catalog || {};
        const catalogId = catalog.catalogId;
        const catalogUid = catalog.catalogUid;

        check(cRes, {
            [`DB Tests: Catalog created successfully (${catalogId})`]: (r) => r.status === 200,
            [`DB Tests: Catalog has valid UID`]: () => catalogUid && catalogUid.length > 0,
        });

        if (!catalogId || !catalogUid) {
            console.log("DB Tests: Failed to create catalog, aborting test");
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
            const fileName = constant.dbIDPrefix + s.originalName;
            const fReq = { name: fileName, type: s.type, content: s.content };
            const uRes = http.request("POST", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files`, JSON.stringify(fReq), data.header);

            const file = ((() => { try { return uRes.json(); } catch (e) { return {}; } })()).file || {};
            if (uRes.status === 200 && file.fileUid) {
                uploaded.push({
                    fileUid: file.fileUid,
                    name: fileName,
                    type: s.type,
                    originalName: s.originalName
                });
                console.log(`DB Tests: Uploaded ${s.originalName} (${file.fileUid})`);
            }
        }

        check({ uploaded }, {
            [`DB Tests: Files uploaded successfully`]: () => uploaded.length === testFiles.length,
        });

        if (uploaded.length === 0) {
            console.log("DB Tests: No files uploaded, aborting test");
            return;
        }

        // Trigger processing for all files
        const fileUids = uploaded.map(f => f.fileUid);
        const pRes = http.request("POST", `${artifactPublicHost}/v1alpha/catalogs/files/processAsync`, JSON.stringify({ fileUids }), data.header);
        check(pRes, { [`DB Tests: Batch processing triggered`]: (r) => r.status === 200 });

        // Poll for completion
        console.log(`DB Tests: Waiting for ${uploaded.length} files to complete processing...`);
        let completedCount = 0;
        const pending = new Set(fileUids);
        const startTime = Date.now();
        const maxWaitMs = 5 * 60 * 1000; // 5 minutes

        while (pending.size > 0 && (Date.now() - startTime) < maxWaitMs) {
            const batch = http.batch(
                Array.from(pending).map((uid) => ({
                    method: "GET",
                    url: `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files/${uid}`,
                    params: data.header,
                }))
            );

            let idx = 0;
            for (const uid of Array.from(pending)) {
                const r = batch[idx++];
                try {
                    const body = r.json();
                    const st = (body.file && body.file.processStatus) || "";
                    if (r.status === 200 && st === "FILE_PROCESS_STATUS_COMPLETED") {
                        pending.delete(uid);
                        completedCount++;
                        console.log(`DB Tests: Completed ${completedCount}/${uploaded.length}`);
                    } else if (r.status === 200 && st === "FILE_PROCESS_STATUS_FAILED") {
                        pending.delete(uid);
                        console.log(`DB Tests: Failed: ${uid} - ${(body.file && body.file.processOutcome) || "Unknown"}`);
                    }
                } catch (e) { }
            }

            if (pending.size === 0) break;
            sleep(0.5);
        }

        check({ completedCount }, {
            [`DB Tests: All files completed processing`]: () => completedCount === uploaded.length,
        });

        if (completedCount !== uploaded.length) {
            console.log(`DB Tests: Only ${completedCount}/${uploaded.length} files completed, continuing with available data`);
        }

        // ==========================================================================
        // TEST 1: Catalog Type Enum Storage
        // ==========================================================================
        group("DB Test 1: Catalog Type Enum Storage", function () {
            console.log(`\nDB Test 1: Verifying catalog_type enum storage format...`);

            const catalogTypeResult = constant.db.query(
                `SELECT catalog_type FROM knowledge_base WHERE uid = $1`,
                catalogUid
            );

            if (catalogTypeResult.length > 0) {
                const catalogType = catalogTypeResult[0].catalog_type;
                console.log(`DB Test 1: catalog_type = '${catalogType}'`);

                check({ catalogType }, {
                    "DB Test 1: catalog_type uses full enum name (CATALOG_TYPE_PERSISTENT)": () =>
                        catalogType === "CATALOG_TYPE_PERSISTENT",
                    "DB Test 1: catalog_type is NOT short form ('persistent')": () =>
                        catalogType !== "persistent",
                });
            } else {
                check(false, { "DB Test 1: Catalog record exists in database": () => false });
            }
        });

        // ==========================================================================
        // TEST 2: Embedding Config JSONB Format
        // ==========================================================================
        group("DB Test 2: Embedding Config JSONB Format", function () {
            console.log(`\nDB Test 2: Verifying embedding_config JSONB format...`);

            const embeddingConfigResult = constant.db.query(
                `SELECT embedding_config::text as embedding_config_text FROM knowledge_base WHERE uid = $1`,
                catalogUid
            );

            if (embeddingConfigResult.length > 0) {
                const embeddingConfigText = embeddingConfigResult[0].embedding_config_text;
                let parsedConfig;
                try {
                    parsedConfig = JSON.parse(embeddingConfigText);
                    console.log(`DB Test 2: embedding_config = ${JSON.stringify(parsedConfig)}`);
                } catch (e) {
                    parsedConfig = null;
                }

                check({ parsedConfig }, {
                    "DB Test 2: embedding_config is valid JSON": () => parsedConfig !== null,
                    "DB Test 2: embedding_config has model_family field": () =>
                        parsedConfig && parsedConfig.model_family !== undefined,
                    "DB Test 2: embedding_config has dimensionality field": () =>
                        parsedConfig && parsedConfig.dimensionality !== undefined,
                });
            }
        });

        // ==========================================================================
        // TEST 3: Text Chunk Reference Format (PageRange)
        // ==========================================================================
        group("DB Test 3: Text Chunk Reference Format", function () {
            console.log(`\nDB Test 3: Verifying text_chunk.reference uses PascalCase PageRange...`);

            const pdfFile = uploaded.find(f => f.type === "TYPE_PDF") || uploaded[0];
            if (!pdfFile) {
                console.log("DB Test 3: No PDF file available, skipping");
                return;
            }

            // Count chunks with correct PascalCase PageRange
            const correctResult = constant.db.query(
                `SELECT COUNT(*) as count FROM text_chunk WHERE kb_file_uid = $1 AND reference ? 'PageRange'`,
                pdfFile.fileUid
            );
            const correctCount = correctResult.length > 0 ? parseInt(correctResult[0].count) : 0;

            // Count chunks with INCORRECT snake_case page_range
            const incorrectResult = constant.db.query(
                `SELECT COUNT(*) as count FROM text_chunk WHERE kb_file_uid = $1 AND reference ? 'page_range'`,
                pdfFile.fileUid
            );
            const incorrectCount = incorrectResult.length > 0 ? parseInt(incorrectResult[0].count) : 0;

            console.log(`DB Test 3: PascalCase PageRange: ${correctCount}, snake_case page_range: ${incorrectCount}`);

            check({ correctCount, incorrectCount }, {
                "DB Test 3: All text_chunk.reference use PascalCase PageRange": () =>
                    correctCount > 0 && incorrectCount === 0,
            });

            // Sample a reference to verify structure
            const sampleResult = constant.db.query(
                `SELECT reference::text as reference_text FROM text_chunk WHERE kb_file_uid = $1 AND reference IS NOT NULL LIMIT 1`,
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
            const correctResult = constant.db.query(
                `SELECT COUNT(*) as count FROM converted_file WHERE file_uid = $1 AND position_data ? 'PageDelimiters'`,
                pdfFile.fileUid
            );
            const correctCount = correctResult.length > 0 ? parseInt(correctResult[0].count) : 0;

            // Count files with INCORRECT snake_case page_delimiters
            const incorrectResult = constant.db.query(
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
            const sampleResult = constant.db.query(
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

            const posDataResult = constant.db.query(
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
            const contentChunkResult = constant.db.query(
                `SELECT source_uid::text as source_uid_text, chunk_type FROM text_chunk WHERE kb_file_uid = $1 AND chunk_type = 'chunk' LIMIT 1`,
                testFile.fileUid
            );

            const summaryChunkResult = constant.db.query(
                `SELECT source_uid::text as source_uid_text, chunk_type FROM text_chunk WHERE kb_file_uid = $1 AND chunk_type = 'summary' LIMIT 1`,
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
                    "DB Test 6: Content chunk has chunk_type='chunk'": () =>
                        contentChunkResult[0].chunk_type === "chunk",
                    "DB Test 6: Summary chunk has chunk_type='summary'": () =>
                        summaryChunkResult[0].chunk_type === "summary",
                });

                // Verify converted files exist
                const contentFileResult = constant.db.query(
                    `SELECT destination, converted_type FROM converted_file WHERE uid = $1::uuid`,
                    contentSourceUid
                );

                const summaryFileResult = constant.db.query(
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
                        "DB Test 6: Content file has converted_type='content'": () =>
                            contentFileResult[0].converted_type === "content",
                        "DB Test 6: Summary file has converted_type='summary'": () =>
                            summaryFileResult[0].converted_type === "summary",
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

            // 7.1: knowledge_base_file.file_type stores FileType enum string
            const kbFileTypeResult = constant.db.query(
                `SELECT file_type FROM knowledge_base_file WHERE uid = $1`,
                testFile.fileUid
            );

            if (kbFileTypeResult.length > 0) {
                const fileType = kbFileTypeResult[0].file_type;
                console.log(`DB Test 7.1: knowledge_base_file.file_type = "${fileType}"`);

                check({ fileType }, {
                    "DB Test 7.1: file_type is FileType enum string (TYPE_*)": () =>
                        fileType && fileType.startsWith("TYPE_"),
                    "DB Test 7.1: file_type is NOT a MIME type": () =>
                        fileType && !fileType.includes("/"),
                });
            }

            // 7.2: converted_file.content_type stores MIME type
            const convertedContentTypeResult = constant.db.query(
                `SELECT content_type FROM converted_file WHERE file_uid = $1 LIMIT 1`,
                testFile.fileUid
            );

            if (convertedContentTypeResult.length > 0) {
                const contentType = convertedContentTypeResult[0].content_type;
                console.log(`DB Test 7.2: converted_file.content_type = "${contentType}"`);

                check({ contentType }, {
                    "DB Test 7.2: content_type is MIME type": () =>
                        contentType && contentType.includes("/"),
                    "DB Test 7.2: content_type is 'text/markdown'": () =>
                        contentType === "text/markdown",
                    "DB Test 7.2: content_type is NOT FileType enum": () =>
                        contentType && !contentType.startsWith("TYPE_"),
                });
            }

            // 7.3: text_chunk.content_type stores MIME type, chunk_type stores classification
            const chunkFieldsResult = constant.db.query(
                `SELECT content_type, chunk_type FROM text_chunk WHERE kb_file_uid = $1 LIMIT 1`,
                testFile.fileUid
            );

            if (chunkFieldsResult.length > 0) {
                const chunkContentType = chunkFieldsResult[0].content_type;
                const chunkChunkType = chunkFieldsResult[0].chunk_type;
                console.log(`DB Test 7.3: text_chunk.content_type = "${chunkContentType}"`);
                console.log(`DB Test 7.3: text_chunk.chunk_type = "${chunkChunkType}"`);

                check({ chunkContentType, chunkChunkType }, {
                    "DB Test 7.3: text_chunk.content_type is MIME type": () =>
                        chunkContentType && chunkContentType.includes("/"),
                    "DB Test 7.3: text_chunk.chunk_type is classification string": () =>
                        chunkChunkType && ["chunk", "summary", "augmented"].includes(chunkChunkType),
                });
            }

            // 7.4: embedding.content_type stores MIME type, chunk_type stores classification
            const embeddingFieldsResult = constant.db.query(
                `SELECT content_type, chunk_type FROM embedding WHERE kb_file_uid = $1 LIMIT 1`,
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
                        embChunkType && ["chunk", "summary", "augmented"].includes(embChunkType),
                });
            }

            // 7.5: converted_file.converted_type has expected values
            const convertedTypeResult = constant.db.query(
                `SELECT converted_type FROM converted_file WHERE file_uid = $1`,
                testFile.fileUid
            );

            if (convertedTypeResult.length > 0) {
                const convertedTypes = convertedTypeResult.map(row => row.converted_type);
                console.log(`DB Test 7.5: converted_file.converted_type values: ${JSON.stringify(convertedTypes)}`);

                check({ convertedTypes }, {
                    "DB Test 7.5: converted_type has expected values (content/summary)": () =>
                        convertedTypes.every(ct => ["content", "summary"].includes(ct)),
                    "DB Test 7.5: File has both content and summary types": () =>
                        convertedTypes.includes("content") && convertedTypes.includes("summary"),
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
                `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files`,
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
                `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files/${testFile.fileUid}`,
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
                        `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files/${fileOfType.fileUid}`,
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

        console.log(`\nDB Tests: All tests completed for catalog ${catalogId}`);
    });
}
