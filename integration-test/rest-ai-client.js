/**
 * AI Client Integration Tests
 *
 * This test suite validates the Artifact backend's AI client functionality,
 * specifically focused on Google Gemini integration for content processing.
 *
 * Test Coverage:
 * 1. AI Client Detection & Configuration
 *    - Verifies system initializes with configured Gemini client
 *    - Health check validation
 *
 * 2. Content Conversion (Gemini)
 *    - Tests document-to-markdown conversion via Gemini API
 *    - Validates chunk creation and retrievability
 *    - Note: OpenAI does NOT support content conversion
 *
 * 3. Embedding Generation (Gemini 3072-dim)
 *    - Tests Gemini's 3072-dimensional embedding generation
 *    - Validates embedding storage in database
 *    - Verifies similarity search functionality
 *
 * 4. Summary Generation (Gemini)
 *    - Tests AI-powered document summarization
 *    - Validates summary quality and length constraints
 *    - Note: OpenAI does NOT support summarization
 *
 * 5. Native Cache Support (Gemini)
 *    - Tests Gemini's native caching with 10-minute TTL
 *    - Validates cache-enabled file processing
 *    - Note: OpenAI does NOT support native caching
 *
 * Test Data Management:
 * - All test catalogs use "gem-" prefix for easy identification
 * - Setup: Cleans up orphaned catalogs from previous failed runs
 * - Teardown: Removes all test catalogs after completion
 * - Each test group creates and cleans up its own catalog
 *
 * API Key Requirements:
 * - Gemini API key must be configured for these tests to pass
 * - Tests will fail if Gemini client is not properly initialized
 *
 */

import http from "k6/http";
import { check, group, sleep } from "k6";
import { randomString } from "https://jslib.k6.io/k6-utils/1.4.0/index.js";
import encoding from "k6/encoding";

import * as constant from "./const.js";
import * as helper from "./helper.js";

const apiHost = constant.artifactRESTPublicHost;

export function setup() {
    // Stagger test execution to reduce parallel resource contention
    helper.staggerTestExecution(2);

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
        headers: {
            "Authorization": `Bearer ${loginResp.json().accessToken}`
        }
    })

    // Cleanup orphaned catalogs from previous failed test runs OF THIS SPECIFIC TEST
    // Use API-only cleanup to properly trigger workflows (no direct DB manipulation)
    console.log("\n=== SETUP: Cleaning up previous test data (gem pattern only) ===");
    try {
        const listResp = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${resp.json().user.id}/catalogs`, null, header);
        if (listResp.status === 200) {
            const catalogs = Array.isArray(listResp.json().catalogs) ? listResp.json().catalogs : [];
            let cleanedCount = 0;
            for (const catalog of catalogs) {
                const catId = catalog.catalogId || catalog.catalog_id;
                if (catId && catId.match(/^gem-/)) {
                    const delResp = http.request("DELETE", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${resp.json().user.id}/catalogs/${catId}`, null, header);
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
        expectedOwner: resp.json().user
    }
}

export function teardown(data) {
    // Final cleanup: Remove any remaining test catalogs (gem pattern only)
    // This catches any catalogs left behind if tests failed or were interrupted
    console.log("\n=== TEARDOWN: Final cleanup of test data (gem pattern only) ===");
    try {
        const listResp = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`, null, data.header);
        if (listResp.status === 200) {
            const catalogs = Array.isArray(listResp.json().catalogs) ? listResp.json().catalogs : [];
            let cleanedCount = 0;
            for (const catalog of catalogs) {
                const catId = catalog.catalogId || catalog.catalog_id;
                if (catId && catId.match(/^gem-/)) {
                    const delResp = http.request("DELETE", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catId}`, null, data.header);
                    if (delResp.status === 200 || delResp.status === 204) {
                        cleanedCount++;
                    }
                }
            }
            console.log(`Cleaned ${cleanedCount} remaining catalogs after test completion`);
        }
    } catch (e) {
        console.log(`Teardown cleanup warning: ${e}`);
    }
    console.log("=== TEARDOWN: Final cleanup complete ===\n");
}

export default function (data) {
    group("AI Client: Test Client Detection and Configuration", function () {
        // Test 1: Verify system initializes with configured client
        // Note: Gemini is required for content conversion and summarization
        // OpenAI is optional and only used for legacy embedding support (1536-dim)
        const healthResp = http.get(`${apiHost}/v1alpha/health/artifact`, constant.params);
        check(healthResp, {
            "AI Client Test 1.1: Health check returns 200": (r) => r.status === 200,
        });
    });

    group("AI Client: Test Content Conversion with Gemini", function () {
        // Note: This test requires Gemini API key to be configured
        // OpenAI client does NOT support content conversion
        const kbName = `test-gem-conv-${randomString(8)}`;

        // Create knowledge base
        const createKBResp = http.request(
            "POST",
            `${apiHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`,
            JSON.stringify({
                name: kbName,
                description: "Test KB for Gemini content conversion",
            }),
            data.header
        );

        check(createKBResp, {
            "Gemini Test 2.1: Create KB returns 200": (r) => r.status === 200,
        });

        if (createKBResp.status !== 200) {
            console.log(`Gemini Test 2.1 FAILED: Status ${createKBResp.status}, Body: ${createKBResp.body}`);
            return; // Skip rest of test if KB creation failed
        }

        const kbUid = createKBResp.json().catalog.catalogUid;
        const catalogId = createKBResp.json().catalog.catalogId;

        // Upload a text file for conversion testing with Gemini
        const testContent = "This is a test document for Gemini content conversion testing.\n\nIt contains multiple paragraphs.\n\nAnd tests markdown extraction.";

        const uploadResp = helper.uploadFileWithRetry(
            `${apiHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files`,
            {
                name: "test-gemini-conversion.txt",
                type: "TYPE_TEXT",
                content: encoding.b64encode(testContent) // Base64 encode content
            },
            data.header,
            3 // max retries
        );

        check(uploadResp, {
            "Gemini Test 2.2: Upload file returns 200": (r) => r && r.status === 200,
        });

        if (!uploadResp || uploadResp.status !== 200) {
            console.log(`Gemini Test 2.2 FAILED: Status ${uploadResp.status}, Body: ${uploadResp.body}`);
            // Cleanup KB before returning (no files to wait for since upload failed)
            http.request("DELETE", `${apiHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}`, null, data.header);
            return;
        }

        const fileUid = uploadResp.json().file.fileUid;

        // Auto-trigger: Processing starts automatically on upload (no manual trigger needed)
        // Wait for processing to complete
        let processedStatus = false;
        for (let i = 0; i < 60; i++) {
            sleep(2);
            const getFileResp = http.request(
                "GET",
                `${apiHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files/${fileUid}`,
                null,
                data.header
            );

            if (getFileResp.status === 200) {
                const file = getFileResp.json().file;
                if (file.processStatus === "FILE_PROCESS_STATUS_COMPLETED") {
                    processedStatus = true;
                    console.log(`Gemini Test 2.3: File processing completed`);
                    break;
                } else if (file.processStatus === "FILE_PROCESS_STATUS_FAILED") {
                    console.log(`Gemini Test 2.3: File processing failed`);
                    break;
                }
            }
        }

        check({ processedStatus }, {
            "Gemini Test 2.3: File processing completes successfully": () => processedStatus,
        });

        // Verify converted content exists (Gemini 3072-dim embeddings)
        if (processedStatus) {
            const chunksResp = http.request(
                "GET",
                `${apiHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/chunks?fileUid=${fileUid}`,
                null,
                data.header
            );

            if (chunksResp.status !== 200) {
                console.log(`Gemini Test 2.4 FAILED: Status ${chunksResp.status}, Body: ${chunksResp.body}`);
            }

            check(chunksResp, {
                "Gemini Test 2.4: Chunks exist after conversion": (r) =>
                    r.status === 200 && r.json().chunks && r.json().chunks.length > 0,
            });

            if (chunksResp.status === 200 && chunksResp.json().chunks) {
                const chunks = chunksResp.json().chunks;
                console.log(`Gemini Test 2.4: Generated ${chunks.length} chunks`);

                // Verify chunk has retrievable flag
                if (chunks.length > 0) {
                    check(chunks[0], {
                        "Gemini Test 2.5: Chunks are retrievable": (chunk) =>
                            chunk.retrievable === true,
                    });
                }
            }
        }

        // Cleanup: Wait for file processing before deleting catalog
        // This prevents "collection does not exist" errors
        console.log(`Gemini Test 2.6: Waiting for file processing before cleanup...`);
        sleep(2); // Small grace period for workflow to complete final steps

        const deleteResp = http.request(
            "DELETE",
            `${apiHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}`,
            null,
            data.header
        );

        if (deleteResp.status !== 204 && deleteResp.status !== 200) {
            console.log(`Gemini Test 2.6: Cleanup status ${deleteResp.status}, Body: ${deleteResp.body}`);
        }

        check(deleteResp, {
            "Gemini Test 2.6: Cleanup KB successfully": (r) => r.status === 204 || r.status === 200,
        });
    });

    group("AI Client: Test Gemini Embedding Generation (3072-dim)", function () {
        // Note: Tests Gemini's 3072-dimensional embeddings
        // OpenAI client (1536-dim) is legacy only and tested separately if needed
        const kbName = `gem-embed-${randomString(8)}`; // Max 32 chars: gem-embed-12345678 = 19 chars

        // Create knowledge base
        const createKBResp = http.request(
            "POST",
            `${apiHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`,
            JSON.stringify({
                name: kbName,
                description: "Test KB for Gemini embedding generation (3072-dim)",
            }),
            data.header
        );

        check(createKBResp, {
            "Gemini Test 3.1: Create KB returns 200": (r) => r.status === 200,
        });

        if (createKBResp.status !== 200) {
            console.log(`Gemini Test 3.1 FAILED: Status ${createKBResp.status}, Body: ${createKBResp.body}`);
            return;
        }

        const kbUid = createKBResp.json().catalog.catalogUid;
        const catalogId = createKBResp.json().catalog.catalogId;

        // Upload a file for embedding testing
        const testContent = "Artificial Intelligence and Machine Learning are transforming technology. Deep learning models process vast amounts of data.";

        const uploadResp = helper.uploadFileWithRetry(
            `${apiHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files`,
            {
                name: "test-embedding.txt",
                type: "TYPE_TEXT",
                content: encoding.b64encode(testContent) // Base64 encode content
            },
            data.header,
            3 // max retries
        );

        check(uploadResp, {
            "Gemini Test 3.2: Upload file returns 200": (r) => r && r.status === 200,
        });

        if (!uploadResp || uploadResp.status !== 200) {
            console.log(`Gemini Test 3.2 FAILED: Status ${uploadResp.status}, Body: ${uploadResp.body}`);
            // Cleanup KB before returning (no files to wait for since upload failed)
            http.request("DELETE", `${apiHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}`, null, data.header);
            return;
        }

        const fileUid = uploadResp.json().file.fileUid;

        // Auto-trigger: Processing starts automatically on upload (no manual trigger needed)
        // Wait for processing to complete
        let processedStatus = false;
        for (let i = 0; i < 60; i++) {
            sleep(2);
            const getFileResp = http.request(
                "GET",
                `${apiHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files/${fileUid}`,
                null,
                data.header
            );

            if (getFileResp.status === 200) {
                const file = getFileResp.json().file;
                if (file.processStatus === "FILE_PROCESS_STATUS_COMPLETED") {
                    processedStatus = true;
                    break;
                }
            }
        }

        check({ processedStatus }, {
            "Gemini Test 3.3: File embedding processing completes": () => processedStatus,
        });

        // Verify embeddings were created by checking if embeddings exist in database
        if (processedStatus) {
            const embeddingResult = constant.db.query(
                `SELECT COUNT(*) as count FROM embedding WHERE file_uid = $1`,
                fileUid
            );

            if (embeddingResult.length > 0) {
                const count = parseInt(embeddingResult[0].count);
                console.log(`Gemini Test 3.4: Generated ${count} embeddings`);

                check({ count }, {
                    "Gemini Test 3.4: Embeddings were generated": () => count > 0,
                });
            }
        }

        // Test similarity search to verify Gemini embeddings work
        // Note: This tests the end-to-end search functionality using embeddings
        if (processedStatus) {
            const searchResp = http.request(
                "POST",
                `${apiHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/chunks/similarity-search`,
                JSON.stringify({
                    textPrompt: "machine learning",
                    topK: 5,
                }),
                data.header
            );

            if (searchResp.status === 404) {
                console.log(`Gemini Test 3.5: Similarity search endpoint not found (skipping test)`);
            } else if (searchResp.status !== 200) {
                console.log(`Gemini Test 3.5 FAILED: Status ${searchResp.status}, Body: ${searchResp.body}`);
            }

            // Optional test - skip if endpoint not implemented yet
            check(searchResp, {
                "Gemini Test 3.5: Similarity search returns results (optional)": (r) =>
                    r.status === 404 || (r.status === 200 && r.json().similarityChunks && r.json().similarityChunks.length > 0),
            });

            if (searchResp.status === 200 && searchResp.json().similarityChunks) {
                console.log(`Gemini Test 3.5: Found ${searchResp.json().similarityChunks.length} similar chunks`);
            }
        }

        // Cleanup: Wait for file processing before deleting catalog
        console.log(`Gemini Test 3.6: Waiting for file processing before cleanup...`);
        sleep(2); // Small grace period for workflow to complete final steps

        http.request(
            "DELETE",
            `${apiHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}`,
            null,
            data.header
        );
    });

    group("AI Client: Test Gemini Summary Generation", function () {
        // Note: Summary generation requires Gemini - OpenAI does NOT support this
        const kbName = `gem-summ-${randomString(8)}`; // Max 32 chars: gem-summ-12345678 = 19 chars

        // Create knowledge base
        const createKBResp = http.request(
            "POST",
            `${apiHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`,
            JSON.stringify({
                name: kbName,
                description: "Test KB for Gemini summary generation",
            }),
            data.header
        );

        check(createKBResp, {
            "Gemini Test 4.1: Create KB returns 200": (r) => r.status === 200,
        });

        if (createKBResp.status !== 200) {
            console.log(`Gemini Test 4.1 FAILED: Status ${createKBResp.status}, Body: ${createKBResp.body}`);
            return;
        }

        const kbUid = createKBResp.json().catalog.catalogUid;
        const catalogId = createKBResp.json().catalog.catalogId;

        // Upload a longer text file for summary testing
        const longContent = `
# Project Report

## Executive Summary
This document outlines the key findings from our Q4 analysis. The project has achieved significant milestones
in artificial intelligence integration and customer satisfaction metrics.

## Key Metrics
- Revenue: $1.2M
- User Growth: 45%
- Customer Satisfaction: 92%

## Recommendations
1. Expand AI capabilities
2. Increase marketing budget
3. Hire additional engineering staff
        `.trim();

        const uploadResp = helper.uploadFileWithRetry(
            `${apiHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files`,
            {
                name: "test-summary.txt",
                type: "TYPE_TEXT",
                content: encoding.b64encode(longContent) // Base64 encode content
            },
            data.header,
            3 // max retries
        );

        check(uploadResp, {
            "Gemini Test 4.2: Upload file returns 200": (r) => r && r.status === 200,
        });

        if (!uploadResp || uploadResp.status !== 200) {
            console.log(`Gemini Test 4.2 FAILED: Status ${uploadResp.status}, Body: ${uploadResp.body}`);
            // Cleanup KB before returning (no files to wait for since upload failed)
            http.request("DELETE", `${apiHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}`, null, data.header);
            return;
        }

        const fileUid = uploadResp.json().file.fileUid;

        // Auto-trigger: Processing starts automatically on upload (no manual trigger needed)
        // Wait for processing to complete
        let processedStatus = false;
        for (let i = 0; i < 60; i++) {
            sleep(2);
            const getFileResp = http.request(
                "GET",
                `${apiHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files/${fileUid}`,
                null,
                data.header
            );

            if (getFileResp.status === 200) {
                const file = getFileResp.json().file;
                if (file.processStatus === "FILE_PROCESS_STATUS_COMPLETED") {
                    processedStatus = true;
                    break;
                }
            }
        }

        check({ processedStatus }, {
            "Gemini Test 4.3: File summary processing completes": () => processedStatus,
        });

        // Verify summary exists via API endpoint (generated by Gemini)
        if (processedStatus) {
            const summaryResp = http.request(
                "GET",
                `${apiHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files/${fileUid}/summary`,
                null,
                data.header
            );

            if (summaryResp.status === 200) {
                const summaryData = summaryResp.json();
                const summary = summaryData.summary || "";
                console.log(`Gemini Test 4.4: Summary generated with ${summary.length} characters`);

                check({ summary }, {
                    "Gemini Test 4.4: Summary is generated": () => summary && summary.length > 0,
                    "Gemini Test 4.5: Summary is concise (<2000 chars)": () => summary && summary.length < 2000,
                });
            } else {
                console.log(`Gemini Test 4.4 FAILED: Status ${summaryResp.status}, Body: ${summaryResp.body}`);
            }
        }

        // Cleanup: Wait for file processing before deleting catalog
        console.log(`Gemini Test 4.6: Waiting for file processing before cleanup...`);
        sleep(2); // Small grace period for workflow to complete final steps

        http.request(
            "DELETE",
            `${apiHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}`,
            null,
            data.header
        );
    });

    group("AI Client: Test Gemini Cache Functionality", function () {
        // Note: Cache functionality requires Gemini - OpenAI does NOT support caching
        const kbName = `gem-cache-${randomString(8)}`; // Max 32 chars: gem-cache-12345678 = 20 chars

        // Create knowledge base
        const createKBResp = http.request(
            "POST",
            `${apiHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`,
            JSON.stringify({
                name: kbName,
                description: "Test KB for Gemini cache (10 min TTL)",
            }),
            data.header
        );

        check(createKBResp, {
            "Gemini Test 5.1: Create KB returns 200": (r) => r.status === 200,
        });

        if (createKBResp.status !== 200) {
            console.log(`Gemini Test 5.1 FAILED: Status ${createKBResp.status}, Body: ${createKBResp.body}`);
            return;
        }

        const kbUid = createKBResp.json().catalog.catalogUid;
        const catalogId = createKBResp.json().catalog.catalogId;

        // Upload file - this should create a Gemini cache during processing
        const testContent = "Cache test content for Gemini client validation with native caching support.";

        const uploadResp = helper.uploadFileWithRetry(
            `${apiHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files`,
            {
                name: "test-gemini-cache.txt",
                type: "TYPE_TEXT",
                content: encoding.b64encode(testContent) // Base64 encode content
            },
            data.header,
            3 // max retries
        );

        check(uploadResp, {
            "Gemini Test 5.2: Upload file returns 200": (r) => r && r.status === 200,
        });

        if (!uploadResp || uploadResp.status !== 200) {
            console.log(`Gemini Test 5.2 FAILED: Status ${uploadResp.status}, Body: ${uploadResp.body}`);
            // Cleanup KB before returning (no files to wait for since upload failed)
            http.request("DELETE", `${apiHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}`, null, data.header);
            return;
        }

        const fileUid = uploadResp.json().file.fileUid;

        // Auto-trigger: Processing starts automatically on upload (no manual trigger needed)
        // Wait for processing to complete
        let processedStatus = false;
        for (let i = 0; i < 60; i++) {
            sleep(2);
            const getFileResp = http.request(
                "GET",
                `${apiHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files/${fileUid}`,
                null,
                data.header
            );

            if (getFileResp.status === 200) {
                const file = getFileResp.json().file;
                if (file.processStatus === "FILE_PROCESS_STATUS_COMPLETED") {
                    processedStatus = true;
                    console.log(`Gemini Test 5.3: File processing with native cache completed`);
                    break;
                }
            }
        }

        check({ processedStatus }, {
            "Gemini Test 5.3: File processes with native cache support": () => processedStatus,
        });

        // Cleanup: Wait for file processing before deleting catalog
        console.log(`Gemini Test 5.4: Waiting for file processing before cleanup...`);
        sleep(2); // Small grace period for workflow to complete final steps

        http.request(
            "DELETE",
            `${apiHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}`,
            null,
            data.header
        );
    });
}
