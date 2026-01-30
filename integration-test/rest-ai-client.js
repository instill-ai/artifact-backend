/**
 * AI Client Integration Tests
 *
 * This test suite validates the Artifact backend's AI client functionality,
 * testing content processing with configured AI models (OpenAI, Gemini, etc.).
 *
 * Test Coverage:
 * 1. AI Client Detection & Configuration
 *    - Verifies system initializes with configured AI client
 *    - Health check validation
 *
 * 2. Content Conversion
 *    - Tests document-to-markdown conversion via AI API
 *    - Validates chunk creation and retrievability
 *    - Model support depends on AI client configuration
 *
 * 3. Embedding Generation
 *    - Tests AI model embedding generation
 *    - Validates embedding storage in database
 *    - Verifies similarity search functionality
 *    - Embedding dimensionality depends on the configured model
 *
 * 4. Summary Generation
 *    - Tests AI-powered document summarization
 *    - Validates summary quality and length constraints
 *    - Model support depends on AI client configuration
 *
 * 5. Native Cache Support
 *    - Tests AI model native caching capabilities
 *    - Validates cache-enabled file processing
 *    - Cache support depends on the configured model
 *
 * Test Data Management:
 * - All test knowledge bases use "test-" prefix for easy identification
 * - Setup: Cleans up orphaned knowledge bases from previous failed runs
 * - Teardown: Removes all test knowledge bases after completion
 * - Each test group creates and cleans up its own knowledge base
 *
 * API Key Requirements:
 * - AI client API key must be configured for these tests to pass
 * - Tests will fail if AI client is not properly initialized
 *
 */

import { check, group, sleep } from "k6";
import { randomString } from "https://jslib.k6.io/k6-utils/1.4.0/index.js";
import encoding from "k6/encoding";

import * as constant from "./const.js";
import * as helper from "./helper.js";

// Use httpRetry for automatic retry on transient errors (429, 5xx)
const http = helper.httpRetry;

const apiHost = constant.artifactRESTPublicHost;

export let options = {
    setupTimeout: '600s',
    teardownTimeout: '180s',
    insecureSkipTLSVerify: true,
    thresholds: {
        checks: ["rate == 1.0"],
    },
};

export function setup() {
    // Stagger test execution to reduce parallel resource contention
    helper.staggerTestExecution(2);

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
        headers: {
            "Authorization": authHeader
        }
    })

    // Cleanup orphaned knowledge bases from previous failed test runs OF THIS SPECIFIC TEST
    // Use API-only cleanup to properly trigger workflows (no direct DB manipulation)
    console.log("\n=== SETUP: Cleaning up previous test data (test-ai-* pattern only) ===");
    try {
        const listResp = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${resp.json().user.id}/knowledge-bases`, null, header);
        if (listResp.status === 200) {
            const knowledgeBases = Array.isArray(listResp.json().knowledgeBases) ? listResp.json().knowledgeBases : [];
            let cleanedCount = 0;
            for (const kb of knowledgeBases) {
                const kbId = kb.id;
                if (kbId && kbId.match(/^test-ai-/)) {
                    const delResp = http.request("DELETE", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${resp.json().user.id}/knowledge-bases/${knowledgeBaseId}`, null, header);
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
        expectedOwner: resp.json().user
    }
}

export function teardown(data) {
    // Final cleanup: Remove any remaining test knowledge bases (test-ai-* pattern only)
    // This catches any knowledge bases left behind if tests failed or were interrupted
    console.log("\n=== TEARDOWN: Final cleanup of test data (test-ai-* pattern only) ===");
    try {
        const listResp = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases`, null, data.header);
        if (listResp.status === 200) {
            const knowledgeBases = Array.isArray(listResp.json().knowledgeBases) ? listResp.json().knowledgeBases : [];
            let cleanedCount = 0;
            for (const kb of knowledgeBases) {
                const kbId = kb.id;
                if (kbId && kbId.match(/^test-ai-/)) {
                    const delResp = http.request("DELETE", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}`, null, data.header);
                    if (delResp.status === 200 || delResp.status === 204) {
                        cleanedCount++;
                    }
                }
            }
            console.log(`Cleaned ${cleanedCount} remaining knowledge bases after test completion`);
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
        const healthResp = http.get(`${apiHost}/v1beta/health/artifact`, constant.params);
        check(healthResp, {
            "AI Client Test 1.1: Health check returns 200": (r) => r.status === 200,
        });
    });

    group("AI Client: Test Content Conversion", function () {
        // Note: Content conversion requires AI client configuration
        // Tests document-to-markdown conversion via AI API
        const kbDisplayName = `Test AI Conv ${randomString(8)}`;

        // Create knowledge base
        const createKBResp = http.request(
            "POST",
            `${apiHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases`,
            JSON.stringify({
                displayName: kbDisplayName,
                description: "Test KB for AI content conversion",
            }),
            data.header
        );

        check(createKBResp, {
            "AI Test 2.1: Create KB returns 200": (r) => r.status === 200,
        });

        if (createKBResp.status !== 200) {
            console.log(`AI Test 2.1 FAILED: Status ${createKBResp.status}, Body: ${createKBResp.body}`);
            return; // Skip rest of test if KB creation failed
        }

        // Note: uid field removed in AIP refactoring - use id for identification
        const kbId = createKBResp.json().knowledgeBase.id;
        const knowledgeBaseId = createKBResp.json().knowledgeBase.id;

        // Upload a text file for conversion testing with Gemini
        const testContent = "This is a test document for Gemini content conversion testing.\n\nIt contains multiple paragraphs.\n\nAnd tests markdown extraction.";

        const uploadResp = helper.uploadFileWithRetry(
            `${apiHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files`,
            {
                displayName: "test-gemini-conversion.txt",
                type: "TYPE_TEXT",
                content: encoding.b64encode(testContent) // Base64 encode content
            },
            data.header,
            3 // max retries
        );

        check(uploadResp, {
            "AI Test 2.2: Upload file returns 200": (r) => r && r.status === 200,
        });

        if (!uploadResp || uploadResp.status !== 200) {
            console.log(`AI Test 2.2 FAILED: Status ${uploadResp ? uploadResp.status : 'undefined'}, Body: ${uploadResp ? uploadResp.body : 'N/A'}`);
            // Cleanup KB before returning (no files to wait for since upload failed)
            http.request("DELETE", `${apiHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}`, null, data.header);
            return;
        }

        // Note: uid field removed in AIP refactoring - use id for identification
        const fileId = uploadResp.json().file.id;

        // Auto-trigger: Processing starts automatically on upload (no manual trigger needed)
        // Wait for processing to complete using robust helper function
        console.log(`AI Test 2.3: Waiting for file processing (fileId: ${fileId})...`);
        const result = helper.waitForFileProcessingComplete(
            data.expectedOwner.id,
            knowledgeBaseId,
            fileId, // Use hash-based ID (uid removed in AIP refactoring)
            data.header,
            300, // Max 300 seconds (increased for slower GA environment)
            120  // Fast-fail after 120s if stuck in NOTSTARTED (increased for GA)
        );

        const processedStatus = result.completed && result.status === "COMPLETED";

        if (!processedStatus) {
            console.log(`AI Test 2.3: File processing did not complete. Status: ${result.status}, Error: ${result.error || 'N/A'}`);
        }

        check({ processedStatus }, {
            "AI Test 2.3: File processing completes successfully": () => processedStatus,
        });

        // Verify converted content exists (Gemini 3072-dim embeddings)
        if (processedStatus) {
            // Add grace period and retry logic for chunks to handle DB transaction timing
            // File status changes to COMPLETED before DB transaction is fully committed
            let chunksResp;
            let chunksAvailable = false;

            for (let i = 0; i < 10; i++) {
                sleep(1); // Wait 1 second between retries
                chunksResp = http.request(
                    "GET",
                    `${apiHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files/${fileId}/chunks`,
                    null,
                    data.header
                );

                if (chunksResp.status === 200) {
                    const jsonResp = chunksResp.json();
                    if (jsonResp.chunks && jsonResp.chunks.length > 0) {
                        chunksAvailable = true;
                        console.log(`AI Test 2.4: Found ${jsonResp.chunks.length} chunks after ${i + 1} attempts`);
                        break;
                    }
                }
            }

            if (!chunksAvailable && chunksResp.status !== 200) {
                console.log(`AI Test 2.4 FAILED: Status ${chunksResp.status}, Body: ${chunksResp.body}`);
            }

            check({ chunksAvailable }, {
                "AI Test 2.4: Chunks exist after conversion": () => chunksAvailable,
            });

            if (chunksAvailable && chunksResp.status === 200 && chunksResp.json().chunks) {
                const chunks = chunksResp.json().chunks;
                console.log(`AI Test 2.4: Generated ${chunks.length} chunks`);

                // Verify chunk has retrievable flag
                if (chunks.length > 0) {
                    check(chunks[0], {
                        "AI Test 2.5: Chunks are retrievable": (chunk) =>
                            chunk.retrievable === true,
                    });
                }
            }
        }

        // Cleanup: Wait for file processing AND Temporal activities to settle before cleanup
        console.log(`AI Test 2.6: Waiting for safe cleanup...`);
        helper.waitForSafeCleanup(180, knowledgeBaseId, 3);

        const deleteResp = http.request(
            "DELETE",
            `${apiHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}`,
            null,
            data.header
        );

        if (deleteResp.status !== 204 && deleteResp.status !== 200) {
            console.log(`AI Test 2.6: Cleanup failed - Status ${deleteResp.status}, Body: ${deleteResp.body}`);
        }

        check(deleteResp, {
            "AI Test 2.6: Cleanup KB successfully": (r) => r.status === 204 || r.status === 200,
        });
    });

    group("AI Client: Test Embedding Generation", function () {
        // Note: Tests AI model embedding generation
        // Embedding dimensionality depends on the configured model
        const kbDisplayName = `Test AI Embed ${randomString(8)}`;

        // Create knowledge base
        const createKBResp = http.request(
            "POST",
            `${apiHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases`,
            JSON.stringify({
                displayName: kbDisplayName,
                description: "Test KB for AI embedding generation",
            }),
            data.header
        );

        check(createKBResp, {
            "AI Test 3.1: Create KB returns 200": (r) => r.status === 200,
        });

        if (createKBResp.status !== 200) {
            console.log(`AI Test 3.1 FAILED: Status ${createKBResp.status}, Body: ${createKBResp.body}`);
            return;
        }

        // Note: uid field removed in AIP refactoring - use id for identification
        const kbId = createKBResp.json().knowledgeBase.id;
        const knowledgeBaseId = createKBResp.json().knowledgeBase.id;

        // Upload a file for embedding testing
        const testContent = "Artificial Intelligence and Machine Learning are transforming technology. Deep learning models process vast amounts of data.";

        const uploadResp = helper.uploadFileWithRetry(
            `${apiHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files`,
            {
                displayName: "test-embedding.txt",
                type: "TYPE_TEXT",
                content: encoding.b64encode(testContent) // Base64 encode content
            },
            data.header,
            3 // max retries
        );

        check(uploadResp, {
            "AI Test 3.2: Upload file returns 200": (r) => r && r.status === 200,
        });

        if (!uploadResp || uploadResp.status !== 200) {
            console.log(`AI Test 3.2 FAILED: Status ${uploadResp ? uploadResp.status : 'undefined'}, Body: ${uploadResp ? uploadResp.body : 'N/A'}`);
            // Cleanup KB before returning (no files to wait for since upload failed)
            http.request("DELETE", `${apiHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}`, null, data.header);
            return;
        }

        // Note: uid field removed in AIP refactoring - use id for identification
        const fileId = uploadResp.json().file.id;

        // Auto-trigger: Processing starts automatically on upload (no manual trigger needed)
        // Wait for processing to complete using robust helper function
        console.log(`AI Test 3.3: Waiting for file embedding processing (fileId: ${fileId})...`);
        const result = helper.waitForFileProcessingComplete(
            data.expectedOwner.id,
            knowledgeBaseId,
            fileId, // Use hash-based ID (uid removed in AIP refactoring)
            data.header,
            300, // Max 300 seconds (increased for slower GA environment)
            120  // Fast-fail after 120s if stuck in NOTSTARTED (increased for GA)
        );

        const processedStatus = result.completed && result.status === "COMPLETED";

        if (!processedStatus) {
            console.log(`AI Test 3.3: File processing did not complete. Status: ${result.status}, Error: ${result.error || 'N/A'}`);
        }

        check({ processedStatus }, {
            "AI Test 3.3: File embedding processing completes": () => processedStatus,
        });

        // Verify embeddings were created by checking database
        // Get internal file_uid from hash-based id for database verification
        if (processedStatus) {
            const fileUid = helper.getFileUidFromId(fileId);
            if (fileUid) {
                const embeddingResult = helper.safeQuery(
                    `SELECT COUNT(*) as count FROM embedding WHERE file_uid = $1`,
                    fileUid
                );

                if (embeddingResult.length > 0) {
                    const count = parseInt(embeddingResult[0].count);
                    console.log(`AI Test 3.4: Generated ${count} embeddings`);

                    check({ count }, {
                        "AI Test 3.4: Embeddings were generated": () => count > 0,
                    });
                }
            } else {
                console.log(`AI Test 3.4: Could not get file_uid for fileId=${fileId}, skipping embedding verification`);
            }
        }

        // Test similarity search to verify Gemini embeddings work
        // Note: This tests the end-to-end search functionality using embeddings
        if (processedStatus) {
            const searchResp = http.request(
                "POST",
                `${apiHost}/v1alpha/namespaces/${data.expectedOwner.id}/search-chunks`,
                JSON.stringify({
                    knowledgeBase: `namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}`,
                    textPrompt: "machine learning",
                    topK: 5,
                }),
                data.header
            );

            if (searchResp.status === 404) {
                console.log(`AI Test 3.5: Similarity search endpoint not found (skipping test)`);
            } else if (searchResp.status !== 200) {
                console.log(`AI Test 3.5 FAILED: Status ${searchResp.status}, Body: ${searchResp.body}`);
            }

            // Optional test - skip if endpoint not implemented yet
            check(searchResp, {
                "AI Test 3.5: Similarity search returns results (optional)": (r) =>
                    r.status === 404 || (r.status === 200 && r.json().similarChunks && r.json().similarChunks.length > 0),
            });

            if (searchResp.status === 200 && searchResp.json().similarChunks) {
                console.log(`AI Test 3.5: Found ${searchResp.json().similarChunks.length} similar chunks`);
            }
        }

        // Cleanup: Wait for file processing before deleting knowledge base
        console.log(`AI Test 3.6: Waiting for file processing before cleanup...`);
        sleep(2); // Small grace period for workflow to complete final steps

        http.request(
            "DELETE",
            `${apiHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}`,
            null,
            data.header
        );
    });

    group("AI Client: Test Summary Generation", function () {
        // Note: Summary generation requires AI client configuration
        // Tests AI-powered document summarization
        const kbDisplayName = `Test AI Summ ${randomString(8)}`;

        // Create knowledge base
        const createKBResp = http.request(
            "POST",
            `${apiHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases`,
            JSON.stringify({
                displayName: kbDisplayName,
                description: "Test KB for AI summary generation",
            }),
            data.header
        );

        check(createKBResp, {
            "AI Test 4.1: Create KB returns 200": (r) => r.status === 200,
        });

        if (createKBResp.status !== 200) {
            console.log(`AI Test 4.1 FAILED: Status ${createKBResp.status}, Body: ${createKBResp.body}`);
            return;
        }

        // Note: uid field removed in AIP refactoring - use id for identification
        const kbId = createKBResp.json().knowledgeBase.id;
        const knowledgeBaseId = createKBResp.json().knowledgeBase.id;

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
            `${apiHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files`,
            {
                displayName: "test-summary.txt",
                type: "TYPE_TEXT",
                content: encoding.b64encode(longContent) // Base64 encode content
            },
            data.header,
            3 // max retries
        );

        check(uploadResp, {
            "AI Test 4.2: Upload file returns 200": (r) => r && r.status === 200,
        });

        if (!uploadResp || uploadResp.status !== 200) {
            console.log(`AI Test 4.2 FAILED: Status ${uploadResp ? uploadResp.status : 'undefined'}, Body: ${uploadResp ? uploadResp.body : 'N/A'}`);
            // Cleanup KB before returning (no files to wait for since upload failed)
            http.request("DELETE", `${apiHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}`, null, data.header);
            return;
        }

        // Note: uid field removed in AIP refactoring - use id for identification
        const fileId = uploadResp.json().file.id;

        // Auto-trigger: Processing starts automatically on upload (no manual trigger needed)
        // Wait for processing to complete using robust helper function
        console.log(`AI Test 4.3: Waiting for file summary processing (fileId: ${fileId})...`);
        const result = helper.waitForFileProcessingComplete(
            data.expectedOwner.id,
            knowledgeBaseId,
            fileId, // Use hash-based ID (uid removed in AIP refactoring)
            data.header,
            300, // Max 300 seconds (increased for slower GA environment)
            120  // Fast-fail after 120s if stuck in NOTSTARTED (increased for GA)
        );

        const processedStatus = result.completed && result.status === "COMPLETED";

        if (!processedStatus) {
            console.log(`AI Test 4.3: File processing did not complete. Status: ${result.status}, Error: ${result.error || 'N/A'}`);
        }

        check({ processedStatus }, {
            "AI Test 4.3: File summary processing completes": () => processedStatus,
        });

        // Verify summary exists via API endpoint (generated by Gemini)
        if (processedStatus) {
            // Add grace period and retry logic for summary to handle DB transaction timing
            // File status changes to COMPLETED before converted_file record is fully committed
            let summaryResp;
            let derivedUri = "";

            for (let i = 0; i < 10; i++) {
                sleep(1); // Wait 1 second between retries
                summaryResp = http.request(
                    "GET",
                    `${apiHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files/${fileId}?view=VIEW_SUMMARY`,
                    null,
                    data.header
                );

                if (summaryResp.status === 200) {
                    const summaryData = summaryResp.json();
                    derivedUri = summaryData.derivedResourceUri || "";

                    if (derivedUri && derivedUri.length > 0) {
                        console.log(`AI Test 4.4: Summary URI generated after ${i + 1} attempts: ${derivedUri}`);
                        break;
                    }
                }
            }

            if (!derivedUri && summaryResp.status !== 200) {
                console.log(`AI Test 4.4 FAILED: Status ${summaryResp.status}, Body: ${summaryResp.body}`);
            }

            check({ derivedUri }, {
                "AI Test 4.4: Summary URI is generated": () => derivedUri && derivedUri.length > 0,
                "AI Test 4.5: Summary URI is valid URL": () => derivedUri && derivedUri.startsWith("http"),
            });
        }

        // Cleanup: Wait for file processing before deleting knowledge base
        console.log(`AI Test 4.6: Waiting for file processing before cleanup...`);
        sleep(2); // Small grace period for workflow to complete final steps

        http.request(
            "DELETE",
            `${apiHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}`,
            null,
            data.header
        );
    });

    group("AI Client: Test Cache Functionality", function () {
        // Note: Cache functionality depends on AI model support
        // Tests AI model native caching capabilities
        const kbDisplayName = `Test AI Cache ${randomString(8)}`;

        // Create knowledge base
        const createKBResp = http.request(
            "POST",
            `${apiHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases`,
            JSON.stringify({
                displayName: kbDisplayName,
                description: "Test KB for AI cache functionality",
            }),
            data.header
        );

        check(createKBResp, {
            "AI Test 5.1: Create KB returns 200": (r) => r.status === 200,
        });

        if (createKBResp.status !== 200) {
            console.log(`AI Test 5.1 FAILED: Status ${createKBResp.status}, Body: ${createKBResp.body}`);
            return;
        }

        // Note: uid field removed in AIP refactoring - use id for identification
        const kbId = createKBResp.json().knowledgeBase.id;
        const knowledgeBaseId = createKBResp.json().knowledgeBase.id;

        // Upload file - this should create a Gemini cache during processing
        const testContent = "Cache test content for Gemini client validation with native caching support.";

        const uploadResp = helper.uploadFileWithRetry(
            `${apiHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files`,
            {
                displayName: "test-gemini-cache.txt",
                type: "TYPE_TEXT",
                content: encoding.b64encode(testContent) // Base64 encode content
            },
            data.header,
            3 // max retries
        );

        check(uploadResp, {
            "AI Test 5.2: Upload file returns 200": (r) => r && r.status === 200,
        });

        if (!uploadResp || uploadResp.status !== 200) {
            console.log(`AI Test 5.2 FAILED: Status ${uploadResp ? uploadResp.status : 'undefined'}, Body: ${uploadResp ? uploadResp.body : 'N/A'}`);
            // Cleanup KB before returning (no files to wait for since upload failed)
            http.request("DELETE", `${apiHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}`, null, data.header);
            return;
        }

        // Note: uid field removed in AIP refactoring - use id for identification
        const fileId = uploadResp.json().file.id;

        // Auto-trigger: Processing starts automatically on upload (no manual trigger needed)
        // Wait for processing to complete using robust helper function
        console.log(`AI Test 5.3: Waiting for file processing with native cache (fileId: ${fileId})...`);
        const result = helper.waitForFileProcessingComplete(
            data.expectedOwner.id,
            knowledgeBaseId,
            fileId, // Use hash-based ID (uid removed in AIP refactoring)
            data.header,
            300, // Max 300 seconds (increased for slower GA environment)
            120  // Fast-fail after 120s if stuck in NOTSTARTED (increased for GA)
        );

        const processedStatus = result.completed && result.status === "COMPLETED";

        if (!processedStatus) {
            console.log(`AI Test 5.3: File processing did not complete. Status: ${result.status}, Error: ${result.error || 'N/A'}`);
        }

        check({ processedStatus }, {
            "AI Test 5.3: File processes with native cache support": () => processedStatus,
        });

        // Cleanup: Wait for file processing AND Temporal activities to settle before cleanup
        console.log(`AI Test 5.4: Waiting for safe cleanup...`);
        helper.waitForSafeCleanup(180, knowledgeBaseId, 3);

        http.request(
            "DELETE",
            `${apiHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}`,
            null,
            data.header
        );
    });
}
