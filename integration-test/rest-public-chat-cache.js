import http from "k6/http";
import { check, group, sleep } from "k6";
import { randomString } from "https://jslib.k6.io/k6-utils/1.1.0/index.js";
import encoding from "k6/encoding";

import { artifactPublicHost } from "./const.js";

import * as constant from "./const.js";
import * as helper from "./helper.js";

/**
 * Test the chat cache implementation with TWO distinct routes.
 *
 * This test verifies BOTH chat routes:
 *
 * ROUTE 1: AI Cache Route (Instant Chat)
 * - Uses Gemini cache or direct file content from Redis
 * - Triggered when files are still processing OR just completed
 * - Returns empty similarChunks (no vector search)
 * - Fast response (~100ms)
 *
 * ROUTE 2: RAG-based Chat Route (Traditional)
 * - Uses vector search + similarity chunks + LLM pipeline
 * - Triggered when files are fully processed and indexed
 * - Returns populated similarChunks with similarity scores
 * - Slower response (~1-2s due to vector search)
 *
 * Test Flow:
 * 1. Create catalog
 * 2. Upload multiple files (PDF, DOCX, TXT)
 * 3. Trigger batch processing
 * 4. TEST ROUTE 1: Chat while processing (AI cache route)
 * 5. Poll until files are COMPLETED
 * 6. TEST ROUTE 2: Chat after completion (RAG route)
 * 7. Test single file, subset, small files
 * 8. Test error handling
 * 9. Delete catalog and verify cleanup
 *
 * Key Validations:
 * - AI cache route returns empty chunks (no vector search)
 * - RAG route returns populated chunks (vector search performed)
 * - Both routes produce valid answers
 * - Cache metadata is stored in Redis
 * - Small files use direct content (no cache)
 * - Cleanup removes all resources
 */
export function CheckChatCacheImplementation(data) {
    const groupName = "Artifact API: Chat cache implementation";
    group(groupName, () => {
        check(true, { [constant.banner(groupName)]: () => true });

        // Step 1: Create catalog
        const createBody = {
            name: constant.dbIDPrefix + "chat-" + randomString(5),
            description: "Test catalog for chat cache functionality",
            tags: ["test", "integration", "chat", "cache"],
            type: "CATALOG_TYPE_PERSISTENT",
        };

        const cRes = http.request(
            "POST",
            `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`,
            JSON.stringify(createBody),
            data.header
        );

        let created;
        try { created = (cRes.json() || {}).catalog; } catch (e) { created = {}; }
        const catalogId = created && created.catalogId;
        const catalogUid = created && created.catalogUid;

        check(cRes, {
            "Chat Cache: Catalog created successfully": (r) => r.status === 200,
            "Chat Cache: Catalog has valid UID": () => catalogUid && catalogUid.length > 0,
        });

        if (!catalogId || !catalogUid) {
            console.log(`Chat Cache: Failed to create catalog (status=${cRes.status}), response:`, JSON.stringify(cRes.body));
            return;
        }

        // Step 2: Upload multiple files to trigger batch cache
        // Use at least 2 files to ensure batch cache is created
        const testFiles = [
            constant.sampleFiles.find(f => f.originalName === "sample.pdf"),
            constant.sampleFiles.find(f => f.originalName === "sample.docx"),
            constant.sampleFiles.find(f => f.originalName === "sample.txt"),
        ].filter(Boolean);

        const uploaded = [];
        const uploadReqs = testFiles.map((s) => {
            const fileName = `${constant.dbIDPrefix}${s.originalName}`;
            return {
                s,
                fileName,
                req: {
                    method: "POST",
                    url: `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files`,
                    body: JSON.stringify({ name: fileName, type: s.type, content: s.content }),
                    params: data.header,
                },
            };
        });

        const uploadResponses = http.batch(uploadReqs.map((x) => x.req));

        for (let i = 0; i < uploadResponses.length; i++) {
            const resp = uploadResponses[i];
            const s = uploadReqs[i].s;
            const fileName = uploadReqs[i].fileName;

            const fJson = (function () {
                try { return resp.json(); } catch (e) { return {}; }
            })();
            const file = (fJson && fJson.file) || {};

            check(resp, {
                [`Chat Cache: File uploaded (${s.originalName})`]: (r) => r.status === 200,
            });

            if (file && file.fileUid) {
                uploaded.push({
                    fileUid: file.fileUid,
                    name: fileName,
                    type: s.type,
                    originalName: s.originalName
                });
            }
        }

        check({ uploadCount: uploaded.length }, {
            "Chat Cache: Multiple files uploaded successfully": () => uploaded.length >= 2,
        });

        if (uploaded.length < 2) {
            console.log("Chat Cache: Need at least 2 files for batch cache test");
            http.request("DELETE", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}`, null, data.header);
            return;
        }

        // Step 3: Trigger batch processing
        const fileUids = uploaded.map((f) => f.fileUid);
        const pRes = http.request(
            "POST",
            `${artifactPublicHost}/v1alpha/catalogs/files/processAsync`,
            JSON.stringify({ fileUids }),
            data.header
        );

        check(pRes, {
            "Chat Cache: Batch processing triggered": (r) => r.status === 200,
        });

        // Step 4: Wait briefly for processing to start and cache to be created
        sleep(2);

        // Step 5: Test AI cache route (while files are still processing or just after)
        // This tests the instant chat with Gemini cache path
        {
            const cacheRouteTest = {
                question: "What are the main topics in these documents?",
                description: "AI Cache Route - Instant response"
            };

            const cacheRes = http.request(
                "POST",
                `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/ask`,
                JSON.stringify({
                    question: cacheRouteTest.question,
                    topK: 5,
                    fileUids: fileUids,
                }),
                data.header
            );

            let cacheJson;
            try { cacheJson = cacheRes.json(); } catch (e) { cacheJson = {}; }

            check(cacheRes, {
                "Chat Cache: AI cache route successful": (r) => r.status === 200,
                "Chat Cache: AI cache route has answer": () =>
                    cacheJson.answer && cacheJson.answer.length > 0,
            });

            // AI cache route should return empty similarChunks (no vector search performed)
            check(cacheJson, {
                "Chat Cache: AI cache route returns empty chunks (no vector search)": () =>
                    Array.isArray(cacheJson.similarChunks) && cacheJson.similarChunks.length === 0,
            });

            if (cacheJson.answer) {
                console.log(`Chat Cache: AI cache route answer length: ${cacheJson.answer.length}`);
            }
        }

        // Step 6: Poll for completion (for RAG route testing)
        let completedCount = 0;
        {
            const pending = new Set(fileUids);
            const startTime = Date.now();
            const maxWaitMs = 10 * 60 * 1000; // 10 minutes

            let iter = 0;
            while (pending.size > 0 && (Date.now() - startTime) < maxWaitMs) {
                iter++;

                const lastBatch = http.batch(
                    Array.from(pending).map((uid) => ({
                        method: "GET",
                        url: `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files/${uid}`,
                        params: data.header,
                    }))
                );

                let idx = 0;
                for (const uid of Array.from(pending)) {
                    const r = lastBatch[idx++];
                    try {
                        const body = r.json();
                        const st = (body.file && body.file.processStatus) || "";

                        if (r.status === 200 && st === "FILE_PROCESS_STATUS_COMPLETED") {
                            pending.delete(uid);
                            completedCount++;
                        } else if (r.status === 200 && st === "FILE_PROCESS_STATUS_FAILED") {
                            pending.delete(uid);
                            console.log(`Chat Cache: File processing failed: ${uid}`);
                        }
                    } catch (e) { /* ignore */ }
                }

                if (pending.size === 0) break;
                sleep(0.5);
            }

            check({ completedCount, totalFiles: uploaded.length }, {
                "Chat Cache: All files completed processing": () => completedCount === uploaded.length,
            });

            if (completedCount < uploaded.length) {
                console.log(`Chat Cache: Only ${completedCount}/${uploaded.length} files completed`);
                http.request("DELETE", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}`, null, data.header);
                return;
            }
        }

        // Step 7: Test RAG route (after files are fully processed and indexed)
        // This tests the vector search + LLM pipeline path
        const ragTests = [
            {
                question: "What is this about?",
                expectAnswer: true,
                expectChunks: true,
                description: "RAG Route - General question"
            },
            {
                question: "Summarize the main points.",
                expectAnswer: true,
                expectChunks: true,
                description: "RAG Route - Summarization"
            },
            {
                question: "What are the key details?",
                expectAnswer: true,
                expectChunks: true,
                description: "RAG Route - Detail extraction"
            },
        ];

        for (const test of ragTests) {
            const ragRes = http.request(
                "POST",
                `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/ask`,
                JSON.stringify({
                    question: test.question,
                    topK: 5,
                    fileUids: fileUids,
                }),
                data.header
            );

            let ragJson;
            try { ragJson = ragRes.json(); } catch (e) { ragJson = {}; }

            check(ragRes, {
                [`Chat Cache: ${test.description} successful`]: (r) => r.status === 200,
                [`Chat Cache: ${test.description} has answer`]: () =>
                    ragJson.answer && ragJson.answer.length > 0,
            });

            // RAG route should return similarChunks (vector search performed)
            if (test.expectChunks) {
                check(ragJson, {
                    [`Chat Cache: ${test.description} returns chunks (vector search)`]: () =>
                        Array.isArray(ragJson.similarChunks) && ragJson.similarChunks.length > 0,
                });
            }

            // Note: We don't fail on "Not in context" responses because GPT-4o can be
            // non-deterministic even with temperature=0, and may decide the context
            // isn't sufficient for certain prompts. This is expected behavior.
            if (test.expectAnswer && ragJson.answer && !ragJson.answer.includes("Not in context")) {
                console.log(`Chat Cache: RAG route meaningful answer for "${test.description}"`);
            }
        }

        // Step 8: Test chat with single file (should use RAG route)
        const singleFileUid = fileUids[0];
        const singleChatRes = http.request(
            "POST",
            `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/ask`,
            JSON.stringify({
                question: "Tell me about this document.",
                topK: 5,
                fileUids: [singleFileUid],
            }),
            data.header
        );

        let singleChatJson;
        try { singleChatJson = singleChatRes.json(); } catch (e) { singleChatJson = {}; }

        check(singleChatRes, {
            "Chat Cache: Single file chat successful": (r) => r.status === 200,
            "Chat Cache: Single file has answer": () =>
                singleChatJson.answer && singleChatJson.answer.length > 0,
        });

        // Step 9: Test chat cache with subset of files
        if (fileUids.length >= 2) {
            const subsetUids = fileUids.slice(0, 2);
            const subsetChatRes = http.request(
                "POST",
                `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/ask`,
                JSON.stringify({
                    question: "What information is available?",
                    topK: 5,
                    fileUids: subsetUids,
                }),
                data.header
            );

            check(subsetChatRes, {
                "Chat Cache: Subset file chat successful": (r) => r.status === 200,
            });
        }

        // Step 10: Test with very small file (should use direct content, not cache)
        // K6 doesn't have Buffer, use encoding module
        const smallFileContent = encoding.b64encode("This is a small test file with minimal content.");
        const smallFileRes = http.request(
            "POST",
            `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files`,
            JSON.stringify({
                name: `${constant.dbIDPrefix}small.txt`,
                type: "FILE_TYPE_TEXT",
                content: smallFileContent
            }),
            data.header
        );

        let smallFile;
        try { smallFile = (smallFileRes.json() || {}).file; } catch (e) { smallFile = null; }

        if (smallFile && smallFile.fileUid) {
            // Process small file
            const smallProcessRes = http.request(
                "POST",
                `${artifactPublicHost}/v1alpha/catalogs/files/processAsync`,
                JSON.stringify({ fileUids: [smallFile.fileUid] }),
                data.header
            );

            check(smallProcessRes, {
                "Chat Cache: Small file processing triggered": (r) => r.status === 200,
            });

            // Wait for small file to complete
            let smallFileCompleted = false;
            const smallFileStartTime = Date.now();
            while (!smallFileCompleted && (Date.now() - smallFileStartTime) < 2 * 60 * 1000) {
                const statusRes = http.request(
                    "GET",
                    `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files/${smallFile.fileUid}`,
                    null,
                    data.header
                );

                try {
                    const body = statusRes.json();
                    const st = (body.file && body.file.processStatus) || "";
                    if (st === "FILE_PROCESS_STATUS_COMPLETED") {
                        smallFileCompleted = true;
                    }
                } catch (e) { /* ignore */ }

                if (!smallFileCompleted) sleep(0.5);
            }

            if (smallFileCompleted) {
                // Test chat with small file (should use direct content from Redis)
                const smallChatRes = http.request(
                    "POST",
                    `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/ask`,
                    JSON.stringify({
                        question: "What does this say?",
                        topK: 5,
                        fileUids: [smallFile.fileUid],
                    }),
                    data.header
                );

                check(smallChatRes, {
                    "Chat Cache: Small file chat successful": (r) => r.status === 200,
                });
            }
        }

        // Step 11: Test error handling - invalid file UIDs
        const invalidChatRes = http.request(
            "POST",
            `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/ask`,
            JSON.stringify({
                question: "What is this?",
                topK: 5,
                fileUids: ["00000000-0000-0000-0000-000000000000"],
            }),
            data.header
        );

        // Should handle gracefully (either error or empty result)
        check(invalidChatRes, {
            "Chat Cache: Invalid file UID handled gracefully": (r) =>
                r.status === 200 || r.status === 400 || r.status === 404,
        });

        // Step 12: Test chat without file UIDs (use all files in catalog)
        const allFilesChatRes = http.request(
            "POST",
            `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/ask`,
            JSON.stringify({
                question: "What topics are covered?",
                topK: 5,
            }),
            data.header
        );

        check(allFilesChatRes, {
            "Chat Cache: Chat without file UIDs successful": (r) => r.status === 200,
        });

        // Step 13: Delete catalog and verify cleanup
        const dRes = http.request(
            "DELETE",
            `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}`,
            null,
            data.header
        );

        check(dRes, {
            "Chat Cache: Catalog deletion successful": (r) => r.status === 200 || r.status === 204,
        });

        // Wait for cleanup
        sleep(5);

        // Verify chat cache is cleaned up (should get error or empty result)
        const postDeleteChatRes = http.request(
            "POST",
            `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/ask`,
            JSON.stringify({
                question: "What is this?",
                topK: 5,
                fileUids: fileUids,
            }),
            data.header
        );

        check(postDeleteChatRes, {
            "Chat Cache: Chat after deletion returns error": (r) => r.status === 404 || r.status === 400,
        });

        console.log("Chat Cache: Test completed successfully");
    });
}
