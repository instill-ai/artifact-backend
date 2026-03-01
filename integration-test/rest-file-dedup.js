import { check, group } from "k6";
import { randomString } from "https://jslib.k6.io/k6-utils/1.1.0/index.js";

import * as constant from "./const.js";
import * as helper from "./helper.js";

export let options = {
    setupTimeout: '60s',
    teardownTimeout: '60s',
    insecureSkipTLSVerify: true,
    thresholds: {
        checks: ["rate == 1.0"],
    },
};

export function setup() {
    const authHeader = helper.getBasicAuthHeader(constant.defaultUsername, constant.defaultPassword);
    const header = {
        "headers": {
            "Authorization": authHeader,
            "Content-Type": "application/json",
        },
        "timeout": "120s",
    };

    const userResp = helper.httpRetry.get(`${constant.mgmtRESTPublicHost}/v1beta/user`, {
        headers: { "Authorization": authHeader }
    });

    return {
        header: header,
        expectedOwner: userResp.json().user,
    };
}

export default function (data) {
    if (!data || !data.header || !data.expectedOwner) {
        console.log("Skipping file dedup tests: no auth data or user info");
        return;
    }
    checkFileDedup(data);
}

export function checkFileDedup(data) {
    const { header, expectedOwner } = data;
    if (!expectedOwner || !expectedOwner.id) {
        console.error("checkFileDedup: expectedOwner is missing or has no id");
        return;
    }
    const apiHost = constant.artifactRESTPublicHost;
    const nsId = expectedOwner.id;

    // Helper: create a knowledge base with a random display name
    function createKB(suffix) {
        const displayName = "dedup-test-kb-" + randomString(8) + "-" + suffix;
        const resp = helper.httpRetry.post(
            `${apiHost}/v1alpha/namespaces/${nsId}/knowledge-bases`,
            JSON.stringify({
                displayName: displayName,
                description: "KB for file dedup integration test",
                tags: ["test", "dedup"],
                type: "KNOWLEDGE_BASE_TYPE_PERSISTENT",
            }),
            header
        );
        check(resp, { [`Create KB ${suffix} succeeds`]: (r) => r.status === 200 || r.status === 201 });
        const kb = resp.json().knowledgeBase;
        return kb;
    }

    // Helper: upload a file to a KB
    function uploadFile(kbId, displayName, content, type) {
        return helper.httpRetry.post(
            `${apiHost}/v1alpha/namespaces/${nsId}/knowledge-bases/${kbId}/files`,
            JSON.stringify({
                file: { displayName: displayName, type: type, content: content },
            }),
            header
        );
    }

    // Helper: delete a file
    function deleteFile(kbId, fileId) {
        return helper.httpRetry.request(
            "DELETE",
            `${apiHost}/v1alpha/namespaces/${nsId}/knowledge-bases/${kbId}/files/${fileId}`,
            null,
            header
        );
    }

    // Helper: delete a KB
    function deleteKB(kbId) {
        helper.httpRetry.request(
            "DELETE",
            `${apiHost}/v1alpha/namespaces/${nsId}/knowledge-bases/${kbId}`,
            null,
            header
        );
    }

    // Helper: assert 409 with DUPLICATE_FILE_CONTENT details
    function assertDuplicateResponse(res, testLabel) {
        check(res, {
            [`${testLabel}: returns 409`]: (r) => r.status === 409,
            [`${testLabel}: has details array`]: (r) => {
                const body = r.json();
                return body.details && body.details.length > 0;
            },
            [`${testLabel}: reason is DUPLICATE_FILE_CONTENT`]: (r) => {
                const detail = r.json().details.find(d => d.reason === "DUPLICATE_FILE_CONTENT");
                return detail !== undefined;
            },
            [`${testLabel}: has existing_file_name`]: (r) => {
                const detail = r.json().details.find(d => d.reason === "DUPLICATE_FILE_CONTENT");
                return detail && detail.metadata && detail.metadata.existing_file_name;
            },
            [`${testLabel}: has existing_file_id`]: (r) => {
                const detail = r.json().details.find(d => d.reason === "DUPLICATE_FILE_CONTENT");
                return detail && detail.metadata && detail.metadata.existing_file_id;
            },
        });
    }

    // ================================================================
    // TEST_01: Same content, same KB -- blocked (409)
    // ================================================================
    group("TEST_01: Same content, same KB -- blocked (409)", () => {
        const kb = createKB("t01");
        const content = constant.docSampleTxt;

        const resp1 = uploadFile(kb.id, "file-a-" + randomString(4) + ".txt", content, "TYPE_TEXT");
        check(resp1, { "First upload succeeds": (r) => r.status === 200 || r.status === 201 });

        const resp2 = uploadFile(kb.id, "file-b-" + randomString(4) + ".txt", content, "TYPE_TEXT");
        assertDuplicateResponse(resp2, "TEST_01");

        deleteKB(kb.id);
    });

    // ================================================================
    // TEST_02: Same content, different KB -- blocked (409)
    // ================================================================
    group("TEST_02: Same content, different KB -- blocked (409)", () => {
        const kbA = createKB("t02-a");
        const kbB = createKB("t02-b");
        const content = constant.docSampleMd;

        const resp1 = uploadFile(kbA.id, "cross-kb-" + randomString(4) + ".md", content, "TYPE_MARKDOWN");
        check(resp1, { "Upload to KB-A succeeds": (r) => r.status === 200 || r.status === 201 });

        const resp2 = uploadFile(kbB.id, "cross-kb-" + randomString(4) + ".md", content, "TYPE_MARKDOWN");
        assertDuplicateResponse(resp2, "TEST_02");

        deleteKB(kbA.id);
        deleteKB(kbB.id);
    });

    // ================================================================
    // TEST_03: Different content, same KB -- allowed (200)
    // ================================================================
    group("TEST_03: Different content, same KB -- allowed (200)", () => {
        const kb = createKB("t03");

        const resp1 = uploadFile(kb.id, "unique-a-" + randomString(4) + ".txt", constant.docSampleTxt, "TYPE_TEXT");
        check(resp1, { "First upload succeeds (txt)": (r) => r.status === 200 || r.status === 201 });

        const resp2 = uploadFile(kb.id, "unique-b-" + randomString(4) + ".pdf", constant.docSamplePdf, "TYPE_PDF");
        check(resp2, { "Second upload succeeds (pdf, different content)": (r) => r.status === 200 || r.status === 201 });

        deleteKB(kb.id);
    });

    // ================================================================
    // TEST_04: Re-upload after soft delete -- allowed (200)
    // ================================================================
    group("TEST_04: Re-upload after soft delete -- allowed (200)", () => {
        const kb = createKB("t04");
        const content = constant.docSampleCsv;

        const resp1 = uploadFile(kb.id, "deleteme-" + randomString(4) + ".csv", content, "TYPE_CSV");
        check(resp1, { "Initial upload succeeds": (r) => r.status === 200 || r.status === 201 });

        const file1 = resp1.json().file;
        const delResp = deleteFile(kb.id, file1.id);
        check(delResp, { "Delete succeeds": (r) => r.status === 200 });

        const resp2 = uploadFile(kb.id, "reupload-" + randomString(4) + ".csv", content, "TYPE_CSV");
        check(resp2, { "Re-upload after delete succeeds (200)": (r) => r.status === 200 || r.status === 201 });

        deleteKB(kb.id);
    });

    // ================================================================
    // TEST_05: Verify content_sha256 stored in DB
    // ================================================================
    group("TEST_05: Verify content_sha256 stored in DB", () => {
        const kb = createKB("t05");
        const content = constant.docSampleHtml;

        const resp1 = uploadFile(kb.id, "hash-check-" + randomString(4) + ".html", content, "TYPE_HTML");
        check(resp1, { "Upload for hash check succeeds": (r) => r.status === 200 || r.status === 201 });

        const file1 = resp1.json().file;
        const fileUid = file1.id;

        const query = `SELECT content_sha256 FROM file WHERE id = '${fileUid}' AND delete_time IS NULL LIMIT 1`;
        const result = constant.artifactDb.exec(query);

        check(result, {
            "DB query returns a row": (r) => r.length > 0,
            "content_sha256 is a 64-char hex string": (r) => {
                if (r.length === 0) return false;
                const hash = r[0].content_sha256;
                return hash && hash.length === 64 && /^[0-9a-f]{64}$/.test(hash);
            },
        });

        deleteKB(kb.id);
    });
}
