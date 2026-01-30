import http from "k6/http";
import {
    check,
    group
} from "k6";
import {
    randomString
} from "https://jslib.k6.io/k6-utils/1.1.0/index.js";

import * as constant from "./const.js";
import * as helper from "./helper.js";

/**
 * TEST SUITE: Namespace Permission Checks
 *
 * PURPOSE:
 * Tests the namespace-level permission checks for Artifact backend APIs.
 *
 * Test Scenarios:
 * 1. User can list KBs in their own namespace (positive)
 * 2. User can list files in their own knowledge base (positive)
 * 3. User cannot list KBs in another user's namespace (401/403/404)
 * 4. User cannot list files in another user's knowledge base (401/403/404)
 */

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
        dbIdPrefix: constant.generateDBIDPrefix(),
    };
}

export default function (data) {
    if (!data || !data.header || !data.expectedOwner) {
        console.log("Skipping namespace permission tests: no auth data or user info");
        return;
    }
    testNamespacePermissions(data);
}

export function teardown(data) {
    if (!data || !data.header || !data.expectedOwner || !data.dbIdPrefix) {
        return;
    }

    // Cleanup any test knowledge bases created during tests
    const apiHost = constant.artifactRESTPublicHost;
    const listResp = helper.httpRetry.get(
        `${apiHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases`,
        data.header
    );

    if (listResp.status === 200) {
        const kbs = listResp.json().knowledgeBases || [];
        for (const kb of kbs) {
            if (kb.displayName && kb.displayName.startsWith(data.dbIdPrefix)) {
                helper.httpRetry.del(
                    `${apiHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${kb.id}`,
                    data.header
                );
            }
        }
    }
}

export function testNamespacePermissions(data) {
    const { header, expectedOwner, dbIdPrefix } = data;
    const apiHost = constant.artifactRESTPublicHost;

    // ===============================================================
    // Test 1: User CAN list knowledge bases in their own namespace
    // ===============================================================
    group("Namespace Permission: User can list KBs in own namespace", () => {
        const listResp = http.request(
            "GET",
            `${apiHost}/v1alpha/namespaces/${expectedOwner.id}/knowledge-bases`,
            null,
            header
        );

        check(listResp, {
            "[Perm] List KBs in own namespace returns 200": (r) => r.status === 200,
            "[Perm] List KBs returns knowledgeBases array": (r) => {
                try {
                    const body = r.json();
                    return Array.isArray(body.knowledgeBases);
                } catch (e) {
                    console.error(`Failed to parse KBs JSON (status ${r.status}): ${r.body}`);
                    return false;
                }
            },
        });
    });

    // ===============================================================
    // Test 2: User CAN list files in their own knowledge base
    // ===============================================================
    group("Namespace Permission: User can list files in own knowledge base", () => {
        let knowledgeBaseId = null;
        let fileId = null;

        // Setup: Create a KB and file to ensure we have something to list
        group("Setup: Create test KB and file", () => {
            const createKBResp = helper.httpRetry.post(
                `${apiHost}/v1alpha/namespaces/${expectedOwner.id}/knowledge-bases`,
                JSON.stringify({
                    displayName: dbIdPrefix + "perm-test-kb-" + randomString(4),
                    description: "Test KB for namespace permission tests",
                    type: "KNOWLEDGE_BASE_TYPE_PERSISTENT",
                }),
                header
            );

            if (createKBResp.status === 200 || createKBResp.status === 201) {
                knowledgeBaseId = createKBResp.json().knowledgeBase?.id;
            }

            if (knowledgeBaseId) {
                const createFileResp = helper.httpRetry.post(
                    `${apiHost}/v1alpha/namespaces/${expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files`,
                    JSON.stringify({
                        displayName: "perm-test-file-" + randomString(4) + ".txt",
                        type: "TYPE_TEXT",
                        content: "dGVzdCBjb250ZW50" // base64 "test content"
                    }),
                    header
                );

                if (createFileResp.status === 200 || createFileResp.status === 201) {
                    fileId = createFileResp.json().file?.id;
                }
            }
        });

        if (knowledgeBaseId) {
            // Test: List files in own KB - should succeed
            group("List files in own KB succeeds", () => {
                const listResp = http.request(
                    "GET",
                    `${apiHost}/v1alpha/namespaces/${expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files`,
                    null,
                    header
                );

                check(listResp, {
                    "[Perm] List files in own KB returns 200": (r) => r.status === 200,
                    "[Perm] List files returns files array": (r) => {
                        try {
                            const body = r.json();
                            return Array.isArray(body.files);
                        } catch (e) {
                            console.error(`Failed to parse files JSON (status ${r.status}): ${r.body}`);
                            return false;
                        }
                    },
                    "[Perm] Listed files include our test file": (r) => {
                        if (!fileId) return true;
                        try {
                            const body = r.json();
                            return body.files && body.files.some(f => f.id === fileId);
                        } catch (e) {
                            return false;
                        }
                    },
                });
            });

            // Cleanup
            helper.httpRetry.del(
                `${apiHost}/v1alpha/namespaces/${expectedOwner.id}/knowledge-bases/${knowledgeBaseId}`,
                header
            );
        }
    });

    // ===============================================================
    // Test 3: User CANNOT list knowledge bases in another user's namespace
    // ===============================================================
    group("Namespace Permission: Cannot access other namespace's KBs", () => {
        const otherNamespaceId = "other-user-namespace-" + randomString(8);

        group("List KBs in other namespace fails", () => {
            const listResp = http.request(
                "GET",
                `${apiHost}/v1alpha/namespaces/${otherNamespaceId}/knowledge-bases`,
                null,
                header
            );

            console.log(`Scenario 3: List KBs in other namespace '${otherNamespaceId}' returned status ${listResp.status}`);

            if (listResp.status === 200) {
                console.error(`SECURITY WARNING: List KBs in other namespace '${otherNamespaceId}' returned 200!`);
            }

            check(listResp, {
                "[Perm] List KBs in other namespace returns 401/403/404": (r) => {
                    return r.status === 401 || r.status === 403 || r.status === 404;
                },
            });
        });
    });

    // ===============================================================
    // Test 4: User CANNOT list files in another user's knowledge base
    // ===============================================================
    group("Namespace Permission: Cannot access other namespace's files", () => {
        const otherNamespaceId = "other-user-namespace-" + randomString(8);
        const someKbId = "some-kb-id";

        group("List files in other namespace fails", () => {
            const listResp = http.request(
                "GET",
                `${apiHost}/v1alpha/namespaces/${otherNamespaceId}/knowledge-bases/${someKbId}/files`,
                null,
                header
            );

            if (listResp.status === 200) {
                console.error(`SECURITY WARNING: List files in other namespace '${otherNamespaceId}' returned 200!`);
            }

            check(listResp, {
                "[Perm] List files in other namespace returns 401/403/404": (r) => {
                    return r.status === 401 || r.status === 403 || r.status === 404;
                },
            });
        });
    });

    // Banner for visual test completion
    check(null, {
        [constant.banner("Namespace Permission Tests Complete")]: () => true,
    });
}
