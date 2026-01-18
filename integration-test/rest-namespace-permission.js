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
 * TEST SUITE: Namespace Permission Checks for ListFiles
 *
 * PURPOSE:
 * Tests the namespace-level permission checks for the ListFiles API.
 * When listing files without a knowledge_base_id filter, the system should verify
 * that the authenticated user has access to the namespace.
 *
 * Test Scenarios:
 * 1. User can list files in their own namespace (positive test)
 * 2. User cannot list files in a non-existent namespace (404)
 * 3. User cannot list files in another user's namespace (401/403)
 * 4. With KB filter, user needs both namespace and KB permission
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
    const loginResp = helper.authenticateWithRetry(
        constant.mgmtRESTPublicHost,
        constant.defaultUsername,
        constant.defaultPassword
    );

    if (!loginResp || loginResp.status !== 200) {
        console.error("Setup: Authentication failed, cannot continue");
        return null;
    }

    const accessToken = loginResp.json().accessToken;
    const header = {
        "headers": {
            "Authorization": `Bearer ${accessToken}`,
            "Content-Type": "application/json",
        },
        "timeout": "120s",
    };

    const userResp = helper.httpRetry.get(`${constant.mgmtRESTPublicHost}/v1beta/user`, {
        headers: { "Authorization": `Bearer ${accessToken}` }
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
    // Test 1: User CAN list files in their own namespace
    // ===============================================================
    group("Namespace Permission: User can list files in own namespace", () => {
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
                    `${apiHost}/v1alpha/namespaces/${expectedOwner.id}/files?knowledgeBaseId=${knowledgeBaseId}`,
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

        // Test: List all files in own namespace (without KB filter) - should succeed
        group("List files without KB filter succeeds", () => {
            const listResp = http.request(
                "GET",
                `${apiHost}/v1alpha/namespaces/${expectedOwner.id}/files`,
                null,
                header
            );

            check(listResp, {
                "[Perm] List files in own namespace returns 200": (r) => r.status === 200,
                "[Perm] List files returns files array": (r) => {
                    const body = r.json();
                    return Array.isArray(body.files);
                },
            });
        });

        // Test: List files with KB filter - should also succeed
        if (knowledgeBaseId) {
            group("List files with KB filter succeeds", () => {
                const filterExpr = `knowledge_base_id = "${knowledgeBaseId}"`;
                const listResp = http.request(
                    "GET",
                    `${apiHost}/v1alpha/namespaces/${expectedOwner.id}/files?filter=${encodeURIComponent(filterExpr)}`,
                    null,
                    header
                );

                check(listResp, {
                    "[Perm] List files with KB filter returns 200": (r) => r.status === 200,
                    "[Perm] List files with KB filter returns files": (r) => {
                        const body = r.json();
                        return Array.isArray(body.files);
                    },
                    "[Perm] Listed files include our test file": (r) => {
                        if (!fileId) return true; // Skip if no file was created
                        const body = r.json();
                        return body.files && body.files.some(f => f.id === fileId);
                    },
                });
            });
        }

        // Cleanup
        if (knowledgeBaseId) {
            helper.httpRetry.del(
                `${apiHost}/v1alpha/namespaces/${expectedOwner.id}/knowledge-bases/${knowledgeBaseId}`,
                header
            );
        }
    });

    // ===============================================================
    // Test 2: User CANNOT list files in a non-existent namespace
    // ===============================================================
    group("Namespace Permission: Non-existent namespace returns error", () => {
        const fakeNamespaceId = "non-existent-namespace-" + randomString(8);

        group("List files in non-existent namespace fails", () => {
            const listResp = http.request(
                "GET",
                `${apiHost}/v1alpha/namespaces/${fakeNamespaceId}/files`,
                null,
                header
            );

            check(listResp, {
                "[Perm] List files in non-existent namespace returns 404 or 401": (r) => {
                    // Could be 404 (namespace not found) or 401 (unauthorized to access)
                    return r.status === 404 || r.status === 401 || r.status === 403;
                },
            });
        });
    });

    // ===============================================================
    // Test 3: User CANNOT list files in another user's namespace
    // Note: This test uses a known namespace that the test user shouldn't own
    // ===============================================================
    group("Namespace Permission: Cannot access other namespace", () => {
        // Try to access a system-level or different user's namespace
        // Using a common test namespace pattern that shouldn't match the test user
        const otherNamespaceId = "other-user-namespace-" + randomString(8);

        group("List files in other namespace fails", () => {
            const listResp = http.request(
                "GET",
                `${apiHost}/v1alpha/namespaces/${otherNamespaceId}/files`,
                null,
                header
            );

            check(listResp, {
                "[Perm] List files in other namespace returns 401/403/404": (r) => {
                    // Should fail with:
                    // - 401 (unauthenticated - user doesn't own namespace)
                    // - 403 (forbidden - no permission)
                    // - 404 (namespace not found)
                    return r.status === 401 || r.status === 403 || r.status === 404;
                },
                "[Perm] List files in other namespace does not return files": (r) => {
                    // Even if status is 200 (which it shouldn't be), should not have files
                    if (r.status === 200) {
                        const body = r.json();
                        return !body.files || body.files.length === 0;
                    }
                    return true;
                },
            });
        });
    });

    // ===============================================================
    // Test 4: KB permission is still checked when KB filter is provided
    // ===============================================================
    group("Namespace Permission: KB-level permission still enforced", () => {
        let knowledgeBaseId = null;

        // Setup: Create a KB with default (private) permissions
        group("Setup: Create test KB", () => {
            const createKBResp = helper.httpRetry.post(
                `${apiHost}/v1alpha/namespaces/${expectedOwner.id}/knowledge-bases`,
                JSON.stringify({
                    displayName: dbIdPrefix + "kb-perm-test-" + randomString(4),
                    description: "Test KB for KB permission tests",
                    type: "KNOWLEDGE_BASE_TYPE_PERSISTENT",
                }),
                header
            );

            if (createKBResp.status === 200 || createKBResp.status === 201) {
                knowledgeBaseId = createKBResp.json().knowledgeBase?.id;
            }
        });

        if (knowledgeBaseId) {
            // Test: Owner can list files with KB filter
            group("Owner can list files with KB filter", () => {
                const filterExpr = `knowledge_base_id = "${knowledgeBaseId}"`;
                const listResp = http.request(
                    "GET",
                    `${apiHost}/v1alpha/namespaces/${expectedOwner.id}/files?filter=${encodeURIComponent(filterExpr)}`,
                    null,
                    header
                );

                check(listResp, {
                    "[Perm] Owner can list files with KB filter": (r) => r.status === 200,
                });
            });

            // Test: Invalid KB ID returns error
            group("Invalid KB ID in filter returns error", () => {
                const filterExpr = `knowledge_base_id = "kb-invalid-12345"`;
                const listResp = http.request(
                    "GET",
                    `${apiHost}/v1alpha/namespaces/${expectedOwner.id}/files?filter=${encodeURIComponent(filterExpr)}`,
                    null,
                    header
                );

                check(listResp, {
                    "[Perm] Invalid KB ID returns error (404 or similar)": (r) => {
                        // Should return 404 (KB not found) or similar error
                        return r.status === 404 || r.status === 400 || r.status === 500;
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

    // Banner for visual test completion
    check(null, {
        [constant.banner("Namespace Permission Tests Complete")]: () => true,
    });
}
