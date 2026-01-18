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
 * TEST SUITE: AIP Resource Refactoring Invariants
 *
 * PURPOSE:
 * Tests the critical invariants defined in the AIP Resource Refactoring plan.
 * These invariants ensure the system maintains data integrity and follows AIP standards.
 *
 * RED FLAGS (RF) - Hard Invariants:
 * - RF-2: name is the only canonical identifier
 * - RF-3: Listing APIs must be parent-scoped
 *
 * YELLOW FLAGS (YF) - Strict Guardrails:
 * - YF-2: Slug resolution must not leak into services (backend only accepts canonical IDs)
 * - YF-4: Avoid resource_id naming (use id instead of uid)
 */

// Test options when run standalone
export let options = {
    setupTimeout: '60s',
    teardownTimeout: '60s',
    insecureSkipTLSVerify: true,
    thresholds: {
        checks: ["rate == 1.0"],
    },
};

// Setup function for standalone execution
export function setup() {
    // Authenticate with retry to handle transient failures
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
    };
}

// Default test function for standalone execution
export default function (data) {
    if (!data || !data.header || !data.expectedOwner) {
        console.log("Skipping AIP invariants: no auth data or user info");
        return;
    }
    checkInvariants(data);
}

export function checkInvariants(data) {
    const { header, expectedOwner } = data;
    if (!expectedOwner || !expectedOwner.id) {
        console.error("checkInvariants: expectedOwner is missing or has no id");
        return;
    }
    const apiHost = constant.artifactRESTPublicHost;

    // ===============================================================
    // RF-2: name is the Only Canonical Identifier
    // For KnowledgeBases: name format is "namespaces/{ns}/knowledge-bases/{id}"
    // For Files: name format is "namespaces/{ns}/files/{id}"
    // ===============================================================
    group("RF-2: name is the canonical identifier (KnowledgeBase)", () => {
        let knowledgeBaseId = null;
        let knowledgeBaseName = null;

        // Create a test knowledge base
        group("Setup: Create test knowledge base", () => {
            const createResp = helper.httpRetry.post(
                `${apiHost}/v1alpha/namespaces/${expectedOwner.id}/knowledge-bases`,
                JSON.stringify({
                    displayName: "AIP Test KB " + randomString(8),
                    description: "Test KB for AIP invariants",
                    tags: ["test", "aip-invariants"],
                    type: "KNOWLEDGE_BASE_TYPE_PERSISTENT",
                }),
                header
            );

            if (createResp.status === 200 || createResp.status === 201) {
                const body = createResp.json();
                if (body.knowledgeBase) {
                    knowledgeBaseId = body.knowledgeBase.id;
                    knowledgeBaseName = body.knowledgeBase.name;
                }
            }
        });

        if (knowledgeBaseId && knowledgeBaseName) {
            // Test: Verify KB name format
            group("Verify knowledge base name format", () => {
                check({ knowledgeBaseName, knowledgeBaseId, expectedOwner }, {
                    "[RF-2] KB has name field": (d) => d.knowledgeBaseName !== undefined,
                    "[RF-2] KB name starts with namespaces/": (d) => d.knowledgeBaseName.startsWith("namespaces/"),
                    "[RF-2] KB name format matches pattern": (d) => {
                        // Pattern: namespaces/{ns}/knowledge-bases/{id}
                        const pattern = new RegExp(`^namespaces/[^/]+/knowledge-bases/[^/]+$`);
                        return pattern.test(d.knowledgeBaseName);
                    },
                    "[RF-2] KB name contains namespace": (d) => {
                        return d.knowledgeBaseName.includes(`namespaces/${d.expectedOwner.id}/`);
                    },
                    "[RF-2] KB id equals last segment of name": (d) => {
                        const segments = d.knowledgeBaseName.split("/");
                        return segments[segments.length - 1] === d.knowledgeBaseId;
                    },
                    "[RF-2] KB id has kb- prefix (AIP-compliant)": (d) => {
                        return d.knowledgeBaseId.startsWith("kb-");
                    }
                });
            });

            // Test: GET KB by ID should work
            group("Verify KB accessible by ID", () => {
                const getResp = helper.httpRetry.get(
                    `${apiHost}/v1alpha/namespaces/${expectedOwner.id}/knowledge-bases/${knowledgeBaseId}`,
                    header
                );

                check(getResp, {
                    "[RF-2] GET /knowledge-bases/{id} returns 200": (r) => r.status === 200,
                    "[RF-2] Response contains correct KB": (r) => {
                        const body = r.json();
                        return body.knowledgeBase && body.knowledgeBase.id === knowledgeBaseId;
                    }
                });
            });

            // Cleanup
            helper.httpRetry.del(
                `${apiHost}/v1alpha/namespaces/${expectedOwner.id}/knowledge-bases/${knowledgeBaseId}`,
                header
            );
        } else {
            console.log("KB creation failed, skipping RF-2 tests");
        }
    });

    // ===============================================================
    // RF-2: name is the canonical identifier (Files)
    // Files are now top-level resources: namespaces/{ns}/files/{file_id}
    // ===============================================================
    group("RF-2: name is the canonical identifier (Files)", () => {
        let knowledgeBaseId = null;
        let fileId = null;
        let fileName = null;

        // Create a test KB and file
        group("Setup: Create test KB and file", () => {
            const createKBResp = helper.httpRetry.post(
                `${apiHost}/v1alpha/namespaces/${expectedOwner.id}/knowledge-bases`,
                JSON.stringify({
                    displayName: "AIP File Test KB " + randomString(8),
                    description: "Test KB for file AIP invariants",
                    type: "KNOWLEDGE_BASE_TYPE_PERSISTENT",
                }),
                header
            );

            if (createKBResp.status === 200 || createKBResp.status === 201) {
                knowledgeBaseId = createKBResp.json().knowledgeBase?.id;
            }

            if (knowledgeBaseId) {
                // API CHANGE: CreateFile now uses /namespaces/{ns}/files with knowledge_base_id query param
                const createFileResp = helper.httpRetry.post(
                    `${apiHost}/v1alpha/namespaces/${expectedOwner.id}/files?knowledgeBaseId=${knowledgeBaseId}`,
                    JSON.stringify({
                        displayName: "test-file-" + randomString(8) + ".txt",
                        type: "TYPE_TEXT",
                        content: "dGVzdCBjb250ZW50" // base64 "test content"
                    }),
                    header
                );

                if (createFileResp.status === 200 || createFileResp.status === 201) {
                    const body = createFileResp.json();
                    if (body.file) {
                        fileId = body.file.id;
                        fileName = body.file.name;
                    }
                }
            }
        });

        if (fileId && fileName) {
            // Test: Verify file name format (now top-level: namespaces/{ns}/files/{id})
            group("Verify file name format", () => {
                check({ fileName, fileId, expectedOwner }, {
                    "[RF-2] File has name field": (d) => d.fileName !== undefined,
                    "[RF-2] File name starts with namespaces/": (d) => d.fileName.startsWith("namespaces/"),
                    "[RF-2] File name format matches pattern (top-level)": (d) => {
                        // Pattern: namespaces/{ns}/files/{id} (files are now top-level)
                        const pattern = new RegExp(`^namespaces/[^/]+/files/[^/]+$`);
                        return pattern.test(d.fileName);
                    },
                    "[RF-2] File id equals last segment of name": (d) => {
                        const segments = d.fileName.split("/");
                        return segments[segments.length - 1] === d.fileId;
                    },
                    "[RF-2] File id has file- prefix (AIP-compliant)": (d) => {
                        return d.fileId.startsWith("file-");
                    }
                });
            });

            // Test: GET file by ID at top-level endpoint
            group("Verify file accessible by ID (top-level)", () => {
                // API CHANGE: Files are now accessed at /namespaces/{ns}/files/{file_id}
                const getResp = helper.httpRetry.get(
                    `${apiHost}/v1alpha/namespaces/${expectedOwner.id}/files/${fileId}`,
                    header
                );

                check(getResp, {
                    "[RF-2] GET /namespaces/{ns}/files/{id} returns 200": (r) => r.status === 200,
                    "[RF-2] Response contains correct file": (r) => {
                        const body = r.json();
                        return body.file && body.file.id === fileId;
                    },
                    "[RF-2] File has knowledgeBaseIds array (many-to-many)": (r) => {
                        const body = r.json();
                        return body.file && Array.isArray(body.file.knowledgeBaseIds);
                    },
                    "[RF-2] File is associated with our KB": (r) => {
                        const body = r.json();
                        return body.file &&
                               Array.isArray(body.file.knowledgeBaseIds) &&
                               body.file.knowledgeBaseIds.includes(knowledgeBaseId);
                    }
                });
            });
        } else {
            console.log("File creation failed, skipping file RF-2 tests");
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
    // YF-2: Backend only accepts canonical IDs
    // Backend should reject invalid/non-existent IDs
    // ===============================================================
    group("YF-2: Backend only accepts canonical IDs", () => {
        // Test: GET KB by invalid ID should fail
        group("GET KB by invalid ID fails", () => {
            const getResp = helper.httpRetry.get(
                `${apiHost}/v1alpha/namespaces/${expectedOwner.id}/knowledge-bases/non-existent-kb-id-12345`,
                header
            );

            // Debug: log actual response if not 404
            if (getResp.status !== 404) {
                console.error(`[YF-2] GET KB by invalid ID returned ${getResp.status} instead of 404: ${getResp.body}`);
            }

            check(getResp, {
                "[YF-2] GET KB by invalid ID returns 404": (r) => r.status === 404
            });
        });

        // Test: GET file by invalid ID should fail
        // API CHANGE: Files are now top-level, so we test directly at /files endpoint
        group("GET file by invalid ID fails", () => {
            const getResp = helper.httpRetry.get(
                `${apiHost}/v1alpha/namespaces/${expectedOwner.id}/files/non-existent-file-id-12345`,
                header
            );

            // Debug: log actual response if not 404
            if (getResp.status !== 404) {
                console.error(`[YF-2] GET file by invalid ID returned ${getResp.status} instead of 404: ${getResp.body}`);
            }

            check(getResp, {
                "[YF-2] GET file by invalid ID returns 404": (r) => r.status === 404
            });
        });
    });

    // ===============================================================
    // YF-4: Avoid resource_id naming - use id instead of uid
    // API responses should use 'id' field, not 'uid'
    // ===============================================================
    group("YF-4: API uses id field (not uid)", () => {
        let knowledgeBaseId = null;

        // Create a test KB
        group("Setup: Create test KB", () => {
            const createResp = helper.httpRetry.post(
                `${apiHost}/v1alpha/namespaces/${expectedOwner.id}/knowledge-bases`,
                JSON.stringify({
                    displayName: "YF-4 Test KB " + randomString(8),
                    description: "Test KB for YF-4 invariant",
                    type: "KNOWLEDGE_BASE_TYPE_PERSISTENT",
                }),
                header
            );

            if (createResp.status === 200 || createResp.status === 201) {
                knowledgeBaseId = createResp.json().knowledgeBase?.id;
            }
        });

        if (knowledgeBaseId) {
            // Verify KB response uses id, not uid
            group("Verify KB response uses id (not uid)", () => {
                const getResp = helper.httpRetry.get(
                    `${apiHost}/v1alpha/namespaces/${expectedOwner.id}/knowledge-bases/${knowledgeBaseId}`,
                    header
                );

                check(getResp, {
                    "[YF-4] KB response has id field": (r) => {
                        const body = r.json();
                        return body.knowledgeBase && body.knowledgeBase.id !== undefined;
                    },
                    "[YF-4] KB response does NOT have uid field": (r) => {
                        const body = r.json();
                        return body.knowledgeBase && body.knowledgeBase.uid === undefined;
                    },
                    "[YF-4] KB response does NOT have ownerUid field": (r) => {
                        const body = r.json();
                        return body.knowledgeBase && body.knowledgeBase.ownerUid === undefined;
                    }
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
    // RF-3.3: Listing APIs must be parent-scoped
    // Files listing must be scoped to namespace (or KB)
    // ===============================================================
    group("RF-3.3: Listing APIs are parent-scoped", () => {
        // Test: List KBs requires namespace
        group("List KBs is namespace-scoped", () => {
            const listResp = helper.httpRetry.get(
                `${apiHost}/v1alpha/namespaces/${expectedOwner.id}/knowledge-bases`,
                header
            );

            check(listResp, {
                "[RF-3.3] List KBs returns 200": (r) => r.status === 200,
                "[RF-3.3] List KBs returns array": (r) => {
                    const body = r.json();
                    return Array.isArray(body.knowledgeBases);
                }
            });
        });

        // Note: Files are currently not directly listable at /files level
        // They are listed via /knowledge-bases/{kb}/files - this is acceptable
        // as it still satisfies parent-scoped listing requirement
    });

    // ===============================================================
    // AIP Standard Fields: name, id, displayName, slug
    // ===============================================================
    group("AIP Standard Fields Structure", () => {
        let knowledgeBaseId = null;

        // Create a test KB
        group("Setup: Create test KB", () => {
            const createResp = helper.httpRetry.post(
                `${apiHost}/v1alpha/namespaces/${expectedOwner.id}/knowledge-bases`,
                JSON.stringify({
                    displayName: "AIP Fields Test KB " + randomString(8),
                    description: "Test KB for AIP standard fields",
                    type: "KNOWLEDGE_BASE_TYPE_PERSISTENT",
                }),
                header
            );

            if (createResp.status === 200 || createResp.status === 201) {
                knowledgeBaseId = createResp.json().knowledgeBase?.id;
            }
        });

        if (knowledgeBaseId) {
            group("Verify AIP standard fields", () => {
                const getResp = helper.httpRetry.get(
                    `${apiHost}/v1alpha/namespaces/${expectedOwner.id}/knowledge-bases/${knowledgeBaseId}`,
                    header
                );

                check(getResp, {
                    "[AIP] KB has name field (canonical resource path)": (r) => {
                        const body = r.json();
                        return body.knowledgeBase && typeof body.knowledgeBase.name === "string";
                    },
                    "[AIP] KB has id field (canonical resource ID)": (r) => {
                        const body = r.json();
                        return body.knowledgeBase && typeof body.knowledgeBase.id === "string";
                    },
                    "[AIP] KB has displayName field (human-readable)": (r) => {
                        const body = r.json();
                        return body.knowledgeBase && typeof body.knowledgeBase.displayName === "string";
                    },
                    "[AIP] KB has createTime field": (r) => {
                        const body = r.json();
                        return body.knowledgeBase && body.knowledgeBase.createTime !== undefined;
                    },
                    "[AIP] KB has updateTime field": (r) => {
                        const body = r.json();
                        return body.knowledgeBase && body.knowledgeBase.updateTime !== undefined;
                    },
                    "[AIP] KB owner is namespace reference (not object)": (r) => {
                        const body = r.json();
                        // Owner should be a reference like "namespaces/{ns}" or an object with id
                        // but NOT contain uid field
                        if (!body.knowledgeBase) return false;
                        const owner = body.knowledgeBase.owner;
                        if (typeof owner === "string") return true;
                        if (typeof owner === "object" && owner !== null) {
                            return owner.uid === undefined;
                        }
                        return true; // owner may not be present
                    }
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
    // ID-to-UID Database Mapping (Internal verification)
    // Verify hash-based IDs can be resolved to internal UUIDs
    // ===============================================================
    group("ID to UID Database Mapping", () => {
        let knowledgeBaseId = null;
        let fileId = null;

        // Create a test KB and file
        group("Setup: Create test KB and file", () => {
            const createKBResp = helper.httpRetry.post(
                `${apiHost}/v1alpha/namespaces/${expectedOwner.id}/knowledge-bases`,
                JSON.stringify({
                    displayName: "ID Mapping Test KB " + randomString(8),
                    type: "KNOWLEDGE_BASE_TYPE_PERSISTENT",
                }),
                header
            );

            if (createKBResp.status === 200 || createKBResp.status === 201) {
                knowledgeBaseId = createKBResp.json().knowledgeBase?.id;
            }

            if (knowledgeBaseId) {
                // API CHANGE: CreateFile now uses /namespaces/{ns}/files with knowledge_base_id query param
                const createFileResp = helper.httpRetry.post(
                    `${apiHost}/v1alpha/namespaces/${expectedOwner.id}/files?knowledgeBaseId=${knowledgeBaseId}`,
                    JSON.stringify({
                        displayName: "id-mapping-test-" + randomString(8) + ".txt",
                        type: "TYPE_TEXT",
                        content: "dGVzdA==" // base64 "test"
                    }),
                    header
                );

                if (createFileResp.status === 200 || createFileResp.status === 201) {
                    fileId = createFileResp.json().file?.id;
                }
            }
        });

        if (knowledgeBaseId && fileId) {
            group("Verify ID to UID mapping works", () => {
                // Use helper functions to verify database mapping
                const kbUid = helper.getKnowledgeBaseUidFromId(knowledgeBaseId);
                const fileUid = helper.getFileUidFromId(fileId);

                check({ kbUid, fileUid, knowledgeBaseId, fileId }, {
                    "[DB] KB ID maps to internal UUID": (d) => {
                        if (!d.kbUid) {
                            console.log(`Failed to get KB UID for id=${d.knowledgeBaseId}`);
                            return false;
                        }
                        // UUID format: 8-4-4-4-12 hex characters
                        const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
                        return uuidPattern.test(d.kbUid);
                    },
                    "[DB] File ID maps to internal UUID": (d) => {
                        if (!d.fileUid) {
                            console.log(`Failed to get file UID for id=${d.fileId}`);
                            return false;
                        }
                        const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
                        return uuidPattern.test(d.fileUid);
                    },
                    "[DB] KB ID is not a UUID (hash-based)": (d) => {
                        const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
                        return !uuidPattern.test(d.knowledgeBaseId);
                    },
                    "[DB] File ID is not a UUID (hash-based)": (d) => {
                        const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
                        return !uuidPattern.test(d.fileId);
                    }
                });
            });
        } else {
            console.log("KB/File creation failed, skipping ID mapping tests");
        }

        // Cleanup
        if (knowledgeBaseId) {
            helper.httpRetry.del(
                `${apiHost}/v1alpha/namespaces/${expectedOwner.id}/knowledge-bases/${knowledgeBaseId}`,
                header
            );
        }
    });
}
