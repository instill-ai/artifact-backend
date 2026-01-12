import http from "k6/http";
import { check, group } from "k6";
import { randomString } from "https://jslib.k6.io/k6-utils/1.1.0/index.js";
import encoding from 'k6/encoding';

import * as constant from "./const.js";

const dbIDPrefix = constant.generateDBIDPrefix();

/**
 * TEST SUITE: Hash-Based Resource IDs for Artifact Backend
 *
 * PURPOSE:
 * Tests the hash-based resource ID system for Knowledge Bases and Files.
 * IDs are now generated as {slug}-{sha256(uid)[:8]} format.
 *
 * WHAT THIS TEST SUITE VALIDATES:
 * - Knowledge Base IDs have hash suffix
 * - File IDs have hash suffix
 * - ID regeneration on display_name/name change
 * - Old IDs stored in aliases for backward compatibility
 * - Lookup by alias works
 *
 * API ENDPOINTS TESTED:
 * - POST   /v1alpha/namespaces/{ns}/knowledge-bases
 * - GET    /v1alpha/namespaces/{ns}/knowledge-bases/{kb_id}
 * - PATCH  /v1alpha/namespaces/{ns}/knowledge-bases/{kb_id}
 * - POST   /v1alpha/namespaces/{ns}/knowledge-bases/{kb_id}/files
 * - GET    /v1alpha/namespaces/{ns}/knowledge-bases/{kb_id}/files/{file_id}
 */

export let options = {
  setupTimeout: '10s',
  insecureSkipTLSVerify: true,
  thresholds: {
    checks: ["rate == 1.0"],
  },
  scenarios: {
    test_kb_hash_id: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_KB_HashBasedID', startTime: '0s' },
    test_file_hash_id: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_File_HashBasedID', startTime: '5s' },
  },
};

export function setup() {
  console.log(`rest-hash-based-ids.js: Using unique test prefix: ${dbIDPrefix}`);

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

  return {
    header: header,
    dbIDPrefix: dbIDPrefix
  }
}

/**
 * Test Knowledge Base hash-based ID
 */
export function TEST_KB_HashBasedID(data) {
  const header = data.header;
  const randomSuffix = randomString(10);
  const namespaceId = "admin"; // namespace ID only, not the full resource name

  let kbUID;
  let kbID;

  // Test 1: Create Knowledge Base and verify hash-based ID
  group("POST knowledge-base - verify hash-based ID", () => {
    const createPayload = {
      knowledgeBase: {
        displayName: `Hash Test KB ${randomSuffix}`,
        description: "Testing hash-based ID generation"
      }
    };

    const resp = http.post(
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${namespaceId}/knowledge-bases`,
      JSON.stringify(createPayload),
      header
    );

    check(resp, {
      "[KB HashID] Create - returns status 200/201": (r) => r.status === 200 || r.status === 201,
      "[KB HashID] Create - has uid": (r) => {
        const body = JSON.parse(r.body);
        return body.knowledgeBase && body.knowledgeBase.uid;
      },
      "[KB HashID] Create - has id with hash suffix": (r) => {
        const body = JSON.parse(r.body);
        if (!body.knowledgeBase || !body.knowledgeBase.id) return false;
        kbUID = body.knowledgeBase.uid;
        kbID = body.knowledgeBase.id;
        // ID should be slug-hash format (e.g., "hash-test-kb-abc12345")
        // Hash suffix is 8 characters
        const id = body.knowledgeBase.id;
        const parts = id.split('-');
        const lastPart = parts[parts.length - 1];
        // Hash should be 8 hex characters
        return lastPart.length === 8 && /^[a-f0-9]+$/.test(lastPart);
      },
      "[KB HashID] Create - id starts with slug from displayName": (r) => {
        const body = JSON.parse(r.body);
        if (!body.knowledgeBase || !body.knowledgeBase.id) return false;
        // "Hash Test KB xxx" should produce "hash-test-kb-xxx-<hash>"
        return body.knowledgeBase.id.startsWith("hash-test-kb");
      }
    });
  });

  if (!kbUID) {
    console.error("Failed to create KB, skipping remaining tests");
    return;
  }

  // Test 2: Get KB by ID
  group("GET knowledge-base by hash ID", () => {
    const resp = http.get(
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${namespaceId}/knowledge-bases/${kbID}`,
      header
    );

    check(resp, {
      "[KB HashID] Get by ID - returns status 200": (r) => r.status === 200,
      "[KB HashID] Get by ID - uid matches": (r) => {
        const body = JSON.parse(r.body);
        return body.knowledgeBase && body.knowledgeBase.uid === kbUID;
      }
    });
  });

  // Test 3: Update KB displayName and verify ID regeneration
  let newKBID;
  group("PATCH knowledge-base - verify ID regeneration and alias creation", () => {
    // PATCH request body: flat fields (not nested in knowledgeBase) with update_mask
    const updatePayload = {
      displayName: `Renamed KB ${randomSuffix}`,
    };

    const resp = http.patch(
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${namespaceId}/knowledge-bases/${kbID}?update_mask=display_name`,
      JSON.stringify(updatePayload),
      header
    );

    check(resp, {
      "[KB HashID] Update - returns status 200": (r) => r.status === 200,
      "[KB HashID] Update - id changed": (r) => {
        const body = JSON.parse(r.body);
        if (!body.knowledgeBase || !body.knowledgeBase.id) return false;
        newKBID = body.knowledgeBase.id;
        return newKBID !== kbID;
      },
      "[KB HashID] Update - new id has correct slug": (r) => {
        const body = JSON.parse(r.body);
        if (!body.knowledgeBase || !body.knowledgeBase.id) return false;
        return body.knowledgeBase.id.startsWith("renamed-kb");
      },
      "[KB HashID] Update - old id in aliases": (r) => {
        const body = JSON.parse(r.body);
        if (!body.knowledgeBase || !body.knowledgeBase.aliases) return false;
        return body.knowledgeBase.aliases.includes(kbID);
      }
    });
  });

  // Test 4: Verify lookup by alias (old ID) still works
  if (newKBID) {
    group("GET knowledge-base by old ID (alias) - backward compatibility", () => {
      const resp = http.get(
        `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${namespaceId}/knowledge-bases/${kbID}`,
        header
      );

      check(resp, {
        "[KB HashID] Get by alias - returns status 200": (r) => r.status === 200,
        "[KB HashID] Get by alias - returns correct KB": (r) => {
          const body = JSON.parse(r.body);
          return body.knowledgeBase && body.knowledgeBase.uid === kbUID;
        },
        "[KB HashID] Get by alias - current id is new id": (r) => {
          const body = JSON.parse(r.body);
          return body.knowledgeBase && body.knowledgeBase.id === newKBID;
        }
      });
    });
  }

  // Cleanup
  group("DELETE knowledge-base - cleanup", () => {
    http.del(
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${namespaceId}/knowledge-bases/${kbUID}`,
      null,
      header
    );
  });
}

/**
 * Test File hash-based ID
 * Covers: Hash ID generation, ID regeneration on rename, Alias lookup
 */
export function TEST_File_HashBasedID(data) {
  const header = data.header;
  const randomSuffix = randomString(10);
  const namespaceId = "admin"; // namespace ID only, not the full resource name

  let kbUID;
  let kbID;

  // Setup: Create a KB first
  group("Setup - Create KB for file tests", () => {
    const createPayload = {
      knowledgeBase: {
        displayName: `File Hash Test KB ${randomSuffix}`,
        description: "KB for file hash-based ID tests"
      }
    };

    const resp = http.post(
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${namespaceId}/knowledge-bases`,
      JSON.stringify(createPayload),
      header
    );

    if (resp.status === 200 || resp.status === 201) {
      const body = JSON.parse(resp.body);
      kbUID = body.knowledgeBase.uid;
      kbID = body.knowledgeBase.id;
    }
  });

  if (!kbUID) {
    console.error("Failed to create KB for file tests");
    return;
  }

  let fileUID;
  let fileID;

  // Test 1: Create File and verify hash-based ID
  group("[File] Create - verify hash-based ID", () => {
    const fileContent = `Test file content ${randomSuffix}`;
    const base64Content = encoding.b64encode(fileContent);

    const createPayload = {
      displayName: `hash-test-file-${randomSuffix}.txt`,
      type: "TYPE_TEXT",
      content: base64Content
    };

    const resp = http.post(
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${namespaceId}/knowledge-bases/${kbID}/files`,
      JSON.stringify(createPayload),
      header
    );

    check(resp, {
      "[File] Create - returns status 200/201": (r) => r.status === 200 || r.status === 201,
      "[File] Create - has uid": (r) => {
        const body = JSON.parse(r.body);
        return body.file && body.file.uid;
      },
      "[File] Create - has hash-based id": (r) => {
        const body = JSON.parse(r.body);
        if (!body.file || !body.file.id) return false;
        fileUID = body.file.uid;
        fileID = body.file.id;
        const parts = body.file.id.split('-');
        const lastPart = parts[parts.length - 1];
        return lastPart.length === 8 && /^[a-f0-9]+$/.test(lastPart);
      }
    });
  });

  if (!fileUID) {
    // Cleanup KB and exit
    http.del(
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${namespaceId}/knowledge-bases/${kbUID}`,
      null,
      header
    );
    return;
  }

  // Test 2: Get File by ID
  group("[File] Get by ID - verify retrieval", () => {
    const resp = http.get(
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${namespaceId}/knowledge-bases/${kbID}/files/${fileID}`,
      header
    );

    check(resp, {
      "[File] Get by ID - returns status 200": (r) => r.status === 200,
      "[File] Get by ID - uid matches": (r) => {
        const body = JSON.parse(r.body);
        return body.file && body.file.uid === fileUID;
      }
    });
  });

  // Test 3: Update File displayName and verify ID regeneration + alias
  let newFileID;
  group("[File] Update - verify ID regeneration and alias creation", () => {
    // PATCH request body: flat fields with update_mask
    const updatePayload = {
      displayName: `renamed-file-${randomSuffix}.txt`
    };

    const resp = http.patch(
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${namespaceId}/knowledge-bases/${kbID}/files/${fileUID}?update_mask=display_name`,
      JSON.stringify(updatePayload),
      header
    );

    check(resp, {
      "[File] Update - returns status 200": (r) => r.status === 200,
      "[File] Update - id changed": (r) => {
        const body = JSON.parse(r.body);
        if (!body.file || !body.file.id) return false;
        newFileID = body.file.id;
        return newFileID !== fileID;
      },
      "[File] Update - new id starts with new slug": (r) => {
        const body = JSON.parse(r.body);
        if (!body.file || !body.file.id) return false;
        return body.file.id.startsWith("renamed-file");
      },
      "[File] Update - old id in aliases": (r) => {
        const body = JSON.parse(r.body);
        if (!body.file || !body.file.aliases) return false;
        return body.file.aliases.includes(fileID);
      }
    });
  });

  // Test 4: Lookup by alias (old ID)
  if (newFileID) {
    group("[File] Get by alias - backward compatibility", () => {
      const resp = http.get(
        `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${namespaceId}/knowledge-bases/${kbID}/files/${fileID}`,
        header
      );

      check(resp, {
        "[File] Get by alias - returns status 200": (r) => r.status === 200,
        "[File] Get by alias - returns correct file": (r) => {
          const body = JSON.parse(r.body);
          return body.file && body.file.uid === fileUID;
        },
        "[File] Get by alias - current id is new id": (r) => {
          const body = JSON.parse(r.body);
          return body.file && body.file.id === newFileID;
        }
      });
    });
  }

  // Cleanup
  group("Cleanup - delete file and KB", () => {
    http.del(
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${namespaceId}/knowledge-bases/${kbID}/files/${fileUID}`,
      null,
      header
    );
    http.del(
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${namespaceId}/knowledge-bases/${kbUID}`,
      null,
      header
    );
  });
}

export function teardown(data) {
  // Cleanup handled by individual tests
}
