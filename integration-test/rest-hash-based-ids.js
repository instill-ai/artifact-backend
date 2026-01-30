import { check, group } from "k6";
import { randomString } from "https://jslib.k6.io/k6-utils/1.1.0/index.js";
import encoding from 'k6/encoding';

import * as constant from "./const.js";
import * as helper from "./helper.js";

// Use httpRetry for automatic retry on transient errors (429, 5xx)
const http = helper.httpRetry;

const dbIDPrefix = constant.generateDBIDPrefix();

/**
 * TEST SUITE: AIP-Compliant Resource IDs for Artifact Backend
 *
 * PURPOSE:
 * Tests the AIP-compliant resource ID system for Knowledge Bases and Files.
 * IDs are now generated as {prefix}-{base62(sha256(uid)[:10])} format.
 *
 * KEY CHANGES FROM PREVIOUS IMPLEMENTATION:
 * - IDs are now IMMUTABLE (OUTPUT_ONLY in protobuf)
 * - ID format: `kb-{base62_hash}` or `file-{base62_hash}` (80+ bits entropy)
 * - Slug is a separate field that changes with display_name
 * - Aliases store previous slugs (not IDs) for backward compatibility
 * - Files are now top-level resources: /namespaces/{ns}/files/{file}
 * - Files have many-to-many relationship with knowledge bases via knowledge_base_ids
 *
 * WHAT THIS TEST SUITE VALIDATES:
 * - Knowledge Base IDs have `kb-` prefix
 * - File IDs have `file-` prefix
 * - IDs are IMMUTABLE (don't change on display_name update)
 * - Slug changes when display_name changes
 * - Old slugs are stored in aliases for backward compatibility
 * - Files can be associated with knowledge bases
 *
 * API ENDPOINTS TESTED:
 * - POST   /v1alpha/namespaces/{ns}/knowledge-bases
 * - GET    /v1alpha/namespaces/{ns}/knowledge-bases/{kb_id}
 * - PATCH  /v1alpha/namespaces/{ns}/knowledge-bases/{kb_id}
 * - POST   /v1alpha/namespaces/{ns}/files (with knowledge_base_id)
 * - GET    /v1alpha/namespaces/{ns}/files/{file_id}
 * - PATCH  /v1alpha/namespaces/{ns}/files/{file_id}
 */

// CI mode: run tests sequentially without scenarios (less resource intensive)
// Non-CI mode: run tests in parallel using scenarios
const isCI = __ENV.CI === 'true';

export let options = {
  setupTimeout: '10s',
  insecureSkipTLSVerify: true,
  thresholds: {
    checks: ["rate == 1.0"],
  },
  // Only use scenarios in non-CI mode for parallel execution
  ...(isCI ? { vus: 1, iterations: 1 } : {
    scenarios: {
      test_kb_hash_id: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_KB_PrefixedID', startTime: '0s' },
      test_file_hash_id: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_File_PrefixedID', startTime: '5s' },
      test_id_immutability: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_ID_Immutability', startTime: '10s' },
    },
  }),
};

// Default function for CI mode - runs all tests sequentially
export default function (data) {
  if (!isCI) return; // In non-CI mode, scenarios handle execution

  TEST_KB_PrefixedID(data);
  TEST_File_PrefixedID(data);
  TEST_ID_Immutability(data);
}

export function setup() {
  console.log(`rest-hash-based-ids.js: Using unique test prefix: ${dbIDPrefix}`);

  // Authenticate with retry to handle transient failures
  const authHeader = helper.getBasicAuthHeader(constant.defaultUsername, constant.defaultPassword);
  var header = {
    "headers": {
      "Authorization": authHeader,
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
 * Test Knowledge Base prefixed ID format
 * ID format: kb-{base62_hash} where base62_hash is alphanumeric (0-9, a-z, A-Z)
 */
export function TEST_KB_PrefixedID(data) {
  const header = data.header;
  const randomSuffix = randomString(10);
  const namespaceId = "admin"; // namespace ID only, not the full resource name

  let kbID;
  let kbSlug;

  // Test 1: Create Knowledge Base and verify prefixed ID format
  group("POST knowledge-base - verify prefixed ID format", () => {
    const createPayload = {
      displayName: `Hash Test KB ${randomSuffix}`,
      description: "Testing prefixed ID generation"
    };

    const resp = http.post(
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${namespaceId}/knowledge-bases`,
      JSON.stringify(createPayload),
      header
    );

    check(resp, {
      "[KB PrefixedID] Create - returns status 200/201": (r) => r.status === 200 || r.status === 201,
      "[KB PrefixedID] Create - has id with kb- prefix": (r) => {
        const body = JSON.parse(r.body);
        if (!body.knowledgeBase || !body.knowledgeBase.id) return false;
        kbID = body.knowledgeBase.id;
        // ID should start with "kb-" prefix
        return kbID.startsWith("kb-");
      },
      "[KB PrefixedID] Create - id has base62 hash after prefix": (r) => {
        const body = JSON.parse(r.body);
        if (!body.knowledgeBase || !body.knowledgeBase.id) return false;
        const id = body.knowledgeBase.id;
        // After "kb-", should be base62 characters (0-9, a-z, A-Z)
        const hashPart = id.substring(3); // Remove "kb-" prefix
        return /^[a-zA-Z0-9]+$/.test(hashPart) && hashPart.length >= 8;
      },
      "[KB PrefixedID] Create - has separate slug field": (r) => {
        const body = JSON.parse(r.body);
        if (!body.knowledgeBase) return false;
        kbSlug = body.knowledgeBase.slug;
        // Slug should be derived from displayName (slugified)
        return kbSlug && kbSlug.includes("hash-test-kb");
      },
      "[KB PrefixedID] Create - has displayName field": (r) => {
        const body = JSON.parse(r.body);
        return body.knowledgeBase && body.knowledgeBase.displayName === createPayload.displayName;
      }
    });
  });

  if (!kbID) {
    console.error("Failed to create KB, skipping remaining tests");
    return;
  }

  // Test 2: Get KB by ID
  group("GET knowledge-base by prefixed ID", () => {
    const resp = http.get(
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${namespaceId}/knowledge-bases/${kbID}`,
      header
    );

    check(resp, {
      "[KB PrefixedID] Get by ID - returns status 200": (r) => r.status === 200,
      "[KB PrefixedID] Get by ID - id matches": (r) => {
        const body = JSON.parse(r.body);
        return body.knowledgeBase && body.knowledgeBase.id === kbID;
      },
      "[KB PrefixedID] Get by ID - name format is correct": (r) => {
        const body = JSON.parse(r.body);
        // name should be: namespaces/{ns}/knowledge-bases/{id}
        return body.knowledgeBase &&
          body.knowledgeBase.name === `namespaces/${namespaceId}/knowledge-bases/${kbID}`;
      }
    });
  });

  // Cleanup
  group("DELETE knowledge-base - cleanup", () => {
    http.del(
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${namespaceId}/knowledge-bases/${kbID}`,
      null,
      header
    );
  });
}

/**
 * Test File prefixed ID format
 * ID format: file-{base62_hash}
 * Files are now top-level resources with many-to-many KB relationship
 */
export function TEST_File_PrefixedID(data) {
  const header = data.header;
  const randomSuffix = randomString(10);
  const namespaceId = "admin";

  let kbID;
  let fileID;
  let fileSlug;

  // Setup: Create a KB first (still needed for file association)
  group("Setup - Create KB for file tests", () => {
    const createPayload = {
      displayName: `File Hash Test KB ${randomSuffix}`,
      description: "KB for file prefixed ID tests"
    };

    const resp = http.post(
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${namespaceId}/knowledge-bases`,
      JSON.stringify(createPayload),
      header
    );

    if (resp.status === 200 || resp.status === 201) {
      try {
        const body = JSON.parse(resp.body);
        kbID = body.knowledgeBase.id;
        console.log(`[File PrefixedID] Created KB with id=${kbID}`);
      } catch (e) {
        console.error(`[File PrefixedID] Failed to parse KB response: ${e}, body: ${resp.body}`);
      }
    } else {
      console.error(`[File PrefixedID] KB creation failed with status ${resp.status}: ${resp.body}`);
    }
  });

  if (!kbID) {
    console.error("[File PrefixedID] Failed to create KB for file tests - kbID is null");
    return;
  }

  // Test 1: Create File and verify prefixed ID format
  // Note: CreateFile now uses /namespaces/{ns}/files with knowledge_base_id in body
  group("[File] Create - verify prefixed ID format", () => {
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

    // Debug: log response status and body for troubleshooting
    if (resp.status !== 200 && resp.status !== 201) {
      console.error(`[File PrefixedID] Create failed with status ${resp.status}: ${resp.body}`);
    }

    check(resp, {
      "[File PrefixedID] Create - returns status 200/201": (r) => r.status === 200 || r.status === 201,
      "[File PrefixedID] Create - has file- prefixed id": (r) => {
        if (r.status !== 200 && r.status !== 201) return false;
        try {
          const body = JSON.parse(r.body);
          if (!body.file || !body.file.id) return false;
          fileID = body.file.id;
          return fileID.startsWith("file-");
        } catch (e) {
          console.error(`[File PrefixedID] JSON parse error: ${e}, body: ${r.body}`);
          return false;
        }
      },
      "[File PrefixedID] Create - id has base62 hash after prefix": (r) => {
        if (r.status !== 200 && r.status !== 201) return false;
        try {
          const body = JSON.parse(r.body);
          if (!body.file || !body.file.id) return false;
          const id = body.file.id;
          // After "file-", should be base62 characters
          const hashPart = id.substring(5); // Remove "file-" prefix
          return /^[a-zA-Z0-9]+$/.test(hashPart) && hashPart.length >= 8;
        } catch (e) {
          return false;
        }
      },
      "[File PrefixedID] Create - has separate slug field": (r) => {
        if (r.status !== 200 && r.status !== 201) return false;
        try {
          const body = JSON.parse(r.body);
          if (!body.file) return false;
          fileSlug = body.file.slug;
          return fileSlug && fileSlug.includes("hash-test-file");
        } catch (e) {
          return false;
        }
      },
      "[File PrefixedID] Create - has knowledge_base_ids field": (r) => {
        if (r.status !== 200 && r.status !== 201) return false;
        try {
          const body = JSON.parse(r.body);
          if (!body.file) return false;
          // File should show which KBs it's associated with
          const kbName = `namespaces/${namespaceId}/knowledge-bases/${kbID}`;
          return Array.isArray(body.file.knowledgeBases) &&
            body.file.knowledgeBases.includes(kbName);
        } catch (e) {
          return false;
        }
      }
    });
  });

  if (!fileID) {
    // Cleanup KB and exit
    http.del(
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${namespaceId}/knowledge-bases/${kbID}`,
      null,
      header
    );
    return;
  }

  // Test 2: Get File by ID (now at top-level /files endpoint)
  group("[File] Get by ID - verify retrieval", () => {
    const resp = http.get(
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${namespaceId}/knowledge-bases/${kbID}/files/${fileID}`,
      header
    );

    if (resp.status !== 200) {
      console.error(`[File PrefixedID] Get by ID failed with status ${resp.status}: ${resp.body}`);
    }

    check(resp, {
      "[File PrefixedID] Get by ID - returns status 200": (r) => r.status === 200,
      "[File PrefixedID] Get by ID - id matches": (r) => {
        if (r.status !== 200) return false;
        try {
          const body = JSON.parse(r.body);
          return body.file && body.file.id === fileID;
        } catch (e) {
          console.error(`[File PrefixedID] Get JSON parse error: ${e}`);
          return false;
        }
      },
      "[File PrefixedID] Get by ID - name format is correct": (r) => {
        if (r.status !== 200) return false;
        try {
          const body = JSON.parse(r.body);
          // name should be: namespaces/{ns}/files/{id}
          return body.file &&
            body.file.name === `namespaces/${namespaceId}/knowledge-bases/${kbID}/files/${fileID}`;
        } catch (e) {
          return false;
        }
      }
    });
  });

  // Cleanup - delete KB (which should also clean up associated files)
  group("Cleanup - delete KB", () => {
    http.del(
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${namespaceId}/knowledge-bases/${kbID}`,
      null,
      header
    );
  });
}

/**
 * Test ID Immutability
 * Critical invariant: IDs do NOT change when display_name is updated
 * Only slug changes, and old slugs are stored in aliases
 */
export function TEST_ID_Immutability(data) {
  const header = data.header;
  const randomSuffix = randomString(10);
  const namespaceId = "admin";

  let kbID;
  let originalSlug;

  // Create KB with initial display_name
  group("[Immutability] Setup - Create KB", () => {
    const createPayload = {
      displayName: `Original Name ${randomSuffix}`,
      description: "Testing ID immutability"
    };

    const resp = http.post(
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${namespaceId}/knowledge-bases`,
      JSON.stringify(createPayload),
      header
    );

    if (resp.status === 200 || resp.status === 201) {
      const body = JSON.parse(resp.body);
      kbID = body.knowledgeBase.id;
      originalSlug = body.knowledgeBase.slug;
      console.log(`Created KB with id=${kbID}, slug=${originalSlug}`);
    }
  });

  if (!kbID) {
    console.error("Failed to create KB for immutability test");
    return;
  }

  // Update display_name and verify ID remains the same
  let newSlug;
  group("[Immutability] Update display_name - verify ID unchanged", () => {
    const updatePayload = {
      displayName: `Renamed Name ${randomSuffix}`,
    };

    const resp = http.patch(
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${namespaceId}/knowledge-bases/${kbID}?updateMask=display_name`,
      JSON.stringify(updatePayload),
      header
    );

    if (resp.status !== 200) {
      console.error(`[Immutability] Update failed with status ${resp.status}: ${resp.body}`);
    }

    check(resp, {
      "[Immutability] Update - returns status 200": (r) => r.status === 200,
      "[Immutability] Update - ID is UNCHANGED": (r) => {
        if (r.status !== 200) return false;
        try {
          const body = JSON.parse(r.body);
          if (!body.knowledgeBase || !body.knowledgeBase.id) return false;
          const newID = body.knowledgeBase.id;
          console.log(`After update: id=${newID} (original=${kbID})`);
          // CRITICAL: ID must not change
          return newID === kbID;
        } catch (e) {
          console.error(`[Immutability] Update JSON parse error: ${e}`);
          return false;
        }
      },
      "[Immutability] Update - slug is CHANGED": (r) => {
        if (r.status !== 200) return false;
        try {
          const body = JSON.parse(r.body);
          if (!body.knowledgeBase) return false;
          newSlug = body.knowledgeBase.slug;
          console.log(`After update: slug=${newSlug} (original=${originalSlug})`);
          // Slug should change with display_name
          return newSlug !== originalSlug && newSlug.includes("renamed-name");
        } catch (e) {
          return false;
        }
      },
      "[Immutability] Update - old slug in aliases": (r) => {
        if (r.status !== 200) return false;
        try {
          const body = JSON.parse(r.body);
          if (!body.knowledgeBase || !body.knowledgeBase.aliases) return false;
          // Old slug should be preserved in aliases for backward compatibility
          console.log(`Aliases: ${JSON.stringify(body.knowledgeBase.aliases)}`);
          return body.knowledgeBase.aliases.includes(originalSlug);
        } catch (e) {
          return false;
        }
      }
    });
  });

  // Verify we can still fetch by ID (not alias)
  group("[Immutability] Get by ID after rename", () => {
    const resp = http.get(
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${namespaceId}/knowledge-bases/${kbID}`,
      header
    );

    if (resp.status !== 200) {
      console.error(`[Immutability] Get by ID failed with status ${resp.status}: ${resp.body}`);
    }

    check(resp, {
      "[Immutability] Get by ID - returns status 200": (r) => r.status === 200,
      "[Immutability] Get by ID - returns KB with new slug": (r) => {
        if (r.status !== 200) return false;
        try {
          const body = JSON.parse(r.body);
          return body.knowledgeBase &&
            body.knowledgeBase.id === kbID &&
            body.knowledgeBase.slug === newSlug;
        } catch (e) {
          return false;
        }
      }
    });
  });

  // Cleanup
  group("[Immutability] Cleanup - delete KB", () => {
    http.del(
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${namespaceId}/knowledge-bases/${kbID}`,
      null,
      header
    );
  });
}

export function teardown(data) {
  // Cleanup handled by individual tests
}
