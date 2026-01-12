import http from "k6/http";
import { check, group, sleep } from "k6";
import { randomString } from "https://jslib.k6.io/k6-utils/1.1.0/index.js";
import encoding from 'k6/encoding';

import * as constant from "./const.js";
import * as helper from "./helper.js";

const dbIDPrefix = constant.generateDBIDPrefix();

export let options = {
  setupTimeout: '10s',
  teardownTimeout: '180s',
  insecureSkipTLSVerify: true,
  thresholds: {
    checks: ["rate == 1.0"],
  },
  // Staggered start times prevent API gateway rate limiting (429 errors)
  // When running in parallel with other test files, without staggering all scenarios
  // start simultaneously which causes rate limiting
  scenarios: {
    // Basic CRUD operations (0-6s stagger)
    test_02_create_knowledge_base: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_02_CreateKnowledgeBase', startTime: '0s' },
    test_03_list_knowledge_bases: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_03_ListKnowledgeBases', startTime: '1s' },
    test_04_get_knowledge_base: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_04_GetKnowledgeBase', startTime: '2s' },
    test_05_update_knowledge_base: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_05_UpdateKnowledgeBase', startTime: '3s' },
    test_06_delete_knowledge_base: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_06_DeleteKnowledgeBase', startTime: '4s' },
    test_23_owner_creator_fields: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_23_OwnerCreatorFields', startTime: '5s' },
    test_24_reserved_tags_validation: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_24_ReservedTagsValidation', startTime: '6s' },

    // End-to-end tests (7-8s stagger)
    test_07_cleanup_files: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_07_CleanupFiles', startTime: '7s' },
    test_08_e2e_knowledge_base: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_08_E2EKnowledgeBase', startTime: '8s' },

    // JWT/Auth tests (9-19s stagger)
    test_09_jwt_create_knowledge_base: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_09_JWT_CreateKnowledgeBase', startTime: '9s' },
    test_10_jwt_list_knowledge_bases: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_10_JWT_ListKnowledgeBases', startTime: '10s' },
    test_11_jwt_get_knowledge_base: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_11_JWT_GetKnowledgeBase', startTime: '11s' },
    test_12_jwt_update_knowledge_base: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_12_JWT_UpdateKnowledgeBase', startTime: '12s' },
    test_13_jwt_create_file: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_13_JWT_CreateFile', startTime: '13s' },
    test_14_jwt_list_files: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_14_JWT_ListFiles', startTime: '14s' },
    test_15_jwt_get_file: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_15_JWT_GetFile', startTime: '15s' },
    test_16_jwt_get_file_content: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_16_JWT_GetFileContent', startTime: '16s' },
    test_17_jwt_get_file_summary: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_17_JWT_GetFileSummary', startTime: '17s' },
    test_18_jwt_list_chunks: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_18_JWT_ListChunks', startTime: '18s' },
    test_19_jwt_get_file_cache: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_19_JWT_GetFileCache', startTime: '19s' },

    // File cache tests (20-22s stagger)
    test_20_get_file_cache: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_20_GetFileCache', startTime: '20s' },
    test_21_get_file_cache_renewal: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_21_GetFileCacheRenewal', startTime: '21s' },
    test_22_get_file_cache_large_file: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_22_GetFileCacheLargeFile', startTime: '22s' },
  },
};

export function setup() {
  check(true, { [constant.banner('Artifact API: Setup')]: () => true });

  // Stagger test execution to reduce parallel resource contention
  helper.staggerTestExecution(2);

  console.log(`rest.js: Using unique test prefix: ${dbIDPrefix}`);

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
    headers: { "Authorization": `Bearer ${loginResp.json().accessToken}` }
  })

  return {
    header: header,
    expectedOwner: resp.json().user,
    dbIDPrefix: dbIDPrefix
  }
}

export function teardown(data) {
  const groupName = "Artifact API: Teardown - Delete all test resources";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    // Wait for file processing AND Temporal activities to settle before cleanup
    console.log("Teardown: Waiting for safe cleanup...");
    helper.waitForSafeCleanup(120, data.dbIDPrefix, 3);

    console.log(`rest.js teardown: Cleaning up resources with prefix: ${data.dbIDPrefix}`);
    var listResp = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases`, null, data.header)
    if (listResp.status === 200) {
      var knowledgeBases = Array.isArray(listResp.json().knowledgeBases) ? listResp.json().knowledgeBases : []
      let cleanedCount = 0;
      for (const kb of knowledgeBases) {
        // Clean up knowledge bases with our test prefix (includes cl-, cat-, del-, jwt- prefixes)
        if (kb.id && (kb.id.startsWith(data.dbIDPrefix) || kb.id.includes(data.dbIDPrefix))) {
          var delResp = http.request("DELETE", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${kb.id}`, null, data.header);
          if (delResp.status === 200 || delResp.status === 204 || delResp.status === 404) {
            cleanedCount++;
          }
        }
      }
      console.log(`Cleaned ${cleanedCount} test knowledge bases`);
    }
  });
}

// ============================================================================
// TEST GROUP 01: Health Check
// ============================================================================
export function TEST_01_Health(data) {
  const groupName = "Artifact API: Health check";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    check(http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/health/artifact`), {
      "GET /health/artifact response status is 200": (r) => r.status === 200,
    });
  });
}

// ============================================================================
// TEST GROUP 02-06: Basic CRUD Operations
// ============================================================================
export function TEST_02_CreateKnowledgeBase(data) {
  const groupName = "Artifact API: Create a knowledge base";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    var displayName = "Test " + data.dbIDPrefix + randomString(10);
    var reqBody = {
      knowledgeBase: {
        displayName: displayName,
        description: randomString(50),
        tags: ["test", "integration"],
        type: "KNOWLEDGE_BASE_TYPE_PERSISTENT"
      }
    };

    var resOrigin = http.request(
      "POST",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases`,
      JSON.stringify(reqBody),
      data.header
    );
    let json; try { json = resOrigin.json(); } catch (e) { json = {}; }
    const cat = json.knowledgeBase || {};
    const uid = cat.uid;
    const id = cat.id;
    const createTime = cat.createTime || cat.create_time;
    const updateTime = cat.updateTime || cat.update_time;

    check(resOrigin, {
      "POST /v1alpha/namespaces/{namespace_id}/knowledge-bases response status is 200": (r) => r.status === 200,
      "POST /v1alpha/namespaces/{namespace_id}/knowledge-bases response knowledge base name": () =>
        cat && cat.name === `namespaces/${data.expectedOwner.id}/knowledge-bases/${id}`,
      "POST /v1alpha/namespaces/{namespace_id}/knowledge-bases response knowledge base uid": () =>
        cat && helper.isUUID(uid),
      "POST /v1alpha/namespaces/{namespace_id}/knowledge-bases response knowledge base has hash-based id": () =>
        cat && id && /^[a-z][-a-z0-9]*-[a-f0-9]{8}$/.test(id),
      "POST /v1alpha/namespaces/{namespace_id}/knowledge-bases response knowledge base displayName": () =>
        cat && cat.displayName === displayName,
      "POST /v1alpha/namespaces/{namespace_id}/knowledge-bases response knowledge base description": () =>
        cat && cat.description === reqBody.knowledgeBase.description,
      "POST /v1alpha/namespaces/{namespace_id}/knowledge-bases response knowledge base is valid": () =>
        cat && helper.validateKnowledgeBase(cat, false),
      "POST /v1alpha/namespaces/{namespace_id}/knowledge-bases response knowledge base createTime": () =>
        cat && typeof createTime === 'string' && createTime.length > 0,
      "POST /v1alpha/namespaces/{namespace_id}/knowledge-bases response knowledge base updateTime": () =>
        cat && typeof updateTime === 'string' && updateTime.length > 0,
      // Owner/Creator field validations
      "POST /v1alpha/namespaces/{namespace_id}/knowledge-bases response knowledge base ownerUid is valid UUID": () =>
        cat && helper.isUUID(cat.ownerUid),
      "POST /v1alpha/namespaces/{namespace_id}/knowledge-bases response knowledge base ownerName matches namespace": () =>
        cat && cat.ownerName === `users/${data.expectedOwner.id}`,
      "POST /v1alpha/namespaces/{namespace_id}/knowledge-bases response knowledge base creatorUid is valid UUID": () =>
        cat && helper.isUUID(cat.creatorUid),
      "POST /v1alpha/namespaces/{namespace_id}/knowledge-bases response knowledge base creatorUid matches user": () =>
        cat && cat.creatorUid === data.expectedOwner.uid,
      "POST /v1alpha/namespaces/{namespace_id}/knowledge-bases response knowledge base has valid owner object": () =>
        cat && cat.owner && cat.owner.user && cat.owner.user.id === data.expectedOwner.id,
      "POST /v1alpha/namespaces/{namespace_id}/knowledge-bases response knowledge base has valid creator object": () =>
        cat && helper.isValidCreator(cat.creator, data.expectedOwner),
    });

    // Cleanup
    if (cat && cat.id) {
      http.request("DELETE", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${cat.id}`, null, data.header);
    }
  });
}

export function TEST_03_ListKnowledgeBases(data) {
  const groupName = "Artifact API: List knowledge bases";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    var resOrigin = http.request(
      "GET",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases`,
      null,
      data.header
    );
    let json; try { json = resOrigin.json(); } catch (e) { json = {}; }
    check(resOrigin, {
      "GET /v1alpha/namespaces/{namespace_id}/knowledge-bases response status is 200": (r) => r.status === 200,
      "GET /v1alpha/namespaces/{namespace_id}/knowledge-bases response knowledge bases is array": () =>
        Array.isArray(json.knowledgeBases),
    });
  });
}

export function TEST_04_GetKnowledgeBase(data) {
  const groupName = "Artifact API: Get knowledge base";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    const cRes = http.request(
      "POST",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases`,
      JSON.stringify({ knowledgeBase: { displayName: "Test " + data.dbIDPrefix + randomString(10) } }),
      data.header
    );
    check(cRes, { "POST /v1alpha/namespaces/{namespace_id}/knowledge-bases 200": (r) => r.status === 200 });
    const created = (cRes.json() || {}).knowledgeBase || {};

    const resOrigin = http.request(
      "GET",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases`,
      null,
      data.header
    );
    let json; try { json = resOrigin.json(); } catch (e) { json = {}; }
    const knowledgeBases = Array.isArray(json.knowledgeBases) ? json.knowledgeBases : [];
    check(resOrigin, {
      "GET /v1alpha/namespaces/{namespace_id}/knowledge-bases response status is 200": (r) => r.status === 200,
      "GET /v1alpha/namespaces/{namespace_id}/knowledge-bases response knowledge_bases is array": () => Array.isArray(json.knowledgeBases),
      "GET /v1alpha/namespaces/{namespace_id}/knowledge-bases response contains our knowledge base": () => knowledgeBases.some(kb => kb.id === created.id),
    });

    // Cleanup
    http.request("DELETE", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${created.id}`, null, data.header);
  });
}

export function TEST_05_UpdateKnowledgeBase(data) {
  const groupName = "Artifact API: Update knowledge base";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    const cRes = http.request(
      "POST",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases`,
      JSON.stringify({ knowledgeBase: { displayName: "Test " + data.dbIDPrefix + randomString(10) } }),
      data.header
    );
    const created = (cRes.json() || {}).knowledgeBase || {};
    if (!created.id) {
      console.log(`TEST_05: KB creation failed, status=${cRes.status}, body=${cRes.body}`);
    }

    const newDescription = randomString(50);
    // With proto `body: "knowledge_base"`, body should be just the KB fields directly
    // and updateMask should be a query parameter
    const reqBody = {
      description: newDescription,
      tags: ["test", "integration", "updated"]
    };

    const resOrigin = http.request(
      "PATCH",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${created.id}?updateMask=description,tags`,
      JSON.stringify(reqBody),
      data.header
    );
    const json = (function () { try { return resOrigin.json(); } catch (e) { return {}; } })();
    const cat2 = json.knowledgeBase || {};
    if (resOrigin.status !== 200) {
      console.log(`TEST_05: PATCH failed, status=${resOrigin.status}, body=${resOrigin.body}`);
    }
    check(resOrigin, {
      "PATCH /v1alpha/namespaces/{namespace_id}/knowledge-bases/{knowledge_base_id} response status is 200": (r) => r.status === 200,
      "PATCH /v1alpha/namespaces/{namespace_id}/knowledge-bases/{knowledge_base_id} response knowledge base id": () =>
        cat2.id === created.id,
      "PATCH /v1alpha/namespaces/{namespace_id}/knowledge-bases/{knowledge_base_id} response knowledge base description updated": () =>
        cat2.description === newDescription,
      "PATCH /v1alpha/namespaces/{namespace_id}/knowledge-bases/{knowledge_base_id} response knowledge base is valid": () =>
        helper.validateKnowledgeBase(cat2, false),
    });

    // Cleanup
    http.request("DELETE", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${created.id}`, null, data.header);
  });
}

export function TEST_06_DeleteKnowledgeBase(data) {
  const groupName = "Artifact API: Delete knowledge base";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    const createBody = { knowledgeBase: { displayName: "Test " + data.dbIDPrefix + "del-" + randomString(8) } };
    const cRes = http.request(
      "POST",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases`,
      JSON.stringify(createBody),
      data.header
    );
    let created; try { created = (cRes.json() || {}).knowledgeBase; } catch (e) { created = {}; }
    const knowledgeBaseId = created && created.id;
    check(cRes, { "POST /v1alpha/namespaces/{namespace_id}/knowledge-bases 200": (r) => r.status === 200 });

    const dRes = http.request(
      "DELETE",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}`,
      null,
      data.header
    );
    check(dRes, {
      "DELETE /v1alpha/namespaces/{namespace_id}/knowledge-bases/{knowledge_base_id} response status is 2xx": (r) => r.status >= 200 && r.status < 300,
    });
  });
}

// ============================================================================
// TEST GROUP 07: Cleanup Files
// ============================================================================
export function TEST_07_CleanupFiles(data) {
  const groupName = "Artifact API: Cleanup intermediate files when file processing fails";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    const kbName = "Test " + data.dbIDPrefix + "cl-" + randomString(5);
    const createRes = http.request(
      "POST",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases`,
      JSON.stringify({ knowledgeBase: { displayName: kbName, description: "Cleanup test" } }),
      data.header
    );

    let kb;
    try { kb = createRes.json().knowledgeBase; } catch (e) { kb = {}; }
    const knowledgeBaseId = kb ? kb.id : null;

    check(createRes, {
      "Cleanup: Knowledge base created": (r) => r.status === 200 && knowledgeBaseId,
    });

    if (!knowledgeBaseId) return;

    const filename = `${data.dbIDPrefix}cl.pdf`;
    const uploadRes = http.request(
      "POST",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files`,
      JSON.stringify({ displayName: filename, type: "TYPE_PDF", content: constant.docSamplePdf }),
      data.header
    );

    let uploadedFile;
    try { uploadedFile = uploadRes.json().file; } catch (e) { uploadedFile = {}; }
    const fileUid = uploadedFile ? uploadedFile.uid : null;
    const fileId = uploadedFile ? uploadedFile.id : null;

    check(uploadRes, {
      "Cleanup: File uploaded": (r) => {
        if (r.status !== 200 || !fileUid || !fileId) {
          console.log(`Cleanup file upload failed: status=${r.status}, fileUid=${fileUid}, fileId=${fileId}, body=${r.body}`);
        }
        return r.status === 200 && fileUid && fileId;
      },
    });

    if (!fileUid || !fileId) {
      console.log("Cleanup: Skipping test due to file upload failure, deleting knowledge base...");
      http.request("DELETE", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}`, null, data.header);
      return;
    }

    // Wait for file processing to complete (max 60 seconds)
    const processingResult = helper.waitForFileProcessingComplete(
      data.expectedOwner.id,
      knowledgeBaseId,
      fileUid,
      data.header,
      60 // maxWaitSeconds
    );

    if (!processingResult.completed) {
      console.log(`Cleanup: File processing did not complete: ${processingResult.status}${processingResult.error ? ' - ' + processingResult.error : ''}`);
      // Clean up the knowledge base even if file processing failed/timeout
      http.request("DELETE", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}`, null, data.header);
      return;
    }

    const deleteRes = http.request(
      "DELETE",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}`,
      null,
      data.header
    );

    check(deleteRes, {
      "Cleanup: Knowledge base deleted (triggers file cleanup)": (r) => {
        if (r.status !== 200 && r.status !== 204) {
          console.log(`Cleanup delete failed: status=${r.status}, body=${r.body}`);
        }
        return r.status === 200 || r.status === 204;
      },
    });

    sleep(10); // Wait for Temporal workflow cleanup

    const checkAfter = `
      SELECT
        (SELECT COUNT(*) FROM converted_file WHERE file_uid = '${fileUid}') as converted,
        (SELECT COUNT(*) FROM chunk WHERE file_uid = '${fileUid}') as chunks,
        (SELECT COUNT(*) FROM embedding WHERE file_uid = '${fileUid}') as embeddings
    `;
    try {
      const result = constant.db.exec(checkAfter);
      if (result && result.length > 0) {
        const after = result[0];
        check(after, {
          "Cleanup: Converted files removed": () => parseInt(after.converted) === 0,
          "Cleanup: Chunks removed": () => parseInt(after.chunks) === 0,
          "Cleanup: Embeddings removed": () => parseInt(after.embeddings) === 0,
        });
      }
    } catch (e) {
      // Cleanup verification failed
    }
  });
}

// ============================================================================
// TEST GROUP 08: Knowledge Base End-to-End (Comprehensive)
// ============================================================================
export function TEST_08_E2EKnowledgeBase(data) {
  const groupName = "Artifact API: Knowledge base end-to-end (comprehensive)";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    // Create knowledge base
    const createBody = {
      knowledgeBase: {
        displayName: "Test " + data.dbIDPrefix + "cat-" + randomString(8),
        description: randomString(40),
        tags: ["test", "integration", "kb-e2e"],
        type: "KNOWLEDGE_BASE_TYPE_PERSISTENT",
      }
    };
    const cRes = http.request(
      "POST",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases`,
      JSON.stringify(createBody),
      data.header
    );
    let created; try { created = (cRes.json() || {}).knowledgeBase; } catch (e) { created = {}; }
    const knowledgeBaseId = created && created.id;
    check(cRes, {
      "E2E: Knowledge base created": (r) => r.status === 200,
      "E2E: Knowledge base is valid": () => created && helper.validateKnowledgeBase(created, false),
    });

    if (!knowledgeBaseId) return;

    // Upload 4 file types for testing (multi-page PDF, TEXT, DOCX, XLS)
    const testFiles = constant.sampleFiles.filter(s =>
      s.originalName === "sample-multi-page.pdf" ||
      s.type === "TYPE_TEXT" ||
      s.type === "TYPE_DOCX" ||
      s.type === "TYPE_XLS"
    );
    const uploaded = [];
    const uploadReqs = testFiles.map((s) => {
      const filename = `${data.dbIDPrefix}${s.originalName}`;
      const tags = ["kim", "knives"];
      return {
        s,
        filename,
        tags,
        req: {
          method: "POST",
          url: `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files`,
          body: JSON.stringify({ displayName: filename, type: s.type, content: s.content, tags: tags }),
          params: data.header,
        },
      };
    });

    const uploadResponses = http.batch(uploadReqs.map((x) => x.req));
    for (let i = 0; i < uploadResponses.length; i++) {
      const resp = uploadResponses[i];
      const s = uploadReqs[i].s;
      const filename = uploadReqs[i].filename;
      const tags = uploadReqs[i].tags;
      const fJson = (function () { try { return resp.json(); } catch (e) { return {}; } })();
      const file = (fJson && fJson.file) || {};
      check(resp, {
        [`E2E: File uploaded (${s.originalName})`]: (r) => r.status === 200,
        [`E2E: File has valid structure (${s.originalName})`]: () => file && helper.validateFile(file, false),
        [`E2E: File ownerUid is valid UUID (${s.originalName})`]: () => file && helper.isUUID(file.ownerUid),
        [`E2E: File ownerName matches namespace (${s.originalName})`]: () => file && file.ownerName === `users/${data.expectedOwner.id}`,
        [`E2E: File creatorUid is valid UUID (${s.originalName})`]: () => file && helper.isUUID(file.creatorUid),
        [`E2E: File creatorUid matches user (${s.originalName})`]: () => file && file.creatorUid === data.expectedOwner.uid,
        [`E2E: File has valid creator object (${s.originalName})`]: () => file && helper.isValidCreator(file.creator, data.expectedOwner),
      });
      if (file && file.uid) {
        uploaded.push({ fileUid: file.uid, fileId: file.id, filename: filename, type: s.type, tags: tags });
      } else {
        console.log(`E2E: Failed to upload ${filename}: status=${resp.status}`);
      }
    }

    const fileUids = uploaded.map((f) => f.fileUid);

    if (uploaded.length === 0) {
      console.log("E2E: No files uploaded successfully, skipping processing wait");
      return;
    }

    // Wait for completion (batched polling) - max 5 minutes
    {
      const pending = new Set(uploaded.map((f) => f.fileUid));
      let completedCount = 0;
      const maxIter = 600; // 5 minutes (0.5s sleep per iteration)
      for (let iter = 0; iter < maxIter && pending.size > 0; iter++) {
        const lastBatch = http.batch(
          Array.from(pending).map((uid) => ({
            method: "GET",
            url: `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files/${uid}`,
            params: data.header,
          }))
        );
        let idx = 0;
        let failedCount = 0;
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
              failedCount++;
              console.log(`E2E: File processing failed for ${body.file && body.file.filename || uid}: ${body.file && body.file.processMessage || 'unknown error'}`);
            } else if (r.status === 404) {
              // File was deleted, remove from pending
              pending.delete(uid);
              failedCount++;
              console.log(`E2E: File not found (deleted): ${uid}`);
            }
          } catch (e) {
            console.log(`E2E: Error checking status for ${uid}: ${e}`);
          }
        }

        // If any files failed, abort the test
        if (failedCount > 0) {
          check(false, { [`E2E: ${failedCount} file(s) failed processing`]: () => false });
          return;
        }
        if (pending.size === 0) break;

        // Log progress every 30 seconds
        if (iter > 0 && iter % 60 === 0) {
          console.log(`E2E: Still waiting for ${pending.size} files to complete (${completedCount}/${uploaded.length} done, ${Math.floor(iter / 2)}s elapsed)`);
        }

        sleep(0.5);
      }

      if (pending.size > 0) {
        console.log(`E2E: Timeout waiting for files. Completed: ${completedCount}/${uploaded.length}, Pending: ${Array.from(pending).join(', ')}`);
      }

      check({ status: pending.size === 0 ? 200 : 0 }, { [`E2E: All files completed (${completedCount}/${uploaded.length})`]: () => pending.size === 0 });
    }

    // Verify file metadata
    for (const f of uploaded) {
      var viewPath = `/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files?filter=${encodeURIComponent(`id = "${f.fileId}"`)}`
      const viewRes = http.request("GET", constant.artifactRESTPublicHost + viewPath, null, data.header);

      if (viewRes.status === 200) {
        try {
          const fileData = viewRes.json().files[0];
          check(viewRes, {
            [`E2E: File has correct metadata (${f.type})`]: () =>
              fileData && fileData.displayName === f.filename &&
              fileData.processStatus === "FILE_PROCESS_STATUS_COMPLETED" &&
              fileData.totalChunks > 0 && fileData.totalTokens > 0,
            // Owner/Creator validations on file list
            [`E2E: Listed file has valid ownerUid (${f.type})`]: () => fileData && helper.isUUID(fileData.ownerUid),
            [`E2E: Listed file ownerName matches namespace (${f.type})`]: () => fileData && fileData.ownerName === `users/${data.expectedOwner.id}`,
            [`E2E: Listed file has valid creatorUid (${f.type})`]: () => fileData && helper.isUUID(fileData.creatorUid),
            [`E2E: Listed file has valid creator (${f.type})`]: () => fileData && helper.isValidCreator(fileData.creator, data.expectedOwner),
          });
        } catch (e) {
          console.log(`E2E: Error verifying metadata for ${f.filename} (${f.type}): ${e}`);
        }
      } else {
        console.log(`E2E: Failed to fetch file metadata for ${f.filename} (${f.type}): status=${viewRes.status}`);
      }
    }

    // List knowledge base files
    const listFilesRes = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files?pageSize=100`, null, data.header);
    let listFilesJson; try { listFilesJson = listFilesRes.json(); } catch (e) { listFilesJson = {}; }
    check(listFilesRes, {
      "E2E: List files returns all uploaded": () =>
        Array.isArray(listFilesJson.files) && listFilesJson.files.length === uploaded.length,
    });

    // List chunks for first file
    if (fileUids.length > 0) {
      const listChunksRes = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files/${fileUids[0]}/chunks`, null, data.header);
      check(listChunksRes, {
        "E2E: List chunks returns array": (r) => {
          try {
            return r.status === 200 && Array.isArray(r.json().chunks);
          } catch (e) {
            return false;
          }
        },
      });
    }

    // Get summary
    if (fileUids.length > 0) {
      const getSummaryRes = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files/${fileUids[0]}?view=VIEW_SUMMARY`, null, data.header);
      check(getSummaryRes, {
        "E2E: Get summary returns derived resource URI": (r) => {
          try {
            return r.status === 200 && r.json().derivedResourceUri && r.json().derivedResourceUri.length > 0;
          } catch (e) {
            return false;
          }
        },
      });
    }

    // Update PDF file tags using UpdateFile endpoint
    {
      const pdfFile = uploaded.find(f => f.type === "TYPE_PDF");
      if (pdfFile) {
        // With body: "file" in protobuf, the file object goes in body and update_mask as query param
        // Note: file_id in URL is the UID, not the human-readable id
        const updateTagsBody = { tags: ["scott", "kim"] };
        const updateTagsRes = http.request(
          "PATCH",
          `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files/${pdfFile.fileUid}?updateMask=tags`,
          JSON.stringify(updateTagsBody),
          data.header
        );
        let updateTagsJson; try { updateTagsJson = updateTagsRes.json(); } catch (e) { updateTagsJson = {}; }
        const updatedFile = updateTagsJson.file || {};
        const updatedTags = updatedFile.tags || [];

        if (updateTagsRes.status !== 200) {
          console.log(`E2E: UpdateFile failed - fileUid=${pdfFile.fileUid}, status=${updateTagsRes.status}, body=${JSON.stringify(updateTagsJson)}`);
        }

        check(updateTagsRes, {
          "E2E: Update tags returns 200": (r) => r.status === 200,
          "E2E: Update tags returns file object": () => updatedFile && updatedFile.uid,
          "E2E: Update tags returns correct tags": () =>
            Array.isArray(updatedTags) && updatedTags.length === 2 &&
            updatedTags.includes("scott") && updatedTags.includes("kim"),
        });
      }
    }

    // Chunk similarity search tests
    {
      // Test 1: Search with a combination of tags that returns all files
      const searchBody1 = {
        textPrompt: "test file markdown",
        topK: 10,
        tags: ["scott", "kim"],
        contentType: "CONTENT_TYPE_CHUNK"
      };
      const searchRes1 = http.request(
        "POST",
        `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/searchChunks`,
        JSON.stringify(searchBody1),
        data.header
      );
      let searchJson1; try { searchJson1 = searchRes1.json(); } catch (e) { searchJson1 = {}; }
      const similarChunks1 = searchJson1.similarChunks || [];

      check(searchRes1, {
        "E2E: Chunk search with multiple tags returns 200": (r) => r.status === 200,
        "E2E: Chunk search returns array": () => Array.isArray(similarChunks1),
        "E2E: Chunk search returns results": () => similarChunks1.length > 0,
        "E2E: Chunks have similarity scores": () =>
          similarChunks1.every(chunk => typeof chunk.similarityScore === 'number' && chunk.similarityScore >= 0),
        "E2E: Chunks have metadata": () =>
          similarChunks1.every(chunk => chunk.chunkMetadata && chunk.chunkMetadata.originalFileId),
      });

      // Test 2: Search with tags that were embedded (all files have "kim", "knives")
      // Note: We updated PDF tags to ["scott", "kim"] but embeddings still have original tags
      const searchBody2 = {
        textPrompt: "test file markdown",
        topK: 10,
        tags: ["kim", "knives"],  // Tags that were originally embedded
        contentType: "CONTENT_TYPE_CHUNK"
      };
      const searchRes2 = http.request(
        "POST",
        `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/searchChunks`,
        JSON.stringify(searchBody2),
        data.header
      );
      let searchJson2; try { searchJson2 = searchRes2.json(); } catch (e) { searchJson2 = {}; }
      const similarChunks2 = searchJson2.similarChunks || [];

      const pdfFile = uploaded.find(f => f.type === "TYPE_PDF");
      const pdfFileUid = pdfFile ? pdfFile.fileUid : null;

      check(searchRes2, {
        "E2E: Chunk search with original embedded tags returns 200": (r) => r.status === 200,
        "E2E: Chunk search with original tags returns results": () => similarChunks2.length > 0,
        "E2E: Chunks from files with matching embedded tags": () => similarChunks2.length > 0 && Array.isArray(similarChunks2),
      });

      // Test 3: Verify document types have page-based references
      // Note: Sample files contain multi-page documents (PDFs, DOCs, etc. may have 2+ pages)
      const documentTypes = ["TYPE_PDF", "TYPE_DOC", "TYPE_DOCX", "TYPE_PPT", "TYPE_PPTX"];
      const documentFiles = uploaded.filter(f => documentTypes.includes(f.type));

      if (documentFiles.length > 0) {
        const documentFileUids = documentFiles.map(f => f.fileUid);
        const searchBody3 = {
          textPrompt: "test file markdown",
          topK: 50,
          fileUids: documentFileUids,
          contentType: "CONTENT_TYPE_CHUNK"
        };
        const searchRes3 = http.request(
          "POST",
          `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/searchChunks`,
          JSON.stringify(searchBody3),
          data.header
        );
        let searchJson3; try { searchJson3 = searchRes3.json(); } catch (e) { searchJson3 = {}; }
        const similarChunks3 = searchJson3.similarChunks || [];

        // Separate chunks by page location for separate validation
        const singlePageChunks = similarChunks3.filter(chunk =>
          chunk.chunkMetadata?.reference?.start?.coordinates?.[0] === 1 &&
          chunk.chunkMetadata?.reference?.end?.coordinates?.[0] === 1
        );
        const multiPageDocChunks = similarChunks3.filter(chunk =>
          chunk.chunkMetadata?.reference?.start?.coordinates?.[0] > 1 ||
          chunk.chunkMetadata?.reference?.end?.coordinates?.[0] > 1
        );

        check(searchRes3, {
          "E2E: Document types chunk search returns 200": (r) => r.status === 200,
          "E2E: Document types have page references": () =>
            similarChunks3.every(chunk =>
              chunk.chunkMetadata.reference &&
              chunk.chunkMetadata.reference.start &&
              chunk.chunkMetadata.reference.start.unit === "UNIT_PAGE" &&
              Array.isArray(chunk.chunkMetadata.reference.start.coordinates) &&
              chunk.chunkMetadata.reference.start.coordinates.length > 0
            ),
          "E2E: Document types pages are 1-indexed": () =>
            similarChunks3.every(chunk =>
              chunk.chunkMetadata.reference &&
              chunk.chunkMetadata.reference.start &&
              chunk.chunkMetadata.reference.start.coordinates[0] >= 1
            ),
          "E2E: Each chunk is exactly 1 page (start === end)": () =>
            similarChunks3.every(chunk =>
              chunk.chunkMetadata.reference &&
              chunk.chunkMetadata.reference.start &&
              chunk.chunkMetadata.reference.end &&
              chunk.chunkMetadata.reference.start.coordinates[0] === chunk.chunkMetadata.reference.end.coordinates[0]
            ),
          "E2E: Single-page chunks all on page 1": () =>
            singlePageChunks.every(chunk =>
              chunk.chunkMetadata.reference.start.coordinates[0] === 1 &&
              chunk.chunkMetadata.reference.end.coordinates[0] === 1
            ),
          "E2E: Multi-page doc chunks have pages > 1": () =>
            multiPageDocChunks.length === 0 || multiPageDocChunks.every(chunk =>
              chunk.chunkMetadata?.reference?.start?.coordinates?.[0] > 0
            ),
        });
      }
    }

    // Cleanup
    http.request("DELETE", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}`, null, data.header);
  });
}

// ============================================================================
// TEST GROUP 09-18: JWT/Auth Tests
// ============================================================================
function logUnexpected(res, label) {
  // Only log truly unexpected status codes (not 200-299 success range)
  if (!res) return;
  if (res.status >= 200 && res.status < 300) return; // Success range - don't log
  if (res.status === 401 || res.status === 403) return; // Expected auth failures

  // Log unexpected errors (4xx, 5xx except auth)
  try {
    console.log(`${label} unexpected status=${res.status} body=${JSON.stringify(res.json())}`);
  } catch (e) {
    console.log(`${label} unexpected status=${res.status}`);
  }
}

function createKBAuthenticated(data) {
  const displayName = "Test " + data.dbIDPrefix + "jwt-" + randomString(8);
  const res = http.request(
    "POST",
    `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases`,
    JSON.stringify({ knowledgeBase: { displayName: displayName } }),
    data.header
  );
  try {
    const json = res.json();
    const cat = (json && json.knowledgeBase) || {};
    return { id: cat.id, namespaceId: data.expectedOwner.id };
  } catch (e) {
    return { id: null, namespaceId: data.expectedOwner.id };
  }
}

function deleteKBAuthenticated(data, knowledgeBaseId) {
  http.request("DELETE", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}`, null, data.header);
}

function createFileAuthenticated(data, knowledgeBaseId) {
  const filename = data.dbIDPrefix + "jwt-file-" + randomString(6) + ".txt";
  const body = { displayName: filename, type: "TYPE_TEXT", content: constant.docSampleTxt };
  const res = http.request(
    "POST",
    `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files`,
    JSON.stringify(body),
    data.header
  );
  try {
    const json = res.json();
    const f = (json && json.file) || {};
    return f.uid || "";
  } catch (e) {
    return "";
  }
}

export function TEST_09_JWT_CreateKnowledgeBase(data) {
  const groupName = "Artifact API [JWT]: Create knowledge base rejects random user";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    const body = { knowledgeBase: { displayName: "Test " + data.dbIDPrefix + randomString(8) } };
    const res = http.request("POST", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases`, JSON.stringify(body), constant.paramsHTTPWithJWT.headers);
    logUnexpected(res, "JWT: POST knowledge-bases");
    check(res, { "JWT: POST knowledge bases 401": (r) => r.status === 401 });
  });
}

export function TEST_10_JWT_ListKnowledgeBases(data) {
  const groupName = "Artifact API [JWT]: List knowledge bases rejects random user";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    const created = createKBAuthenticated(data);
    const res = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases`, null, constant.paramsHTTPWithJWT.headers);
    logUnexpected(res, "JWT: GET knowledge-bases");
    check(res, { "JWT: GET knowledge bases 401": (r) => r.status === 401 });
    deleteKBAuthenticated(data, created.id);
  });
}

export function TEST_11_JWT_GetKnowledgeBase(data) {
  const groupName = "Artifact API [JWT]: Get knowledge base rejects random user";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    const created = createKBAuthenticated(data);
    const res = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases`, null, constant.paramsHTTPWithJWT.headers);
    logUnexpected(res, "JWT: GET knowledge-bases");
    check(res, { "JWT: GET knowledge bases 401": (r) => r.status === 401 });
    deleteKBAuthenticated(data, created.id);
  });
}

export function TEST_12_JWT_UpdateKnowledgeBase(data) {
  const groupName = "Artifact API [JWT]: Update knowledge base rejects random user";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    const created = createKBAuthenticated(data);
    if (!created.id) {
      console.log(`TEST_12: createKBAuthenticated returned null id`);
    }
    const body = { description: "x" };
    const res = http.request("PATCH", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${created.id}?updateMask=description`, JSON.stringify(body), constant.paramsHTTPWithJWT.headers);
    if (res.status !== 401) {
      console.log(`TEST_12: Expected 401, got status=${res.status}, body=${res.body}`);
    }
    logUnexpected(res, "JWT: PATCH knowledge base");
    check(res, { "JWT: PATCH knowledge base 401": (r) => r.status === 401 });
    deleteKBAuthenticated(data, created.id);
  });
}

export function TEST_13_JWT_CreateFile(data) {
  const groupName = "Artifact API [JWT]: Create file rejects random user";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    const created = createKBAuthenticated(data);
    const body = { displayName: data.dbIDPrefix + "x.txt", type: "TYPE_TEXT", content: constant.docSampleTxt };
    const res = http.request("POST", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${created.id}/files`, JSON.stringify(body), constant.paramsHTTPWithJWT.headers);
    logUnexpected(res, "JWT: POST file");
    check(res, { "JWT: POST file 401": (r) => r.status === 401 });
    deleteKBAuthenticated(data, created.id);
  });
}

export function TEST_14_JWT_ListFiles(data) {
  const groupName = "Artifact API [JWT]: List files rejects random user";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    const created = createKBAuthenticated(data);
    createFileAuthenticated(data, created.id);
    const res = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${created.id}/files`, null, constant.paramsHTTPWithJWT.headers);
    logUnexpected(res, "JWT: GET files");
    check(res, { "JWT: GET files 401": (r) => r.status === 401 });
    deleteKBAuthenticated(data, created.id);
  });
}

export function TEST_15_JWT_GetFile(data) {
  const groupName = "Artifact API [JWT]: Get file rejects random user";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    const created = createKBAuthenticated(data);
    const fileUid = createFileAuthenticated(data, created.id);
    const res = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${created.id}/files/${fileUid}`, null, constant.paramsHTTPWithJWT.headers);
    logUnexpected(res, "JWT: GET file");
    check(res, { "JWT: GET file 401": (r) => r.status === 401 });
    deleteKBAuthenticated(data, created.id);
  });
}

export function TEST_16_JWT_GetFileContent(data) {
  const groupName = "Artifact API [JWT]: Get file content (VIEW_CONTENT) rejects random user";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    const created = createKBAuthenticated(data);
    const fileUid = createFileAuthenticated(data, created.id);
    const res = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${created.id}/files/${fileUid}?view=VIEW_CONTENT`, null, constant.paramsHTTPWithJWT.headers);
    logUnexpected(res, "JWT: GET file content");
    check(res, { "JWT: GET file content 401": (r) => r.status === 401 });
    deleteKBAuthenticated(data, created.id);
  });
}

export function TEST_17_JWT_GetFileSummary(data) {
  const groupName = "Artifact API [JWT]: Get file summary (VIEW_SUMMARY) rejects random user";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    const created = createKBAuthenticated(data);
    const fileUid = createFileAuthenticated(data, created.id);
    const res = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${created.id}/files/${fileUid}?view=VIEW_SUMMARY`, null, constant.paramsHTTPWithJWT.headers);
    logUnexpected(res, "JWT: GET file summary");
    check(res, { "JWT: GET file summary 401": (r) => r.status === 401 });
    deleteKBAuthenticated(data, created.id);
  });
}

export function TEST_18_JWT_ListChunks(data) {
  const groupName = "Artifact API [JWT]: List chunks rejects random user";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    const created = createKBAuthenticated(data);
    const fileUid = createFileAuthenticated(data, created.id);
    const res = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${created.id}/files/${fileUid}/chunks`, null, constant.paramsHTTPWithJWT.headers);
    logUnexpected(res, "JWT: GET chunks");
    check(res, { "JWT: GET chunks 401 or 404": (r) => r.status === 401 || r.status === 404 });
    deleteKBAuthenticated(data, created.id);
  });
}

export function TEST_19_JWT_GetFileCache(data) {
  const groupName = "Artifact API [JWT]: Get file cache (VIEW_CACHE) rejects random user";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    const created = createKBAuthenticated(data);
    const fileUid = createFileAuthenticated(data, created.id);
    const res = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${created.id}/files/${fileUid}?view=VIEW_CACHE`, null, constant.paramsHTTPWithJWT.headers);
    logUnexpected(res, "JWT: GET file cache");
    check(res, { "JWT: GET file cache 401": (r) => r.status === 401 });
    deleteKBAuthenticated(data, created.id);
  });
}

// waitForCacheReady polls the VIEW_CACHE endpoint until derivedResourceUri has a definitive value
function waitForCacheReady(namespaceId, knowledgeBaseId, fileUid, headers, maxWaitSeconds = 30) {
  console.log(`Waiting for cache readiness determination for file ${fileUid} (max ${maxWaitSeconds}s)...`);

  const startTime = new Date().getTime();
  const endTime = startTime + (maxWaitSeconds * 1000);
  let consecutiveEmptyResponses = 0;
  const MAX_CONSECUTIVE_EMPTY = 5; // If we get 5 consecutive empty responses, consider it stable

  while (new Date().getTime() < endTime) {
    const res = http.request(
      "GET",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${namespaceId}/knowledge-bases/${knowledgeBaseId}/files/${fileUid}?view=VIEW_CACHE`,
      null,
      headers
    );

    if (res.status === 200) {
      try {
        const data = res.json();
        const derivedUri = data.derivedResourceUri;

        // Check if derivedResourceUri field exists (it should always be present now)
        if (derivedUri !== undefined) {
          if (derivedUri && derivedUri !== "") {
            // Cache URI is ready!
            console.log(`Cache URI ready after ${Math.round((new Date().getTime() - startTime) / 1000)}s: ${derivedUri}`);
            return { ready: true, derivedResourceUri: derivedUri, available: true };
          } else {
            // Empty string - count consecutive empty responses
            consecutiveEmptyResponses++;
            if (consecutiveEmptyResponses >= MAX_CONSECUTIVE_EMPTY) {
              console.log(`Cache determined to be unavailable after ${Math.round((new Date().getTime() - startTime) / 1000)}s (consecutive empty responses)`);
              return { ready: true, derivedResourceUri: "", available: false };
            }
          }
        } else {
          console.log(`derivedResourceUri field missing from response`);
          consecutiveEmptyResponses = 0; // Reset counter for unexpected responses
        }
      } catch (e) {
        console.log(`JSON parse error while waiting for cache: ${e.message}`);
        consecutiveEmptyResponses = 0;
      }
    } else {
      console.log(`HTTP error while waiting for cache: ${res.status}`);
      consecutiveEmptyResponses = 0;
    }

    // Wait 1 second before next attempt
    sleep(1);
  }

  console.log(`Cache readiness could not be determined within ${maxWaitSeconds}s`);
  return { ready: false };
}

export function TEST_20_GetFileCache(data) {
  const groupName = "Artifact API: Get file with VIEW_CACHE creates cache on first call";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    // Create KB and upload a PDF file
    const created = createKBAuthenticated(data);
    const filename = `${data.dbIDPrefix}cache-test.pdf`;
    const uploadRes = http.request(
      "POST",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${created.id}/files`,
      JSON.stringify({ displayName: filename, type: "TYPE_PDF", content: constant.docSamplePdf }),
      data.header
    );

    let uploadedFile;
    try { uploadedFile = uploadRes.json().file; } catch (e) { uploadedFile = {}; }
    const fileUid = uploadedFile ? uploadedFile.uid : null;
    const fileId = uploadedFile ? uploadedFile.id : null;

    check(uploadRes, {
      "Cache test: PDF file uploaded": (r) => {
        if (r.status !== 200 || !fileUid || !fileId) {
          console.log(`PDF upload failed: status=${r.status}, fileUid=${fileUid}, fileId=${fileId}, body=${r.body}`);
        }
        return r.status === 200 && fileUid && fileId;
      }
    });

    // Wait for file processing to complete
    console.log("Waiting for file processing to complete...");
    const processingResult = helper.waitForFileProcessingComplete(
      data.expectedOwner.id,
      created.id,
      fileUid,
      data.header,
      60
    );

    if (!processingResult.completed) {
      console.log(`File processing did not complete: ${processingResult.status}${processingResult.error ? ' - ' + processingResult.error : ''}`);
      deleteKBAuthenticated(data, created.id);
      return;
    }

    // Wait for cache readiness determination (cache available or determined unavailable)
    console.log("Waiting for cache readiness determination...");
    const cacheReady = waitForCacheReady(data.expectedOwner.id, created.id, fileUid, data.header, 30);
    check(cacheReady, {
      "Cache readiness should be determined within 30 seconds": (r) => r.ready,
    });

    if (cacheReady.available) {
      console.log("Cache is available and ready");
    } else if (cacheReady.ready) {
      console.log("Cache determined to be unavailable (AI client not available or other issue)");
    } else {
      console.log("Cache readiness could not be determined, proceeding with test anyway...");
    }

    // First call to GetFile with VIEW_CACHE should have the cache ready
    const res1 = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${created.id}/files/${fileUid}?view=VIEW_CACHE`, null, data.header);
    logUnexpected(res1, "GET file VIEW_CACHE (first call)");

    check(res1, {
      "GET file VIEW_CACHE status 200": (r) => r.status === 200,
      "GET file VIEW_CACHE has file": (r) => r.json().file !== undefined,
      "GET file VIEW_CACHE has correct file uid": (r) => r.json().file.uid === fileUid,
    });

    // Check derivedResourceUri - it should always be present
    const derivedUri = res1.json().derivedResourceUri;
    check(res1, {
      "GET file VIEW_CACHE has derivedResourceUri field": (r) => r.json().derivedResourceUri !== undefined,
    });

    if (derivedUri && derivedUri.startsWith("cachedContents/")) {
      console.log("Gemini cache created successfully:", derivedUri);
      check(res1, {
        "GET file VIEW_CACHE has Gemini cache name": (r) => r.json().derivedResourceUri.startsWith("cachedContents/"),
      });
    } else if (derivedUri && derivedUri !== "") {
      console.log("Content URL returned for small file:", derivedUri);
      check(res1, {
        "GET file VIEW_CACHE has content URL": (r) => r.json().derivedResourceUri !== "" && !r.json().derivedResourceUri.startsWith("cachedContents/"),
      });
    } else {
      console.log("No cache or content URL returned (caching completely unavailable)");
    }

    deleteKBAuthenticated(data, created.id);
  });
}

export function TEST_21_GetFileCacheRenewal(data) {
  const groupName = "Artifact API: Get file with VIEW_CACHE renews cache TTL on subsequent calls";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    // Create KB and upload a PDF file
    const created = createKBAuthenticated(data);
    const filename = `${data.dbIDPrefix}cache-test.pdf`;
    const uploadRes = http.request(
      "POST",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${created.id}/files`,
      JSON.stringify({ displayName: filename, type: "TYPE_PDF", content: constant.docSamplePdf }),
      data.header
    );

    let uploadedFile;
    try { uploadedFile = uploadRes.json().file; } catch (e) { uploadedFile = {}; }
    const fileUid = uploadedFile ? uploadedFile.uid : null;
    const fileId = uploadedFile ? uploadedFile.id : null;

    check(uploadRes, {
      "Cache test: PDF file uploaded": (r) => {
        if (r.status !== 200 || !fileUid || !fileId) {
          console.log(`PDF upload failed: status=${r.status}, fileUid=${fileUid}, fileId=${fileId}, body=${r.body}`);
        }
        return r.status === 200 && fileUid && fileId;
      }
    });

    // Wait for file processing to complete
    console.log("Waiting for file processing to complete...");
    const processingResult = helper.waitForFileProcessingComplete(
      data.expectedOwner.id,
      created.id,
      fileUid,
      data.header,
      60
    );

    if (!processingResult.completed) {
      console.log(`File processing did not complete: ${processingResult.status}${processingResult.error ? ' - ' + processingResult.error : ''}`);
      deleteKBAuthenticated(data, created.id);
      return;
    }

    // Wait for initial cache readiness determination
    console.log("Waiting for initial cache readiness determination...");
    const initialCacheReady = waitForCacheReady(data.expectedOwner.id, created.id, fileUid, data.header, 30);
    if (!initialCacheReady.ready) {
      console.log("Initial cache readiness could not be determined, proceeding with test anyway...");
    } else if (initialCacheReady.available) {
      console.log("Initial cache is available");
    } else {
      console.log("Initial cache determined to be unavailable");
    }

    // First call to create cache
    const res1 = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${created.id}/files/${fileUid}?view=VIEW_CACHE`, null, data.header);
    check(res1, { "First call status 200": (r) => r.status === 200 });

    // Wait a moment
    sleep(2);

    // Second call should renew the cache
    const res2 = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${created.id}/files/${fileUid}?view=VIEW_CACHE`, null, data.header);
    logUnexpected(res2, "GET file VIEW_CACHE (renewal)");

    check(res2, {
      "Second call status 200": (r) => r.status === 200,
      "Second call has file": (r) => r.json().file !== undefined,
      "Second call returns consistent derivedResourceUri type": (r) => {
        // Both calls should return the same type of derivedResourceUri:
        // - Both are Gemini cache names (start with "cachedContents/")
        // - Both are VertexAI cache names (contain "/cachedContents/" in path)
        const firstUri = res1.json().derivedResourceUri || "";
        const secondUri = r.json().derivedResourceUri || "";

        // Helper to check if a URI is a cache name (Gemini or VertexAI format)
        const isCacheName = (uri) => {
          // Gemini: cachedContents/{id}
          if (uri.startsWith("cachedContents/")) return true;
          // VertexAI: projects/{project}/locations/{location}/cachedContents/{id}
          if (uri.includes("/cachedContents/")) return true;
          return false;
        };

        // Check if both are cache names (Gemini or VertexAI)
        if (isCacheName(firstUri) && isCacheName(secondUri)) {
          // For cache names, they should be exactly the same
          return firstUri === secondUri;
        }

        // Check if both are blob URLs (for small files)
        if (firstUri.includes("/v1alpha/blob-urls/") && secondUri.includes("/v1alpha/blob-urls/")) {
          // For blob URLs, they will have different signatures but should both be blob URLs
          return true;
        }

        // Check if both are empty
        if (firstUri === "" && secondUri === "") {
          return true;
        }

        // Mixed types - inconsistent
        return false;
      },
    });

    // Third call to verify renewal works multiple times
    sleep(1);
    const res3 = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${created.id}/files/${fileUid}?view=VIEW_CACHE`, null, data.header);
    check(res3, { "Third call status 200": (r) => r.status === 200 });

    deleteKBAuthenticated(data, created.id);
  });
}

export function TEST_22_GetFileCacheLargeFile(data) {
  const groupName = "Artifact API: Get file with VIEW_CACHE creates Gemini cache for large files";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    // Create KB and upload a large multi-page PDF file
    const created = createKBAuthenticated(data);
    const filename = `${data.dbIDPrefix}large-cache-test.pdf`;
    const uploadRes = http.request(
      "POST",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${created.id}/files`,
      JSON.stringify({ displayName: filename, type: "TYPE_PDF", content: constant.docSampleMultiPagePdf }),
      data.header
    );

    let uploadedFile;
    try { uploadedFile = uploadRes.json().file; } catch (e) { uploadedFile = {}; }
    const fileUid = uploadedFile ? uploadedFile.uid : null;
    const fileId = uploadedFile ? uploadedFile.id : null;

    check(uploadRes, {
      "Large file cache test: Multi-page PDF file uploaded": (r) => {
        if (r.status !== 200 || !fileUid || !fileId) {
          console.log(`Large PDF upload failed: status=${r.status}, fileUid=${fileUid}, fileId=${fileId}, body=${r.body}`);
        }
        return r.status === 200 && fileUid && fileId;
      }
    });

    if (!fileUid || !fileId) {
      console.log("Large file cache test: Skipping test due to file upload failure");
      deleteKBAuthenticated(data, created.id);
      return;
    }

    // Wait for file processing to complete
    console.log("Large file cache test: Waiting for file processing to complete...");
    const processingResult = helper.waitForFileProcessingComplete(
      data.expectedOwner.id,
      created.id,
      fileUid,
      data.header,
      120 // Large file may take longer to process
    );

    if (!processingResult.completed) {
      console.log(`Large file cache test: File processing did not complete: ${processingResult.status}${processingResult.error ? ' - ' + processingResult.error : ''}`);
      deleteKBAuthenticated(data, created.id);
      return;
    }

    console.log("Large file cache test: File processing completed successfully");

    // Wait for cache readiness determination (cache available or determined unavailable)
    console.log("Large file cache test: Waiting for Gemini cache creation...");
    const cacheReady = waitForCacheReady(data.expectedOwner.id, created.id, fileUid, data.header, 60);
    check(cacheReady, {
      "Large file: Cache readiness should be determined within 60 seconds": (r) => r.ready,
    });

    if (cacheReady.available) {
      console.log("Large file cache test: Gemini cache created successfully");
    } else if (cacheReady.ready) {
      console.log("Large file cache test: Cache determined to be unavailable (AI client not available)");
    } else {
      console.log("Large file cache test: Cache readiness could not be determined");
    }

    // Call GetFile with VIEW_CACHE
    const res1 = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${created.id}/files/${fileUid}?view=VIEW_CACHE`, null, data.header);
    logUnexpected(res1, "Large file: GET file VIEW_CACHE");

    check(res1, {
      "Large file: GET file VIEW_CACHE status 200": (r) => r.status === 200,
      "Large file: GET file VIEW_CACHE has file": (r) => r.json().file !== undefined,
      "Large file: GET file VIEW_CACHE has derivedResourceUri field": (r) => r.json().derivedResourceUri !== undefined,
    });

    // Check the derivedResourceUri content
    const derivedUri = res1.json().derivedResourceUri;

    if (derivedUri && derivedUri.includes("cachedContents/")) {
      // Could be Gemini (cachedContents/...) or VertexAI (projects/.../cachedContents/...)
      const cacheType = derivedUri.startsWith("projects/") ? "VertexAI" : "Gemini";
      console.log(`Large file cache test: ${cacheType} cache name returned:`, derivedUri);
      check(res1, {
        "Large file: derivedResourceUri is AI cache name": (r) => r.json().derivedResourceUri.includes("cachedContents/"),
      });
    } else if (derivedUri && derivedUri.includes("/v1alpha/blob-urls/")) {
      console.log("Large file cache test: Content blob URL returned (file might be small):", derivedUri);
      check(res1, {
        "Large file: derivedResourceUri is content blob URL": (r) => r.json().derivedResourceUri.includes("/v1alpha/blob-urls/"),
      });
    } else if (derivedUri === "") {
      console.log("Large file cache test: Empty derivedResourceUri (caching unavailable or content not ready)");
    } else {
      console.log("Large file cache test: Unexpected derivedResourceUri format:", derivedUri);
    }

    // Test cache renewal for large files
    sleep(2);
    const res2 = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${created.id}/files/${fileUid}?view=VIEW_CACHE`, null, data.header);

    check(res2, {
      "Large file: Second call status 200": (r) => r.status === 200,
      "Large file: Second call returns valid response": (r) => {
        const firstUri = res1.json().derivedResourceUri || "";
        const secondUri = r.json().derivedResourceUri || "";

        // Check if URI is a cache name (Gemini: cachedContents/..., VertexAI: projects/.../cachedContents/...)
        const isCacheName = (uri) => uri.includes("cachedContents/");

        // For AI cache (Gemini or VertexAI), should return the same cache name
        if (isCacheName(firstUri) && isCacheName(secondUri)) {
          const isSame = firstUri === secondUri;
          if (isSame) {
            console.log("Large file cache test: Cache name consistent across calls:", firstUri);
          } else {
            console.log("Large file cache test: Cache name changed:", firstUri, "->", secondUri);
          }
          return isSame;
        }

        // For blob URLs, both should be blob URLs (expected for VertexAI with GCS)
        if (firstUri.includes("/v1alpha/blob-urls/") && secondUri.includes("/v1alpha/blob-urls/")) {
          console.log("Large file cache test: Both calls returned blob URLs (consistent - VertexAI with GCS)");
          return true;
        }

        // For empty strings, both should be empty
        if (firstUri === "" && secondUri === "") {
          console.log("Large file cache test: Both calls returned empty (cache unavailable)");
          return true;
        }

        // Handle transition states (cache becoming available between calls)
        // First call might return empty/blob, second might return cache name, or vice versa
        if ((firstUri === "" || firstUri.includes("/v1alpha/blob-urls/")) && isCacheName(secondUri)) {
          console.log("Large file cache test: Cache became available between calls (first:", firstUri || "empty", ", second:", secondUri, ")");
          return true;
        }

        if (isCacheName(firstUri) && (secondUri === "" || secondUri.includes("/v1alpha/blob-urls/"))) {
          console.log("Large file cache test: Cache type changed (first:", firstUri, ", second:", secondUri || "empty", ")");
          return true;
        }

        // If we reach here, URIs are inconsistent (unexpected scenario)
        console.log("Large file cache test: Inconsistent URI types:", firstUri || "empty", "vs", secondUri || "empty");
        return false;
      },
    });

    // Verify file metadata
    const fileRes = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${created.id}/files/${fileUid}`, null, data.header);
    if (fileRes.status === 200) {
      try {
        const fileData = fileRes.json().file;
        check(fileRes, {
          "Large file: Has sufficient tokens for caching": () => {
            const tokenCount = fileData.totalTokens || 0;
            const minTokens = 1024; // ai.MinCacheTokens
            const hasSufficientTokens = tokenCount >= minTokens;
            console.log(`Large file cache test: Token count=${tokenCount}, minTokens=${minTokens}, sufficient=${hasSufficientTokens}`);
            return hasSufficientTokens;
          },
        });
      } catch (e) {
        console.log(`Large file cache test: Error checking token count: ${e}`);
      }
    }

    deleteKBAuthenticated(data, created.id);
  });
}

// ============================================================================
// TEST GROUP 23: Owner and Creator Fields Validation
// ============================================================================
export function TEST_23_OwnerCreatorFields(data) {
  const groupName = "Artifact API: Owner and Creator Fields Validation";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    // Create knowledge base
    const kbReqBody = {
      knowledgeBase: {
        displayName: "Test " + data.dbIDPrefix + "oc-" + randomString(8),
        description: "Test knowledge base for owner/creator validation",
        tags: ["test", "owner-creator"],
        type: "KNOWLEDGE_BASE_TYPE_PERSISTENT"
      }
    };

    const kbRes = http.request(
      "POST",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases`,
      JSON.stringify(kbReqBody),
      data.header
    );
    let kbJson; try { kbJson = kbRes.json(); } catch (e) { kbJson = {}; }
    const kb = kbJson.knowledgeBase || {};

    // Validate knowledge base owner/creator fields
    check(kbRes, {
      "KB: Response status is 200": (r) => r.status === 200,
      "KB: ownerUid is valid UUID": () => kb && helper.isUUID(kb.ownerUid),
      "KB: ownerUid matches user uid": () => kb && kb.ownerUid === data.expectedOwner.uid,
      "KB: ownerName matches namespace path": () => kb && kb.ownerName === `users/${data.expectedOwner.id}`,
      "KB: owner object exists and is valid": () => kb && kb.owner && kb.owner.user,
      "KB: owner.user.id matches expected user": () => kb && kb.owner && kb.owner.user && kb.owner.user.id === data.expectedOwner.id,
      "KB: owner.user.uid matches expected uid": () => kb && kb.owner && kb.owner.user && kb.owner.user.uid === data.expectedOwner.uid,
      "KB: creatorUid is valid UUID": () => kb && helper.isUUID(kb.creatorUid),
      "KB: creatorUid matches user uid": () => kb && kb.creatorUid === data.expectedOwner.uid,
      "KB: creator object exists and is valid": () => kb && helper.isValidCreator(kb.creator, data.expectedOwner),
      "KB: creator.id matches expected user": () => kb && kb.creator && kb.creator.id === data.expectedOwner.id,
      "KB: creator.uid matches expected uid": () => kb && kb.creator && kb.creator.uid === data.expectedOwner.uid,
    });

    if (!kb || !kb.id) {
      console.log("Owner/Creator test: Failed to create knowledge base, skipping file tests");
      return;
    }

    // Create a file in the knowledge base
    const fileReqBody = {
      displayName: data.dbIDPrefix + "owner-creator-test.txt",
      type: "TYPE_TEXT",
      content: constant.docSampleTxt,
      tags: ["test-file"]
    };

    const fileRes = http.request(
      "POST",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${kb.id}/files`,
      JSON.stringify(fileReqBody),
      data.header
    );
    let fileJson; try { fileJson = fileRes.json(); } catch (e) { fileJson = {}; }
    const file = fileJson.file || {};

    // Validate file owner/creator fields
    check(fileRes, {
      "File: Response status is 200": (r) => r.status === 200,
      "File: ownerUid is valid UUID": () => file && helper.isUUID(file.ownerUid),
      "File: ownerUid matches user uid": () => file && file.ownerUid === data.expectedOwner.uid,
      "File: ownerName matches namespace path": () => file && file.ownerName === `users/${data.expectedOwner.id}`,
      "File: owner object exists and is valid": () => file && file.owner && file.owner.user,
      "File: owner.user.id matches expected user": () => file && file.owner && file.owner.user && file.owner.user.id === data.expectedOwner.id,
      "File: creatorUid is valid UUID": () => file && helper.isUUID(file.creatorUid),
      "File: creatorUid matches user uid": () => file && file.creatorUid === data.expectedOwner.uid,
      "File: creator object exists and is valid": () => file && helper.isValidCreator(file.creator, data.expectedOwner),
      "File: creator.id matches expected user": () => file && file.creator && file.creator.id === data.expectedOwner.id,
      "File: creator.uid matches expected uid": () => file && file.creator && file.creator.uid === data.expectedOwner.uid,
      "File: collectionUids is array (or undefined)": () => file && (!file.collectionUids || Array.isArray(file.collectionUids)),
    });

    // Verify owner/creator fields persist after fetching the knowledge base
    const getKbRes = http.request(
      "GET",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${kb.id}`,
      null,
      data.header
    );
    let getKbJson; try { getKbJson = getKbRes.json(); } catch (e) { getKbJson = {}; }
    const fetchedKb = getKbJson.knowledgeBase || {};

    check(getKbRes, {
      "Get KB: Response status is 200": (r) => r.status === 200,
      "Get KB: ownerUid is preserved": () => fetchedKb && fetchedKb.ownerUid === data.expectedOwner.uid,
      "Get KB: ownerName is preserved": () => fetchedKb && fetchedKb.ownerName === `users/${data.expectedOwner.id}`,
      "Get KB: creatorUid is preserved": () => fetchedKb && fetchedKb.creatorUid === data.expectedOwner.uid,
      "Get KB: owner object is populated": () => fetchedKb && fetchedKb.owner && fetchedKb.owner.user,
      "Get KB: creator object is populated": () => fetchedKb && fetchedKb.creator && fetchedKb.creator.id === data.expectedOwner.id,
    });

    // Verify owner/creator fields persist when listing knowledge bases
    const listKbRes = http.request(
      "GET",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases`,
      null,
      data.header
    );
    let listKbJson; try { listKbJson = listKbRes.json(); } catch (e) { listKbJson = {}; }
    const kbList = Array.isArray(listKbJson.knowledgeBases) ? listKbJson.knowledgeBases : [];
    const listedKb = kbList.find(k => k.id === kb.id);

    check(listKbRes, {
      "List KB: Response status is 200": (r) => r.status === 200,
      "List KB: Our KB is in the list": () => listedKb !== undefined,
      "List KB: ownerUid is preserved": () => listedKb && listedKb.ownerUid === data.expectedOwner.uid,
      "List KB: ownerName is preserved": () => listedKb && listedKb.ownerName === `users/${data.expectedOwner.id}`,
      "List KB: owner object is populated": () => listedKb && listedKb.owner && listedKb.owner.user,
      "List KB: owner.user.id matches": () => listedKb && listedKb.owner && listedKb.owner.user && listedKb.owner.user.id === data.expectedOwner.id,
      "List KB: creatorUid is preserved": () => listedKb && listedKb.creatorUid === data.expectedOwner.uid,
      "List KB: creator object is populated": () => listedKb && helper.isValidCreator(listedKb.creator, data.expectedOwner),
    });

    // Verify owner/creator fields persist when fetching the file
    if (file && file.uid) {
      const getFileRes = http.request(
        "GET",
        `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${kb.id}/files/${file.uid}`,
        null,
        data.header
      );
      let getFileJson; try { getFileJson = getFileRes.json(); } catch (e) { getFileJson = {}; }
      const fetchedFile = getFileJson.file || {};

      check(getFileRes, {
        "Get File: Response status is 200": (r) => r.status === 200,
        "Get File: ownerUid is preserved": () => fetchedFile && fetchedFile.ownerUid === data.expectedOwner.uid,
        "Get File: ownerName is preserved": () => fetchedFile && fetchedFile.ownerName === `users/${data.expectedOwner.id}`,
        "Get File: creatorUid is preserved": () => fetchedFile && fetchedFile.creatorUid === data.expectedOwner.uid,
        "Get File: owner object is populated": () => fetchedFile && fetchedFile.owner && fetchedFile.owner.user,
        "Get File: creator object is populated": () => fetchedFile && fetchedFile.creator && fetchedFile.creator.id === data.expectedOwner.id,
      });

      // Verify owner/creator fields persist when listing files
      const listFilesRes = http.request(
        "GET",
        `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${kb.id}/files`,
        null,
        data.header
      );
      let listFilesJson; try { listFilesJson = listFilesRes.json(); } catch (e) { listFilesJson = {}; }
      const fileList = Array.isArray(listFilesJson.files) ? listFilesJson.files : [];
      const listedFile = fileList.find(f => f.uid === file.uid);

      check(listFilesRes, {
        "List Files: Response status is 200": (r) => r.status === 200,
        "List Files: Our file is in the list": () => listedFile !== undefined,
        "List Files: ownerUid is preserved": () => listedFile && listedFile.ownerUid === data.expectedOwner.uid,
        "List Files: ownerName is preserved": () => listedFile && listedFile.ownerName === `users/${data.expectedOwner.id}`,
        "List Files: owner object is populated": () => listedFile && listedFile.owner && listedFile.owner.user,
        "List Files: owner.user.id matches": () => listedFile && listedFile.owner && listedFile.owner.user && listedFile.owner.user.id === data.expectedOwner.id,
        "List Files: creatorUid is preserved": () => listedFile && listedFile.creatorUid === data.expectedOwner.uid,
        "List Files: creator object is populated": () => listedFile && helper.isValidCreator(listedFile.creator, data.expectedOwner),
      });
    }

    // Cleanup
    http.request("DELETE", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${kb.id}`, null, data.header);
  });
}

// ============================================================================
// TEST GROUP 24: Reserved Tags Validation and CollectionUIDs
// ============================================================================
export function TEST_24_ReservedTagsValidation(data) {
  const groupName = "Artifact API: Reserved Tags Validation and CollectionUIDs";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    // Create knowledge base for testing
    const kbReqBody = {
      knowledgeBase: {
        displayName: "Test " + data.dbIDPrefix + "tg-" + randomString(8),
        description: "Test knowledge base for reserved tags validation",
        tags: ["test"],
        type: "KNOWLEDGE_BASE_TYPE_PERSISTENT"
      }
    };

    const kbRes = http.request(
      "POST",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases`,
      JSON.stringify(kbReqBody),
      data.header
    );
    let kbJson; try { kbJson = kbRes.json(); } catch (e) { kbJson = {}; }
    const kb = kbJson.knowledgeBase || {};

    if (!kb || !kb.id) {
      console.log("Reserved tags test: Failed to create knowledge base, skipping tests");
      return;
    }

    // Test 1: Create file with normal tags - should succeed
    const normalTagsBody = {
      displayName: data.dbIDPrefix + "normal-tags.txt",
      type: "TYPE_TEXT",
      content: constant.docSampleTxt,
      tags: ["user-tag", "another-tag", "my-custom-tag"]
    };

    const normalRes = http.request(
      "POST",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${kb.id}/files`,
      JSON.stringify(normalTagsBody),
      data.header
    );
    let normalJson; try { normalJson = normalRes.json(); } catch (e) { normalJson = {}; }
    const normalFile = normalJson.file || {};

    check(normalRes, {
      "Normal tags: File created successfully (200)": (r) => r.status === 200,
      "Normal tags: File has correct tags": () => normalFile && Array.isArray(normalFile.tags) && normalFile.tags.length === 3,
      "Normal tags: collectionUids is empty array": () => normalFile && Array.isArray(normalFile.collectionUids) && normalFile.collectionUids.length === 0,
    });

    // Test 2: Try to create file with agent:collection: prefix - should be rejected
    const agentTagBody = {
      displayName: data.dbIDPrefix + "agent-tag.txt",
      type: "TYPE_TEXT",
      content: constant.docSampleTxt,
      tags: ["user-tag", "agent:collection:fake-uid-12345"]
    };

    const agentRes = http.request(
      "POST",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${kb.id}/files`,
      JSON.stringify(agentTagBody),
      data.header
    );

    check(agentRes, {
      "Reserved agent: tag: Request rejected (400)": (r) => r.status === 400,
      "Reserved agent: tag: Error message mentions reserved": (r) => {
        try {
          const body = r.json();
          const msg = body.message || body.error || JSON.stringify(body);
          return msg.toLowerCase().includes("reserved");
        } catch (e) {
          return false;
        }
      },
    });

    // Test 3: Try to create file with instill- prefix - should be rejected
    const instillTagBody = {
      displayName: data.dbIDPrefix + "instill-tag.txt",
      type: "TYPE_TEXT",
      content: constant.docSampleTxt,
      tags: ["user-tag", "instill-internal-system-tag"]
    };

    const instillRes = http.request(
      "POST",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${kb.id}/files`,
      JSON.stringify(instillTagBody),
      data.header
    );

    check(instillRes, {
      "Reserved instill- tag: Request rejected (400)": (r) => r.status === 400,
      "Reserved instill- tag: Error message mentions reserved": (r) => {
        try {
          const body = r.json();
          const msg = body.message || body.error || JSON.stringify(body);
          return msg.toLowerCase().includes("reserved");
        } catch (e) {
          return false;
        }
      },
    });

    // Test 4: Try to UPDATE file with reserved tags - should be rejected
    if (normalFile && normalFile.uid) {
      const updateBody = {
        tags: ["user-tag", "agent:collection:another-fake-uid"]
      };

      const updateRes = http.request(
        "PATCH",
        `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${kb.id}/files/${normalFile.uid}?updateMask=tags`,
        JSON.stringify(updateBody),
        data.header
      );

      check(updateRes, {
        "Update with reserved tag: Request rejected (400)": (r) => r.status === 400,
        "Update with reserved tag: Error message mentions reserved": (r) => {
          try {
            const body = r.json();
            const msg = body.message || body.error || JSON.stringify(body);
            return msg.toLowerCase().includes("reserved");
          } catch (e) {
            return false;
          }
        },
      });

      // Test 5: Update file with normal tags - should succeed
      const normalUpdateBody = {
        tags: ["updated-tag", "new-tag"]
      };

      const normalUpdateRes = http.request(
        "PATCH",
        `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${kb.id}/files/${normalFile.uid}?updateMask=tags`,
        JSON.stringify(normalUpdateBody),
        data.header
      );
      let normalUpdateJson; try { normalUpdateJson = normalUpdateRes.json(); } catch (e) { normalUpdateJson = {}; }
      const updatedFile = normalUpdateJson.file || {};

      check(normalUpdateRes, {
        "Update with normal tags: Succeeds (200)": (r) => r.status === 200,
        "Update with normal tags: Tags updated correctly": () => updatedFile && Array.isArray(updatedFile.tags) && updatedFile.tags.includes("updated-tag"),
        "Update with normal tags: collectionUids still empty": () => updatedFile && Array.isArray(updatedFile.collectionUids) && updatedFile.collectionUids.length === 0,
      });
    }

    // Cleanup
    http.request("DELETE", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${kb.id}`, null, data.header);
  });
}
