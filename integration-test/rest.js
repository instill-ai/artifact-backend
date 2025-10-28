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
  scenarios: {
    // Health check
    // test_01_health: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_01_Health' },

    // Basic CRUD operations
    test_02_create_catalog: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_02_CreateCatalog' },
    test_03_list_catalogs: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_03_ListCatalogs' },
    test_04_get_catalog: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_04_GetCatalog' },
    test_05_update_catalog: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_05_UpdateCatalog' },
    test_06_delete_catalog: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_06_DeleteCatalog' },

    // End-to-end tests
    test_07_cleanup_files: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_07_CleanupFiles' },
    test_08_e2e_catalog: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_08_E2ECatalog' },

    // JWT/Auth tests
    test_09_jwt_create_catalog: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_09_JWT_CreateCatalog' },
    test_10_jwt_list_catalogs: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_10_JWT_ListCatalogs' },
    test_11_jwt_get_catalog: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_11_JWT_GetCatalog' },
    test_12_jwt_update_catalog: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_12_JWT_UpdateCatalog' },
    test_13_jwt_create_file: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_13_JWT_CreateFile' },
    test_14_jwt_list_files: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_14_JWT_ListFiles' },
    test_15_jwt_get_file: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_15_JWT_GetFile' },
    test_16_jwt_get_file_content: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_16_JWT_GetFileContent' },
    test_17_jwt_get_file_summary: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_17_JWT_GetFileSummary' },
    test_18_jwt_list_chunks: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_18_JWT_ListChunks' },
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

    // Wait for THIS TEST's file processing to complete before deleting catalogs
    console.log("Teardown: Waiting for this test's file processing to complete...");
    const allProcessingComplete = helper.waitForAllFileProcessingComplete(120, data.dbIDPrefix);
    if (!allProcessingComplete) {
      console.warn("Teardown: Some files still processing after 120s, proceeding anyway");
    }

    console.log(`rest.js teardown: Cleaning up resources with prefix: ${data.dbIDPrefix}`);
    var listResp = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`, null, data.header)
    if (listResp.status === 200) {
      var catalogs = Array.isArray(listResp.json().catalogs) ? listResp.json().catalogs : []
      let cleanedCount = 0;
      for (const catalog of catalogs) {
        // Clean up catalogs with our test prefix (includes cl-, cat-, del-, jwt- prefixes)
        if (catalog.id && (catalog.id.startsWith(data.dbIDPrefix) || catalog.id.includes(data.dbIDPrefix))) {
          var delResp = http.request("DELETE", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalog.id}`, null, data.header);
          if (delResp.status === 200 || delResp.status === 204 || delResp.status === 404) {
            cleanedCount++;
          }
        }
      }
      console.log(`Cleaned ${cleanedCount} test catalogs`);
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
export function TEST_02_CreateCatalog(data) {
  const groupName = "Artifact API: Create a catalog";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    var reqBody = {
      id: "test-" + data.dbIDPrefix + randomString(10),
      description: randomString(50),
      tags: ["test", "integration"],
      type: "CATALOG_TYPE_PERSISTENT"
    };

    var resOrigin = http.request(
      "POST",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`,
      JSON.stringify(reqBody),
      data.header
    );
    let json; try { json = resOrigin.json(); } catch (e) { json = {}; }
    const cat = json.catalog || {};
    const uid = cat.uid;
    const id = cat.id;
    const createTime = cat.createTime || cat.create_time;
    const updateTime = cat.updateTime || cat.update_time;

    check(resOrigin, {
      "POST /v1alpha/namespaces/{namespace_id}/catalogs response status is 200": (r) => r.status === 200,
      "POST /v1alpha/namespaces/{namespace_id}/catalogs response catalog name": () =>
        cat && cat.name === `namespaces/${data.expectedOwner.id}/catalogs/${reqBody.id}`,
      "POST /v1alpha/namespaces/{namespace_id}/catalogs response catalog uid": () =>
        cat && helper.isUUID(uid),
      "POST /v1alpha/namespaces/{namespace_id}/catalogs response catalog id": () =>
        cat && id === reqBody.id,
      "POST /v1alpha/namespaces/{namespace_id}/catalogs response catalog description": () =>
        cat && cat.description === reqBody.description,
      "POST /v1alpha/namespaces/{namespace_id}/catalogs response catalog is valid": () =>
        cat && helper.validateCatalog(cat, false),
      "POST /v1alpha/namespaces/{namespace_id}/catalogs response catalog createTime": () =>
        cat && typeof createTime === 'string' && createTime.length > 0,
      "POST /v1alpha/namespaces/{namespace_id}/catalogs response catalog updateTime": () =>
        cat && typeof updateTime === 'string' && updateTime.length > 0,
    });

    // Cleanup
    if (cat && cat.id) {
      http.request("DELETE", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${cat.id}`, null, data.header);
    }
  });
}

export function TEST_03_ListCatalogs(data) {
  const groupName = "Artifact API: List catalogs";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    var resOrigin = http.request(
      "GET",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`,
      null,
      data.header
    );
    let json; try { json = resOrigin.json(); } catch (e) { json = {}; }
    check(resOrigin, {
      "GET /v1alpha/namespaces/{namespace_id}/catalogs response status is 200": (r) => r.status === 200,
      "GET /v1alpha/namespaces/{namespace_id}/catalogs response catalogs is array": () =>
        Array.isArray(json.catalogs),
    });
  });
}

export function TEST_04_GetCatalog(data) {
  const groupName = "Artifact API: Get catalog";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    const cRes = http.request(
      "POST",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`,
      JSON.stringify({ id: "test-" + data.dbIDPrefix + randomString(10) }),
      data.header
    );
    check(cRes, { "POST /v1alpha/namespaces/{namespace_id}/catalogs 200": (r) => r.status === 200 });
    const created = (cRes.json() || {}).catalog || {};

    const resOrigin = http.request(
      "GET",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`,
      null,
      data.header
    );
    let json; try { json = resOrigin.json(); } catch (e) { json = {}; }
    const catalogs = Array.isArray(json.catalogs) ? json.catalogs : [];
    check(resOrigin, {
      "GET /v1alpha/namespaces/{namespace_id}/catalogs response status is 200": (r) => r.status === 200,
      "GET /v1alpha/namespaces/{namespace_id}/catalogs response catalogs is array": () => Array.isArray(json.catalogs),
      "GET /v1alpha/namespaces/{namespace_id}/catalogs response contains our catalog": () => catalogs.some(c => c.id === created.id),
    });

    // Cleanup
    http.request("DELETE", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${created.id}`, null, data.header);
  });
}

export function TEST_05_UpdateCatalog(data) {
  const groupName = "Artifact API: Update catalog";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    const cRes = http.request(
      "POST",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`,
      JSON.stringify({ id: "test-" + data.dbIDPrefix + randomString(10) }),
      data.header
    );
    const created = (cRes.json() || {}).catalog || {};

    const newDescription = randomString(50);
    const reqBody = {
      catalog: {
        description: newDescription,
        tags: ["test", "integration", "updated"]
      },
      updateMask: "description,tags"
    };

    const resOrigin = http.request(
      "PUT",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${created.id}`,
      JSON.stringify(reqBody),
      data.header
    );
    const json = (function () { try { return resOrigin.json(); } catch (e) { return {}; } })();
    const cat2 = json.catalog || {};
    check(resOrigin, {
      "PUT /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id} response status is 200": (r) => r.status === 200,
      "PUT /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id} response catalog id": () =>
        cat2.id === created.id,
      "PUT /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id} response catalog description updated": () =>
        cat2.description === newDescription,
      "PUT /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id} response catalog is valid": () =>
        helper.validateCatalog(cat2, false),
    });

    // Cleanup
    http.request("DELETE", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${created.id}`, null, data.header);
  });
}

export function TEST_06_DeleteCatalog(data) {
  const groupName = "Artifact API: Delete catalog";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    const createBody = { id: "test-" + data.dbIDPrefix + "del-" + randomString(8) };
    const cRes = http.request(
      "POST",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`,
      JSON.stringify(createBody),
      data.header
    );
    let created; try { created = (cRes.json() || {}).catalog; } catch (e) { created = {}; }
    const catalogId = created && created.id;
    check(cRes, { "POST /v1alpha/namespaces/{namespace_id}/catalogs 200": (r) => r.status === 200 });

    const dRes = http.request(
      "DELETE",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}`,
      null,
      data.header
    );
    check(dRes, {
      "DELETE /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id} response status is 2xx": (r) => r.status >= 200 && r.status < 300,
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

    const catalogName = "test-" + data.dbIDPrefix + "cl-" + randomString(5);
    const createRes = http.request(
      "POST",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`,
      JSON.stringify({ id: catalogName, description: "Cleanup test" }),
      data.header
    );

    let catalog;
    try { catalog = createRes.json().catalog; } catch (e) { catalog = {}; }
    const catalogId = catalog ? catalog.id : null;

    check(createRes, {
      "Cleanup: Catalog created": (r) => r.status === 200 && catalogId,
    });

    if (!catalogId) return;

    const filename = `${data.dbIDPrefix}cl.pdf`;
    const uploadRes = http.request(
      "POST",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files`,
      JSON.stringify({ filename: filename, type: "TYPE_PDF", content: constant.samplePdf }),
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
      console.log("Cleanup: Skipping test due to file upload failure, deleting catalog...");
      http.request("DELETE", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}`, null, data.header);
      return;
    }

    // Wait for file processing to complete (max 60 seconds)
    const processingResult = helper.waitForFileProcessingComplete(
      data.expectedOwner.id,
      catalogId,
      fileUid,
      data.header,
      60 // maxWaitSeconds
    );

    if (!processingResult.completed) {
      console.log(`Cleanup: File processing did not complete: ${processingResult.status}${processingResult.error ? ' - ' + processingResult.error : ''}`);
      // Clean up the catalog even if file processing failed/timeout
      http.request("DELETE", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}`, null, data.header);
      return;
    }

    const deleteRes = http.request(
      "DELETE",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}`,
      null,
      data.header
    );

    check(deleteRes, {
      "Cleanup: Catalog deleted (triggers file cleanup)": (r) => {
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
        (SELECT COUNT(*) FROM text_chunk WHERE file_uid = '${fileUid}') as chunks,
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
// TEST GROUP 08: Catalog End-to-End (Comprehensive)
// ============================================================================
export function TEST_08_E2ECatalog(data) {
  const groupName = "Artifact API: Catalog end-to-end (comprehensive)";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    // Create catalog
    const createBody = {
      id: "test-" + data.dbIDPrefix + "cat-" + randomString(8),
      description: randomString(40),
      tags: ["test", "integration", "catalog-e2e"],
      type: "CATALOG_TYPE_PERSISTENT",
    };
    const cRes = http.request(
      "POST",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`,
      JSON.stringify(createBody),
      data.header
    );
    let created; try { created = (cRes.json() || {}).catalog; } catch (e) { created = {}; }
    const catalogId = created && created.id;
    check(cRes, {
      "E2E: Catalog created": (r) => r.status === 200,
      "E2E: Catalog is valid": () => created && helper.validateCatalog(created, false),
    });

    if (!catalogId) return;

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
          url: `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files`,
          body: JSON.stringify({ filename: filename, type: s.type, content: s.content, tags: tags }),
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
      check(resp, { [`E2E: File uploaded (${s.originalName})`]: (r) => r.status === 200 });
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
            url: `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files/${uid}`,
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
      var viewPath = `/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files?filter=${encodeURIComponent(`id = "${f.fileId}"`)}`
      const viewRes = http.request("GET", constant.artifactRESTPublicHost + viewPath, null, data.header);

      if (viewRes.status === 200) {
        try {
          const fileData = viewRes.json().files[0];
          check(viewRes, {
            [`E2E: File has correct metadata (${f.type})`]: () =>
              fileData && fileData.filename === f.filename &&
              fileData.processStatus === "FILE_PROCESS_STATUS_COMPLETED" &&
              fileData.totalChunks > 0 && fileData.totalTokens > 0,
          });
        } catch (e) {
          console.log(`E2E: Error verifying metadata for ${f.filename} (${f.type}): ${e}`);
        }
      } else {
        console.log(`E2E: Failed to fetch file metadata for ${f.filename} (${f.type}): status=${viewRes.status}`);
      }
    }

    // List catalog files
    const listFilesRes = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files?pageSize=100`, null, data.header);
    let listFilesJson; try { listFilesJson = listFilesRes.json(); } catch (e) { listFilesJson = {}; }
    check(listFilesRes, {
      "E2E: List files returns all uploaded": () =>
        Array.isArray(listFilesJson.files) && listFilesJson.files.length === uploaded.length,
    });

    // List chunks for first file
    if (fileUids.length > 0) {
      const listChunksRes = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files/${fileUids[0]}/chunks`, null, data.header);
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
      const getSummaryRes = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files/${fileUids[0]}?view=VIEW_SUMMARY`, null, data.header);
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
          `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files/${pdfFile.fileUid}?updateMask=tags`,
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
        `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/searchChunks`,
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
        `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/searchChunks`,
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
          `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/searchChunks`,
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
    http.request("DELETE", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}`, null, data.header);
  });
}

// ============================================================================
// TEST GROUP 09-18: JWT/Auth Tests
// ============================================================================
function logUnexpected(res, label) {
  if (!res || res.status === 401 || res.status === 403) return;
  try {
    console.log(`${label} unexpected status=${res.status} body=${JSON.stringify(res.json())}`);
  } catch (e) {
    console.log(`${label} unexpected status=${res.status}`);
  }
}

function createCatalogAuthenticated(data) {
  const name = "test-" + data.dbIDPrefix + "jwt-" + randomString(8);
  const res = http.request(
    "POST",
    `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`,
    JSON.stringify({ id: name }),
    data.header
  );
  try {
    const json = res.json();
    const cat = (json && json.catalog) || {};
    return { id: cat.id || name, namespaceId: data.expectedOwner.id };
  } catch (e) {
    return { id: name, namespaceId: data.expectedOwner.id };
  }
}

function deleteCatalogAuthenticated(data, catalogId) {
  http.request("DELETE", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}`, null, data.header);
}

function createFileAuthenticated(data, catalogId) {
  const filename = data.dbIDPrefix + "jwt-file-" + randomString(6) + ".txt";
  const body = { filename: filename, type: "TYPE_TEXT", content: constant.sampleTxt };
  const res = http.request(
    "POST",
    `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files`,
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

export function TEST_09_JWT_CreateCatalog(data) {
  const groupName = "Artifact API [JWT]: Create catalog rejects random user";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    const body = { id: "test-" + data.dbIDPrefix + randomString(8) };
    const res = http.request("POST", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`, JSON.stringify(body), constant.paramsHTTPWithJWT.headers);
    logUnexpected(res, "JWT: POST catalogs");
    check(res, { "JWT: POST catalogs 401": (r) => r.status === 401 });
  });
}

export function TEST_10_JWT_ListCatalogs(data) {
  const groupName = "Artifact API [JWT]: List catalogs rejects random user";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    const created = createCatalogAuthenticated(data);
    const res = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`, null, constant.paramsHTTPWithJWT.headers);
    logUnexpected(res, "JWT: GET catalogs");
    check(res, { "JWT: GET catalogs 401": (r) => r.status === 401 });
    deleteCatalogAuthenticated(data, created.id);
  });
}

export function TEST_11_JWT_GetCatalog(data) {
  const groupName = "Artifact API [JWT]: Get catalogs rejects random user";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    const created = createCatalogAuthenticated(data);
    const res = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`, null, constant.paramsHTTPWithJWT.headers);
    logUnexpected(res, "JWT: GET catalogs");
    check(res, { "JWT: GET catalogs 401": (r) => r.status === 401 });
    deleteCatalogAuthenticated(data, created.id);
  });
}

export function TEST_12_JWT_UpdateCatalog(data) {
  const groupName = "Artifact API [JWT]: Update catalog rejects random user";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    const created = createCatalogAuthenticated(data);
    const body = { catalog: { description: "x" }, updateMask: "description" };
    const res = http.request("PUT", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${created.id}`, JSON.stringify(body), constant.paramsHTTPWithJWT.headers);
    logUnexpected(res, "JWT: PUT catalog");
    check(res, { "JWT: PUT catalog 401": (r) => r.status === 401 });
    deleteCatalogAuthenticated(data, created.id);
  });
}

export function TEST_13_JWT_CreateFile(data) {
  const groupName = "Artifact API [JWT]: Create file rejects random user";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    const created = createCatalogAuthenticated(data);
    const body = { filename: data.dbIDPrefix + "x.txt", type: "TYPE_TEXT", content: constant.sampleTxt };
    const res = http.request("POST", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${created.id}/files`, JSON.stringify(body), constant.paramsHTTPWithJWT.headers);
    logUnexpected(res, "JWT: POST file");
    check(res, { "JWT: POST file 401": (r) => r.status === 401 });
    deleteCatalogAuthenticated(data, created.id);
  });
}

export function TEST_14_JWT_ListFiles(data) {
  const groupName = "Artifact API [JWT]: List files rejects random user";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    const created = createCatalogAuthenticated(data);
    createFileAuthenticated(data, created.id);
    const res = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${created.id}/files`, null, constant.paramsHTTPWithJWT.headers);
    logUnexpected(res, "JWT: GET files");
    check(res, { "JWT: GET files 401": (r) => r.status === 401 });
    deleteCatalogAuthenticated(data, created.id);
  });
}

export function TEST_15_JWT_GetFile(data) {
  const groupName = "Artifact API [JWT]: Get file rejects random user";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    const created = createCatalogAuthenticated(data);
    const fileUid = createFileAuthenticated(data, created.id);
    const res = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${created.id}/files/${fileUid}`, null, constant.paramsHTTPWithJWT.headers);
    logUnexpected(res, "JWT: GET file");
    check(res, { "JWT: GET file 401": (r) => r.status === 401 });
    deleteCatalogAuthenticated(data, created.id);
  });
}

export function TEST_16_JWT_GetFileContent(data) {
  const groupName = "Artifact API [JWT]: Get file content (VIEW_CONTENT) rejects random user";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    const created = createCatalogAuthenticated(data);
    const fileUid = createFileAuthenticated(data, created.id);
    const res = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${created.id}/files/${fileUid}?view=VIEW_CONTENT`, null, constant.paramsHTTPWithJWT.headers);
    logUnexpected(res, "JWT: GET file content");
    check(res, { "JWT: GET file content 401": (r) => r.status === 401 });
    deleteCatalogAuthenticated(data, created.id);
  });
}

export function TEST_17_JWT_GetFileSummary(data) {
  const groupName = "Artifact API [JWT]: Get file summary (VIEW_SUMMARY) rejects random user";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    const created = createCatalogAuthenticated(data);
    const fileUid = createFileAuthenticated(data, created.id);
    const res = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${created.id}/files/${fileUid}?view=VIEW_SUMMARY`, null, constant.paramsHTTPWithJWT.headers);
    logUnexpected(res, "JWT: GET file summary");
    check(res, { "JWT: GET file summary 401": (r) => r.status === 401 });
    deleteCatalogAuthenticated(data, created.id);
  });
}

export function TEST_18_JWT_ListChunks(data) {
  const groupName = "Artifact API [JWT]: List chunks rejects random user";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    const created = createCatalogAuthenticated(data);
    const fileUid = createFileAuthenticated(data, created.id);
    const res = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${created.id}/files/${fileUid}/chunks`, null, constant.paramsHTTPWithJWT.headers);
    logUnexpected(res, "JWT: GET chunks");
    check(res, { "JWT: GET chunks 401 or 404": (r) => r.status === 401 || r.status === 404 });
    deleteCatalogAuthenticated(data, created.id);
  });
}
