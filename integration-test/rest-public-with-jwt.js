import http from "k6/http";
import { check, group, sleep } from "k6";
import { randomString } from "https://jslib.k6.io/k6-utils/1.1.0/index.js";

import { artifactRESTPublicHost } from "./const.js";

import * as constant from "./const.js";
import * as helper from "./helper.js";

const dbIDPrefix = constant.generateDBIDPrefix();

export function setup() {
  // Stagger test execution to reduce parallel resource contention
  helper.staggerTestExecution(2);

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
    headers: {
      "Authorization": `Bearer ${loginResp.json().accessToken}`
    }
  })

  return {
    header: header,
    expectedOwner: resp.json().user
  }
}

export function teardown(data) {
  // Cleanup: Remove any test catalogs created during JWT tests
  console.log("\n=== TEARDOWN: Cleaning up JWT test catalogs ===");
  try {
    const listResp = http.request("GET", `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`, null, data.header);
    if (listResp.status === 200) {
      const catalogs = Array.isArray(listResp.json().catalogs) ? listResp.json().catalogs : [];
      let cleanedCount = 0;
      for (const catalog of catalogs) {
        const catId = catalog.catalogId || catalog.catalog_id;
        // Clean up catalogs with jwt prefix or dbIDPrefix
        if (catId && (catId.includes(dbIDPrefix) || catId.includes("jwt-"))) {
          const delResp = http.request("DELETE", `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catId}`, null, data.header);
          if (delResp.status === 200 || delResp.status === 204) {
            cleanedCount++;
          }
        }
      }
      console.log(`Cleaned ${cleanedCount} JWT test catalogs`);
    }
  } catch (e) {
    console.log(`Teardown cleanup warning: ${e}`);
  }
  console.log("=== TEARDOWN: JWT tests cleanup complete ===\n");
}

function logUnexpected(res, label) {
  if (!res || res.status === 401 || res.status === 403) return;
  try {
    console.log(`${label} unexpected status=${res.status} body=${JSON.stringify(res.json())}`);
  } catch (e) {
    console.log(`${label} unexpected status=${res.status}`);
  }
}

// Helpers to create resources with authorized header, then test unauthorized access
function createCatalogAuthenticated(data) {
  const name = dbIDPrefix + "jwt-" + randomString(8);
  const res = http.request(
    "POST",
    `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`,
    JSON.stringify({ name }),
    data.header
  );
  if (res.status !== 200) {
    try { console.log(`Authorized create catalog failed status=${res.status} body=${JSON.stringify(res.json())}`); } catch (e) { console.log(`Authorized create catalog failed status=${res.status}`); }
  }
  try {
    const json = res.json();
    const cat = (json && json.catalog) || {};
    return { catalogId: cat.catalogId || name, namespaceId: data.expectedOwner.id };
  } catch (e) {
    return { catalogId: name, namespaceId: data.expectedOwner };
  }
}

function deleteCatalogAuthenticated(data, catalogId) {
  http.request("DELETE", `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}`, null, data.header);
}

function createFileAuthenticated(data, catalogId) {
  const fileName = dbIDPrefix + "jwt-file-" + randomString(6) + ".txt";
  const body = { name: fileName, type: "TYPE_TEXT", content: constant.sampleTxt };
  const res = http.request(
    "POST",
    `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files`,
    JSON.stringify(body),
    data.header
  );
  if (res.status !== 200) {
    try { console.log(`Authorized create file failed status=${res.status} body=${JSON.stringify(res.json())}`); } catch (e) { console.log(`Authorized create file failed status=${res.status}`); }
  }
  try {
    const json = res.json();
    const f = (json && json.file) || {};
    return f.fileUid || "";
  } catch (e) {
    return "";
  }
}

export function CheckCreateCatalogUnauthenticated(data) {
  const groupName = "Artifact API [JWT]: Create catalog rejects random user";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    const body = { name: dbIDPrefix + randomString(8) };
    const res = http.request("POST", `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`, JSON.stringify(body), constant.paramsHTTPWithJWT.headers);
    logUnexpected(res, "POST /v1alpha/namespaces/{namespace_id}/catalogs");
    check(res, { "POST /v1alpha/namespaces/{namespace_id}/catalogs 401": (r) => r.status === 401 });
  });
}

export function CheckListCatalogsUnauthenticated(data) {
  const groupName = "Artifact API [JWT]: List catalogs rejects random user";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    // Ensure at least one catalog exists, then attempt unauthorized list
    const created = createCatalogAuthenticated(data);
    const res = http.request("GET", `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`, null, constant.paramsHTTPWithJWT.headers);
    logUnexpected(res, "GET /v1alpha/namespaces/{namespace_id}/catalogs");
    check(res, { "GET /v1alpha/namespaces/{namespace_id}/catalogs 401": (r) => r.status === 401 });

    // Cleanup
    deleteCatalogAuthenticated(data, created.catalogId);
  });
}

export function CheckGetCatalogUnauthenticated(data) {
  const groupName = "Artifact API [JWT]: Get catalogs rejects random user";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    // Create one catalog, then try to list (as a proxy for get)
    const created = createCatalogAuthenticated(data);
    const res = http.request("GET", `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`, null, constant.paramsHTTPWithJWT.headers);
    logUnexpected(res, "GET /v1alpha/namespaces/{namespace_id}/catalogs");
    check(res, { "GET /v1alpha/namespaces/{namespace_id}/catalogs 401": (r) => r.status === 401 });

    // Cleanup
    deleteCatalogAuthenticated(data, created.catalogId);
  });
}

export function CheckUpdateCatalogUnauthenticated(data) {
  const groupName = "Artifact API [JWT]: Update catalog rejects random user";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    // Create catalog with authorized header, then try to update with random user
    const created = createCatalogAuthenticated(data);
    const body = { description: "x" };
    const res = http.request("PUT", `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${created.catalogId}`, JSON.stringify(body), constant.paramsHTTPWithJWT.headers);
    logUnexpected(res, "PUT /v1alpha/namespaces/{namespace_id}/catalogs/{id}");
    check(res, { "PUT /v1alpha/namespaces/{namespace_id}/catalogs/{id} 401": (r) => r.status === 401 });

    // Cleanup
    deleteCatalogAuthenticated(data, created.catalogId);
  });
}

export function CheckCreateFileUnauthenticated(data) {
  const groupName = "Artifact API [JWT]: Create file rejects random user";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    // Create catalog with authorized header, then try to create file with random user
    const created = createCatalogAuthenticated(data);
    const body = { name: dbIDPrefix + "x.txt", type: "TYPE_TEXT", content: constant.sampleTxt };
    const res = http.request("POST", `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${created.catalogId}/files`, JSON.stringify(body), constant.paramsHTTPWithJWT.headers);
    logUnexpected(res, "POST /v1alpha/namespaces/{namespace_id}/catalogs/{id}/files");
    check(res, { "POST /v1alpha/namespaces/{namespace_id}/catalogs/{id}/files 401": (r) => r.status === 401 });

    // Cleanup
    deleteCatalogAuthenticated(data, created.catalogId);
  });
}

export function CheckListFilesUnauthenticated(data) {
  const groupName = "Artifact API [JWT]: List files rejects random user";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    // Create catalog and at least one file authorized, then list with random user
    const created = createCatalogAuthenticated(data);
    createFileAuthenticated(data, created.catalogId);
    const res = http.request("GET", `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${created.catalogId}/files`, null, constant.paramsHTTPWithJWT.headers);
    logUnexpected(res, "GET /v1alpha/namespaces/{namespace_id}/catalogs/{id}/files");
    check(res, { "GET /v1alpha/namespaces/{namespace_id}/catalogs/{id}/files 401": (r) => r.status === 401 });

    // Cleanup
    deleteCatalogAuthenticated(data, created.catalogId);
  });
}

export function CheckProcessFilesUnauthorized(data) {
  const groupName = "Artifact API [JWT]: Process files rejects random user";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    // Create catalog and file authorized, then try to trigger processing as unauthorized user
    // Note: Processing auto-triggers on upload, but this test verifies that manual
    // triggering via ProcessCatalogFiles API is still rejected for unauthorized users
    const created = createCatalogAuthenticated(data);
    const fileUid = createFileAuthenticated(data, created.catalogId);
    const body = { fileUids: [fileUid] };

    // Try to call deprecated ProcessCatalogFiles API as unauthorized user (has user ID but no permission)
    const res = http.request(
      "POST",
      `${constant.artifactRESTPublicHost}/v1alpha/catalogs/files/processAsync`,
      JSON.stringify(body),
      constant.paramsHTTPWithJWT.headers
    );
    logUnexpected(res, "POST /v1alpha/catalogs/files/processAsync");
    check(res, { "POST /v1alpha/catalogs/files/processAsync 403": (r) => r.status === 403 });

    // Cleanup
    deleteCatalogAuthenticated(data, created.catalogId);
  });
}

export function CheckGetFileUnauthenticated(data) {
  const groupName = "Artifact API [JWT]: Get file rejects random user";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    // Create catalog and file authorized, then get file with random user
    const created = createCatalogAuthenticated(data);
    const fileUid = createFileAuthenticated(data, created.catalogId);
    const res = http.request("GET", `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${created.catalogId}/files/${fileUid}`, null, constant.paramsHTTPWithJWT.headers);
    logUnexpected(res, "GET /v1alpha/namespaces/{namespace_id}/catalogs/{id}/files/{uid}");
    check(res, { "GET /v1alpha/namespaces/{namespace_id}/catalogs/{id}/files/{uid} 401": (r) => r.status === 401 });

    // Cleanup
    deleteCatalogAuthenticated(data, created.catalogId);
  });
}

export function CheckGetFileSourceUnauthorized(data) {
  const groupName = "Artifact API [JWT]: Get file source rejects random user";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    // Create catalog and file authorized, then get source with random user
    const created = createCatalogAuthenticated(data);
    const fileUid = createFileAuthenticated(data, created.catalogId);
    const res = http.request("GET", `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${created.catalogId}/files/${fileUid}/source`, null, constant.paramsHTTPWithJWT.headers);
    logUnexpected(res, "GET /v1alpha/namespaces/{namespace_id}/catalogs/{id}/files/{uid}/source");
    check(res, { "GET /v1alpha/namespaces/{namespace_id}/catalogs/{id}/files/{uid}/source 403": (r) => r.status === 403 });

    // Cleanup
    deleteCatalogAuthenticated(data, created.catalogId);
  });
}

export function CheckGetFileSummaryUnauthenticated(data) {
  const groupName = "Artifact API [JWT]: Get file summary rejects random user";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    // Create catalog and file authorized, then get summary with random user
    const created = createCatalogAuthenticated(data);
    const fileUid = createFileAuthenticated(data, created.catalogId);
    const res = http.request("GET", `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${created.catalogId}/files/${fileUid}/summary`, null, constant.paramsHTTPWithJWT.headers);
    logUnexpected(res, "GET /v1alpha/namespaces/{namespace_id}/catalogs/{id}/files/{uid}/summary");
    check(res, { "GET /v1alpha/namespaces/{namespace_id}/catalogs/{id}/files/{uid}/summary 401": (r) => r.status === 401 });

    // Cleanup
    deleteCatalogAuthenticated(data, created.catalogId);
  });
}

export function CheckListChunksUnauthenticated(data) {
  const groupName = "Artifact API [JWT]: List chunks rejects random user";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    // Create catalog and a file with authorized header, then list chunks with random user using file_uid
    const created = createCatalogAuthenticated(data);
    const fileUid = createFileAuthenticated(data, created.catalogId);
    const res = http.request("GET", `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${created.catalogId}/chunks?fileUid=${fileUid}`, null, constant.paramsHTTPWithJWT.headers);
    logUnexpected(res, "GET /v1alpha/namespaces/{namespace_id}/catalogs/{id}/chunks");
    check(res, { "GET /v1alpha/namespaces/{namespace_id}/catalogs/{id}/chunks 401": (r) => r.status === 401 });

    // Cleanup
    deleteCatalogAuthenticated(data, created.catalogId);
  });
}
