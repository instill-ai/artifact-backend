import http from "k6/http";
import { check, group, sleep } from "k6";
import { randomString } from "https://jslib.k6.io/k6-utils/1.1.0/index.js";

import { artifactPublicHost } from "./const.js";

import * as constant from "./const.js";

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
  const name = constant.dbIDPrefix + "jwt-" + randomString(8);
  const res = http.request(
    "POST",
    `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`,
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
  http.request("DELETE", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}`, null, data.header);
}

function createFileAuthenticated(data, catalogId) {
  const fileName = constant.dbIDPrefix + "jwt-file-" + randomString(6) + ".txt";
  const body = { name: fileName, type: "TYPE_TEXT", content: constant.sampleTxt };
  const res = http.request(
    "POST",
    `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files`,
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

    const body = { name: constant.dbIDPrefix + randomString(8) };
    const res = http.request("POST", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`, JSON.stringify(body), constant.paramsHTTPWithJWT.headers);
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
    const res = http.request("GET", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`, null, constant.paramsHTTPWithJWT.headers);
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
    const res = http.request("GET", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`, null, constant.paramsHTTPWithJWT.headers);
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
    const res = http.request("PUT", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${created.catalogId}`, JSON.stringify(body), constant.paramsHTTPWithJWT.headers);
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
    const body = { name: constant.dbIDPrefix + "x.txt", type: "TYPE_TEXT", content: constant.sampleTxt };
    const res = http.request("POST", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${created.catalogId}/files`, JSON.stringify(body), constant.paramsHTTPWithJWT.headers);
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
    const res = http.request("GET", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${created.catalogId}/files`, null, constant.paramsHTTPWithJWT.headers);
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

    // Create catalog and file authorized, then process with random user
    const created = createCatalogAuthenticated(data);
    const fileUid = createFileAuthenticated(data, created.catalogId);
    const body = { fileUids: [fileUid] };
    const res = http.request("POST", `${artifactPublicHost}/v1alpha/catalogs/files/processAsync`, JSON.stringify(body), constant.paramsHTTPWithJWT.headers);
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
    const res = http.request("GET", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${created.catalogId}/files/${fileUid}`, null, constant.paramsHTTPWithJWT.headers);
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
    const res = http.request("GET", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${created.catalogId}/files/${fileUid}/source`, null, constant.paramsHTTPWithJWT.headers);
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
    const res = http.request("GET", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${created.catalogId}/files/${fileUid}/summary`, null, constant.paramsHTTPWithJWT.headers);
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
    const res = http.request("GET", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${created.catalogId}/chunks?fileUid=${fileUid}`, null, constant.paramsHTTPWithJWT.headers);
    logUnexpected(res, "GET /v1alpha/namespaces/{namespace_id}/catalogs/{id}/chunks");
    check(res, { "GET /v1alpha/namespaces/{namespace_id}/catalogs/{id}/chunks 401": (r) => r.status === 401 });

    // Cleanup
    deleteCatalogAuthenticated(data, created.catalogId);
  });
}
