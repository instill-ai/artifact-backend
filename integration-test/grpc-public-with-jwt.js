import grpc from "k6/net/grpc";
import http from "k6/http";
import { check, group } from "k6";
import { randomString } from "https://jslib.k6.io/k6-utils/1.1.0/index.js";

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
    metadata: { Authorization: `Bearer ${loginResp.json().accessToken}` },
    expectedOwner: resp.json().user
  }
}

export function teardown(data) {
  // Cleanup: Remove catalogs created by gRPC JWT tests
  console.log("\n=== TEARDOWN: Cleaning up gRPC JWT test catalogs ===");
  try {
    const listResp = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`, null, data.header);
    if (listResp.status === 200) {
      const catalogs = Array.isArray(listResp.json().catalogs) ? listResp.json().catalogs : [];
      let cleanedCount = 0;
      for (const catalog of catalogs) {
        const catId = catalog.catalogId || catalog.catalog_id;
        // Clean up catalogs with dbIDPrefix
        if (catId && catId.includes(dbIDPrefix)) {
          const delResp = http.request("DELETE", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catId}`, null, data.header);
          if (delResp.status === 200 || delResp.status === 204) {
            cleanedCount++;
          }
        }
      }
      console.log(`Cleaned ${cleanedCount} gRPC JWT test catalogs`);
    }
  } catch (e) {
    console.log(`Teardown cleanup warning: ${e}`);
  }
  console.log("=== TEARDOWN: gRPC JWT tests cleanup complete ===\n");
}

export function CheckUploadCatalogFile(client, data) {
  const groupName = "Artifact API [gRPC/JWT]: Upload file rejects random user (Unauthenticated)";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    // Create catalog with authorized metadata
    const cRes = client.invoke("artifact.artifact.v1alpha.ArtifactPublicService/CreateCatalog", { namespaceId: data.expectedOwner.id, name: dbIDPrefix + randomString(10), description: randomString(30), tags: ["test", "integration", "grpc"], type: "CATALOG_TYPE_PERSISTENT" }, data.metadata);
    const catalog = cRes.message.catalog;

    const reqBody = { namespaceId: data.expectedOwner.id, catalogId: catalog.catalogId, file: { name: dbIDPrefix + "test-file-grpc-jwt-" + randomString(5) + ".docx", type: "TYPE_DOCX", content: constant.sampleDocx } };
    // Invoke with invalid Authorization metadata → expect Unauthenticated/PermissionDenied
    const resNeg = client.invoke("artifact.artifact.v1alpha.ArtifactPublicService/UploadCatalogFile", reqBody, constant.paramsGRPCWithJwt);
    check(resNeg, {
      "UploadCatalogFile unauthenticated/denied": (r) => r.status === grpc.StatusPermissionDenied,
    });

    // Cleanup (authorized)
    // no file created in negative call; just delete catalog
    client.invoke("artifact.artifact.v1alpha.ArtifactPublicService/DeleteCatalog", { namespaceId: data.expectedOwner.id, catalogId: catalog.catalogId }, data.metadata);
  });
}

export function CheckListCatalogFiles(client, data) {
  const groupName = "Artifact API [gRPC/JWT]: List files rejects random user (Unauthenticated)";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    // Create resources with authorized metadata
    const cRes = client.invoke("artifact.artifact.v1alpha.ArtifactPublicService/CreateCatalog", { namespaceId: data.expectedOwner.id, name: dbIDPrefix + randomString(10), description: randomString(30), tags: ["test", "integration", "grpc"], type: "CATALOG_TYPE_PERSISTENT" }, data.metadata);
    const catalog = cRes.message.catalog;
    client.invoke("artifact.artifact.v1alpha.ArtifactPublicService/UploadCatalogFile", { namespaceId: data.expectedOwner.id, catalogId: catalog.catalogId, file: { name: dbIDPrefix + "test-file-grpc-jwt-" + randomString(5) + ".docx", type: "TYPE_DOCX", content: constant.sampleDocx } }, data.metadata);

    // Negative: list with invalid Authorization
    const resNeg = client.invoke("artifact.artifact.v1alpha.ArtifactPublicService/ListCatalogFiles", { namespaceId: data.expectedOwner.id, catalogId: catalog.catalogId, pageSize: 10 }, constant.paramsGRPCWithJwt);
    check(resNeg, { "artifact.artifact.v1alpha.ArtifactPublicService/ListCatalogFiles unauthenticated/denied": (r) => r.status === grpc.StatusPermissionDenied });

    // Cleanup
    // Delete catalog (files cascade or ignore failure)
    client.invoke("artifact.artifact.v1alpha.ArtifactPublicService/DeleteCatalog", { namespaceId: data.expectedOwner.id, catalogId: catalog.catalogId }, data.metadata);
  });
}

export function CheckGetCatalogFile(client, data) {
  const groupName = "Artifact API [gRPC/JWT]: Get file rejects random user (Unauthenticated)";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    const cRes = client.invoke("artifact.artifact.v1alpha.ArtifactPublicService/CreateCatalog", { namespaceId: data.expectedOwner.id, name: dbIDPrefix + randomString(10), description: randomString(30), tags: ["test", "integration", "grpc"], type: "CATALOG_TYPE_PERSISTENT" }, data.metadata);
    const catalog = cRes.message.catalog;
    const fRes = client.invoke("artifact.artifact.v1alpha.ArtifactPublicService/UploadCatalogFile", { namespaceId: data.expectedOwner.id, catalogId: catalog.catalogId, file: { name: dbIDPrefix + "test-file-grpc-jwt-" + randomString(5) + ".docx", type: "TYPE_DOCX", content: constant.sampleDocx } }, data.metadata);
    const file = fRes.message.file;

    // Negative: get file with invalid Authorization
    const resNeg = client.invoke("artifact.artifact.v1alpha.ArtifactPublicService/GetCatalogFile", { namespaceId: data.expectedOwner.id, catalogId: catalog.catalogId, fileUid: file.fileUid }, constant.paramsGRPCWithJwt);
    check(resNeg, { "artifact.artifact.v1alpha.ArtifactPublicService/GetCatalogFile unauthenticated/denied": (r) => r.status === grpc.StatusPermissionDenied });

    // Cleanup (authorized)
    client.invoke("artifact.artifact.v1alpha.ArtifactPublicService/DeleteCatalogFile", { fileUid: file.fileUid }, data.metadata);
    client.invoke("artifact.artifact.v1alpha.ArtifactPublicService/DeleteCatalog", { namespaceId: data.expectedOwner.id, catalogId: catalog.catalogId }, data.metadata);
  });
}
