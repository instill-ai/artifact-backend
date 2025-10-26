import grpc from "k6/net/grpc";
import http from "k6/http";
import { check, group } from "k6";
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
  // Cleanup: Remove any test catalogs created (private API tests may create test data)
  console.log("\n=== TEARDOWN: Cleaning up gRPC private test data ===");
  try {
    // Clean up any catalogs with dbIDPrefix
    const listResp = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`, null, data.header);
    if (listResp.status === 200) {
      const catalogs = Array.isArray(listResp.json().catalogs) ? listResp.json().catalogs : [];
      let cleanedCount = 0;
      for (const catalog of catalogs) {
        const catId = catalog.catalogId || catalog.catalog_id;
        if (catId && catId.includes(dbIDPrefix)) {
          const delResp = http.request("DELETE", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catId}`, null, data.header);
          if (delResp.status === 200 || delResp.status === 204) {
            cleanedCount++;
          }
        }
      }
      if (cleanedCount > 0) {
        console.log(`Cleaned ${cleanedCount} test catalogs`);
      }
    }

    // Note: Repository tags are ephemeral test data created with timestamp-based IDs
    // They are cleaned up within each test function via DeleteRepositoryTagAdmin
    console.log("Repository tags are cleaned up inline by test functions");
  } catch (e) {
    console.log(`Teardown cleanup warning: ${e}`);
  }
  console.log("=== TEARDOWN: gRPC private tests cleanup complete ===\n");
}

export function CheckListRepositoryTags(client, data) {
  const groupName = "Artifact API: List repository tags (private gRPC)";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    var resOrigin = client.invoke(
      "artifact.artifact.v1alpha.ArtifactPrivateService/ListRepositoryTagsAdmin",
      {
        parent: `repositories/${data.expectedOwner.id}`,
        pageSize: 10
      },
      data.metadata
    );
    check(resOrigin, {
      "artifact.artifact.v1alpha.ArtifactPrivateService/ListRepositoryTagsAdmin response status is StatusOK": (r) => r.status === grpc.StatusOK,
      "artifact.artifact.v1alpha.ArtifactPrivateService/ListRepositoryTagsAdmin response tags is array": (r) =>
        Array.isArray(r.message.tags),
    });
  });
}

export function CheckGetRepositoryTag(client, data) {
  const groupName = "Artifact API: Get repository tag (private gRPC)";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    const tagName = constant.testRepositoryTagName;
    if (!tagName) {
      check(true, { "skipped: testRepositoryTagName not provided": () => true });
      return;
    }

    var res = client.invoke(
      "artifact.artifact.v1alpha.ArtifactPrivateService/GetRepositoryTagAdmin",
      {
        name: tagName,
      },
      data.metadata
    );
    check(res, {
      "GetRepositoryTag returns StatusOK": (r) => r.status === grpc.StatusOK,
      "GetRepositoryTag returns tag": (r) => !!r.message && !!r.message.tag,
    });
  });
}

export function CheckCreateAndDeleteRepositoryTag(client, data) {
  const groupName = "Artifact API: Create and delete repository tag (private gRPC)";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    const repoId = constant.testRepositoryId || `${data.expectedOwner.id}/test-repo`;
    const tagId = `${dbIDPrefix}tag-${Date.now()}`;
    const tagName = `repositories/${repoId}/tags/${tagId}`;

    var createRes = client.invoke(
      "artifact.artifact.v1alpha.ArtifactPrivateService/CreateRepositoryTagAdmin",
      {
        tag: {
          name: tagName,
          id: tagId,
        },
      },
      data.metadata
    );
    check(createRes, {
      "CreateRepositoryTag returns a response": (r) => r.status === grpc.StatusOK,
    });

    var deleteRes = client.invoke(
      "artifact.artifact.v1alpha.ArtifactPrivateService/DeleteRepositoryTagAdmin",
      {
        name: tagName,
      },
      data.metadata
    );
    check(deleteRes, {
      "DeleteRepositoryTag returns a response": (r) => r.status === grpc.StatusOK,
    });
  });
}

export function CheckGetObject(client, data) {
  const groupName = "Artifact API: Get Object (private gRPC)";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });
    const uid = constant.testObjectUid;
    if (!uid) {
      check(true, { "skipped: testObjectUid not provided": () => true });
      return;
    }
    var res = client.invoke(
      "artifact.artifact.v1alpha.ArtifactPrivateService/GetObjectAdmin",
      { uid: uid },
      data.metadata
    );
    check(res, {
      "GetObject returns StatusOK": (r) => r.status === grpc.StatusOK,
      "GetObject returns object": (r) => !!r.message && !!r.message.object,
    });
  });
}

export function CheckGetObjectURL(client, data) {
  const groupName = "Artifact API: Get Object URL (private gRPC)";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });
    const uid = constant.testObjectUrlUid;
    const encoded = constant.testEncodedUrlPath;
    if (!uid && !encoded) {
      check(true, { "skipped: testObjectUrlUid or testEncodedUrlPath not provided": () => true });
      return;
    }
    var res = client.invoke(
      "artifact.artifact.v1alpha.ArtifactPrivateService/GetObjectURLAdmin",
      uid ? { uid: uid } : { encodedUrlPath: encoded },
      data.metadata
    );
    check(res, {
      "GetObjectURL returns a response": (r) => r && typeof r.status === grpc.StatusOK,
    });
  });
}

export function CheckUpdateObject(client, data) {
  const groupName = "Artifact API: Update Object (private gRPC)";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });
    const uid = constant.testObjectUidToUpdate;
    if (!uid) {
      check(true, { "skipped: testObjectUidToUpdate not provided": () => true });
      return;
    }
    var res = client.invoke(
      "artifact.artifact.v1alpha.ArtifactPrivateService/UpdateObjectAdmin",
      { uid: uid, isUploaded: true },
      data.metadata
    );
    check(res, {
      "UpdateObject returns a response": (r) => r && typeof r.status === grpc.StatusOK,
    });
  });
}

export function CheckGetFileAsMarkdown(client, data) {
  const groupName = "Artifact API: Get file as Markdown (private gRPC)";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });
    const fileUid = constant.testFileUidForMarkdown;
    if (!fileUid) {
      check(true, { "skipped: testFileUidForMarkdown not provided": () => true });
      return;
    }
    var res = client.invoke(
      "artifact.artifact.v1alpha.ArtifactPrivateService/GetFileAsMarkdownAdmin",
      { fileUid: fileUid },
      data.metadata
    );
    check(res, {
      "GetFileAsMarkdown returns StatusOK": (r) => r.status === grpc.StatusOK,
      "GetFileAsMarkdown returns markdown": (r) => !!r.message && typeof r.message.markdown === "string",
    });
  });
}
