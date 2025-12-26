import grpc from "k6/net/grpc";
import http from "k6/http";
import { check, group, sleep } from "k6";
import { randomString } from "https://jslib.k6.io/k6-utils/1.1.0/index.js";

import * as constant from "./const.js";
import * as helper from "./helper.js";

const dbIDPrefix = constant.generateDBIDPrefix();

// Initialize gRPC clients in init context (required by k6)
const publicClient = new grpc.Client();
const privateClient = new grpc.Client();

// Load proto files in init context (required by k6)
publicClient.load(["./proto", "./proto/artifact/artifact/v1alpha"], "artifact/artifact/v1alpha/artifact_public_service.proto");
privateClient.load(["./proto", "./proto/artifact/artifact/v1alpha"], "artifact/artifact/v1alpha/artifact_private_service.proto");

export let options = {
  setupTimeout: "10s",
  teardownTimeout: "180s",
  insecureSkipTLSVerify: true,
  thresholds: {
    checks: ["rate == 1.0"],
  },
  scenarios: {
    // Health checks
    test_01_health: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_01_Health' },

    // Public gRPC tests - Knowledge Bases
    test_02_create_knowledge_base: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_02_CreateKnowledgeBase' },
    test_03_list_knowledge_bases: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_03_ListKnowledgeBases' },
    test_04_get_knowledge_base: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_04_GetKnowledgeBase' },
    test_05_update_knowledge_base: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_05_UpdateKnowledgeBase' },
    test_06_delete_knowledge_base: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_06_DeleteKnowledgeBase' },

    // Public gRPC tests - Files
    test_07_upload_file: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_07_UploadFile' },
    test_08_list_files: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_08_ListFiles' },
    test_09_get_file: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_09_GetFile' },
    test_10_cleanup_on_delete: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_10_CleanupOnDelete' },

    // JWT/Auth tests
    test_11_jwt_upload_file: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_11_JWT_UploadFile' },
    test_12_jwt_list_files: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_12_JWT_ListFiles' },
    test_13_jwt_get_file: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_13_JWT_GetFile' },

    // Private gRPC tests (admin only)
    test_14_get_object: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_14_GetObject' },
    test_15_get_object_url: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_15_GetObjectURL' },
    test_16_update_object: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_16_UpdateObject' },
    test_17_create_kb_admin: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_17_CreateKnowledgeBaseAdmin' },
    test_18_update_kb_admin: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_18_UpdateKnowledgeBaseAdmin' },
    test_19_update_file_admin: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_19_UpdateFileAdmin' },
  },
};

export function setup() {
  check(true, { [constant.banner('Artifact API (gRPC): Setup')]: () => true });

  // Stagger test execution to reduce parallel resource contention
  helper.staggerTestExecution(2);

  console.log(`grpc.js: Using unique test prefix: ${dbIDPrefix}`);

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

  var grpcMetadata = {
    "metadata": {
      "Authorization": `Bearer ${loginResp.json().accessToken}`,
    },
  }

  var resp = http.request("GET", `${constant.mgmtRESTPublicHost}/v1beta/user`, {}, {
    headers: { "Authorization": `Bearer ${loginResp.json().accessToken}` }
  })

  return {
    header: header,
    metadata: grpcMetadata,
    expectedOwner: resp.json().user,
    dbIDPrefix: dbIDPrefix
  }
}

export function teardown(data) {
  const groupName = "Artifact API (gRPC): Teardown - Delete all test resources";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    // Wait for file processing AND Temporal activities to settle before cleanup
    console.log("Teardown: Waiting for safe cleanup...");
    helper.waitForSafeCleanup(120, data.dbIDPrefix, 3);

    console.log(`grpc.js teardown: Cleaning up resources with prefix: ${data.dbIDPrefix}`);
    var listResp = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases`, null, data.header)
    if (listResp.status === 200) {
      var knowledgeBases = Array.isArray(listResp.json().knowledgeBases) ? listResp.json().knowledgeBases : []
      let cleanedCount = 0;
      for (const kb of knowledgeBases) {
        // Clean up knowledge bases with our test prefix
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
  const groupName = "Artifact API: Health check (gRPC)";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    publicClient.connect(constant.artifactGRPCPublicHost, { plaintext: true });

    check(
      publicClient.invoke("artifact.artifact.v1alpha.ArtifactPublicService/Liveness", {}),
      { "Liveness response status is StatusOK": (r) => r.status === grpc.StatusOK }
    );
    check(
      publicClient.invoke("artifact.artifact.v1alpha.ArtifactPublicService/Readiness", {}),
      { "Readiness response status is StatusOK": (r) => r.status === grpc.StatusOK }
    );

    publicClient.close();
  });
}

// ============================================================================
// TEST GROUP 02-06: Knowledge Base Operations (Public gRPC)
// ============================================================================
export function TEST_02_CreateKnowledgeBase(data) {
  const groupName = "Artifact API: Create knowledge base (gRPC)";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    publicClient.connect(constant.artifactGRPCPublicHost, { plaintext: true });

    const id = "test-" + data.dbIDPrefix + randomString(10);
    const req = { namespaceId: data.expectedOwner.id, id, description: randomString(30), tags: ["test", "grpc"], type: "KNOWLEDGE_BASE_TYPE_PERSISTENT" };
    const res = publicClient.invoke("artifact.artifact.v1alpha.ArtifactPublicService/CreateKnowledgeBase", req, data.metadata);

    check(res, {
      "CreateKnowledgeBase response status is StatusOK": (r) => r.status === grpc.StatusOK,
      "CreateKnowledgeBase response knowledge base id matches": (r) => r.message && r.message.knowledgeBase && r.message.knowledgeBase.id === id,
    });

    // Cleanup
    if (res.message && res.message.knowledgeBase) {
      publicClient.invoke("artifact.artifact.v1alpha.ArtifactPublicService/DeleteKnowledgeBase", { namespaceId: data.expectedOwner.id, knowledgeBaseId: res.message.knowledgeBase.id }, data.metadata);
    }

    publicClient.close();
  });
}

export function TEST_03_ListKnowledgeBases(data) {
  const groupName = "Artifact API: List knowledge bases (gRPC)";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    publicClient.connect(constant.artifactGRPCPublicHost, { plaintext: true });

    const res = publicClient.invoke("artifact.artifact.v1alpha.ArtifactPublicService/ListKnowledgeBases", { namespaceId: data.expectedOwner.id }, data.metadata);
    check(res, {
      "ListKnowledgeBases response status is StatusOK": (r) => r.status === grpc.StatusOK,
      "ListKnowledgeBases response knowledge_bases is array": (r) => Array.isArray(r.message.knowledgeBases),
    });

    publicClient.close();
  });
}

export function TEST_04_GetKnowledgeBase(data) {
  const groupName = "Artifact API: Get knowledge base (gRPC)";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    publicClient.connect(constant.artifactGRPCPublicHost, { plaintext: true });

    const id = "test-" + data.dbIDPrefix + randomString(10);
    const cRes = publicClient.invoke("artifact.artifact.v1alpha.ArtifactPublicService/CreateKnowledgeBase", { namespaceId: data.expectedOwner.id, id, description: randomString(20), tags: ["test", "grpc"], type: "KNOWLEDGE_BASE_TYPE_PERSISTENT" }, data.metadata);
    const knowledgeBase = cRes.message.knowledgeBase;

    const res = publicClient.invoke("artifact.artifact.v1alpha.ArtifactPublicService/ListKnowledgeBases", { namespaceId: data.expectedOwner.id }, data.metadata);
    const found = Array.isArray(res.message.knowledgeBases) && res.message.knowledgeBases.some((c) => c.id === knowledgeBase.id);

    check(res, {
      "ListKnowledgeBases response includes created knowledge base": () => found,
    });

    publicClient.invoke("artifact.artifact.v1alpha.ArtifactPublicService/DeleteKnowledgeBase", { namespaceId: data.expectedOwner.id, knowledgeBaseId: knowledgeBase.id }, data.metadata);
    publicClient.close();
  });
}

export function TEST_05_UpdateKnowledgeBase(data) {
  const groupName = "Artifact API: Update knowledge base (gRPC)";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    publicClient.connect(constant.artifactGRPCPublicHost, { plaintext: true });

    const id = "test-" + data.dbIDPrefix + randomString(10);
    const cRes = publicClient.invoke("artifact.artifact.v1alpha.ArtifactPublicService/CreateKnowledgeBase", { namespaceId: data.expectedOwner.id, id, description: randomString(20), tags: ["test", "grpc"], type: "KNOWLEDGE_BASE_TYPE_PERSISTENT" }, data.metadata);
    const knowledgeBase = cRes.message.knowledgeBase;

    const newDescription = randomString(25);
    const uRes = publicClient.invoke("artifact.artifact.v1alpha.ArtifactPublicService/UpdateKnowledgeBase", {
      namespaceId: data.expectedOwner.id,
      knowledgeBaseId: knowledgeBase.id,
      knowledgeBase: {
        description: newDescription,
        tags: ["test", "grpc", "updated"]
      },
      update_mask: "description,tags"
    }, data.metadata);

    check(uRes, {
      "UpdateKnowledgeBase response status is StatusOK": (r) => r.status === grpc.StatusOK,
      "UpdateKnowledgeBase response id stable": (r) => r.message.knowledgeBase.id === knowledgeBase.id,
      "UpdateKnowledgeBase response description applied": (r) => r.message.knowledgeBase.description === newDescription,
    });

    publicClient.invoke("artifact.artifact.v1alpha.ArtifactPublicService/DeleteKnowledgeBase", { namespaceId: data.expectedOwner.id, knowledgeBaseId: knowledgeBase.id }, data.metadata);
    publicClient.close();
  });
}

export function TEST_06_DeleteKnowledgeBase(data) {
  const groupName = "Artifact API: Delete knowledge base (gRPC)";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    publicClient.connect(constant.artifactGRPCPublicHost, { plaintext: true });

    const id = "test-" + data.dbIDPrefix + randomString(10);
    const cRes = publicClient.invoke("artifact.artifact.v1alpha.ArtifactPublicService/CreateKnowledgeBase", { namespaceId: data.expectedOwner.id, id, description: randomString(20), tags: ["test", "grpc"], type: "KNOWLEDGE_BASE_TYPE_PERSISTENT" }, data.metadata);
    const knowledgeBase = cRes.message.knowledgeBase;

    const res = publicClient.invoke("artifact.artifact.v1alpha.ArtifactPublicService/DeleteKnowledgeBase", { namespaceId: data.expectedOwner.id, knowledgeBaseId: knowledgeBase.id }, data.metadata);
    check(res, {
      "DeleteKnowledgeBase response status is StatusOK": (r) => r.status === grpc.StatusOK,
    });

    publicClient.close();
  });
}

// ============================================================================
// TEST GROUP 07-10: File Operations (Public gRPC)
// ============================================================================
export function TEST_07_UploadFile(data) {
  const groupName = "Artifact API: Upload file (gRPC)";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    publicClient.connect(constant.artifactGRPCPublicHost, { plaintext: true });

    const cRes = publicClient.invoke("artifact.artifact.v1alpha.ArtifactPublicService/CreateKnowledgeBase",
      { namespaceId: data.expectedOwner.id, id: "test-" + data.dbIDPrefix + randomString(10), description: randomString(30), tags: ["test", "integration", "grpc"], type: "KNOWLEDGE_BASE_TYPE_PERSISTENT" },
      data.metadata
    );
    const knowledgeBase = cRes.message && cRes.message.knowledgeBase ? cRes.message.knowledgeBase : {};

    const reqBody = { namespaceId: data.expectedOwner.id, knowledgeBaseId: knowledgeBase.id, file: { filename: data.dbIDPrefix + "test-file-grpc-" + randomString(5) + ".doc", type: "TYPE_DOC", content: constant.docSampleDoc } };
    const resOrigin = publicClient.invoke("artifact.artifact.v1alpha.ArtifactPublicService/CreateFile", reqBody, data.metadata);

    check(resOrigin, {
      "CreateFile response status is StatusOK": (r) => r.status === grpc.StatusOK,
      "CreateFile response file filename": (r) => r.message.file.filename === reqBody.file.filename,
      "CreateFile response file uid": (r) => helper.isUUID(r.message.file.uid),
      "CreateFile response file type": (r) => r.message.file.type === "TYPE_DOC",
      "CreateFile response file is valid": (r) => helper.validateFileGRPC(r.message.file, false),
    });

    publicClient.invoke("artifact.artifact.v1alpha.ArtifactPublicService/DeleteFile", { namespaceId: data.expectedOwner.id, knowledgeBaseId: knowledgeBase.id, fileId: resOrigin.message.file.uid }, data.metadata);
    publicClient.invoke("artifact.artifact.v1alpha.ArtifactPublicService/DeleteKnowledgeBase", { namespaceId: data.expectedOwner.id, knowledgeBaseId: knowledgeBase.id }, data.metadata);
    publicClient.close();
  });
}

export function TEST_08_ListFiles(data) {
  const groupName = "Artifact API: List files (gRPC)";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    publicClient.connect(constant.artifactGRPCPublicHost, { plaintext: true });

    const cRes = publicClient.invoke("artifact.artifact.v1alpha.ArtifactPublicService/CreateKnowledgeBase", { namespaceId: data.expectedOwner.id, id: "test-" + data.dbIDPrefix + randomString(10), description: randomString(30), tags: ["test", "integration", "grpc"], type: "KNOWLEDGE_BASE_TYPE_PERSISTENT" }, data.metadata);
    const knowledgeBase = cRes.message && cRes.message.knowledgeBase ? cRes.message.knowledgeBase : {};
    const fRes = publicClient.invoke("artifact.artifact.v1alpha.ArtifactPublicService/CreateFile", { namespaceId: data.expectedOwner.id, knowledgeBaseId: knowledgeBase.id, file: { filename: data.dbIDPrefix + "test-file-grpc-" + randomString(5) + ".doc", type: "TYPE_DOC", content: constant.docSampleDoc } }, data.metadata);

    const resOrigin = publicClient.invoke("artifact.artifact.v1alpha.ArtifactPublicService/ListFiles", { namespaceId: data.expectedOwner.id, knowledgeBaseId: knowledgeBase.id, pageSize: 10 }, data.metadata);
    check(resOrigin, {
      "ListFiles response status is StatusOK": (r) => r.status === grpc.StatusOK,
      "ListFiles response files is array": (r) => Array.isArray(r.message.files)
    });

    publicClient.invoke("artifact.artifact.v1alpha.ArtifactPublicService/DeleteFile", { namespaceId: data.expectedOwner.id, knowledgeBaseId: knowledgeBase.id, fileId: fRes.message.file.uid }, data.metadata);
    publicClient.invoke("artifact.artifact.v1alpha.ArtifactPublicService/DeleteKnowledgeBase", { namespaceId: data.expectedOwner.id, knowledgeBaseId: knowledgeBase.id }, data.metadata);
    publicClient.close();
  });
}

export function TEST_09_GetFile(data) {
  const groupName = "Artifact API: Get file (gRPC)";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    publicClient.connect(constant.artifactGRPCPublicHost, { plaintext: true });

    const cRes = publicClient.invoke("artifact.artifact.v1alpha.ArtifactPublicService/CreateKnowledgeBase", { namespaceId: data.expectedOwner.id, id: "test-" + data.dbIDPrefix + randomString(10), description: randomString(30), tags: ["test", "integration", "grpc"], type: "KNOWLEDGE_BASE_TYPE_PERSISTENT" }, data.metadata);
    const knowledgeBase = cRes.message && cRes.message.knowledgeBase ? cRes.message.knowledgeBase : {};
    const fRes = publicClient.invoke("artifact.artifact.v1alpha.ArtifactPublicService/CreateFile", { namespaceId: data.expectedOwner.id, knowledgeBaseId: knowledgeBase.id, file: { filename: data.dbIDPrefix + "test-file-grpc-" + randomString(5) + ".doc", type: "TYPE_DOC", content: constant.docSampleDoc } }, data.metadata);
    const file = fRes.message.file;

    const resOrigin = publicClient.invoke("artifact.artifact.v1alpha.ArtifactPublicService/GetFile", { namespaceId: data.expectedOwner.id, knowledgeBaseId: knowledgeBase.id, fileId: file.uid }, data.metadata);
    check(resOrigin, {
      "GetFile response status is StatusOK": (r) => r.status === grpc.StatusOK,
      "GetFile response file uid": (r) => r.message.file.uid === file.uid,
      "GetFile response file name": (r) => r.message.file.filename === file.filename,
      "GetFile response file is valid": (r) => helper.validateFileGRPC(r.message.file, false)
    });

    publicClient.invoke("artifact.artifact.v1alpha.ArtifactPublicService/DeleteFile", { namespaceId: data.expectedOwner.id, knowledgeBaseId: knowledgeBase.id, fileId: file.uid }, data.metadata);
    publicClient.invoke("artifact.artifact.v1alpha.ArtifactPublicService/DeleteKnowledgeBase", { namespaceId: data.expectedOwner.id, knowledgeBaseId: knowledgeBase.id }, data.metadata);
    publicClient.close();
  });
}

export function TEST_10_CleanupOnDelete(data) {
  const groupName = "Artifact API: Cleanup intermediate files when file deleted (gRPC)";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    publicClient.connect(constant.artifactGRPCPublicHost, { plaintext: true });

    // Create knowledge base
    const cRes = publicClient.invoke("artifact.artifact.v1alpha.ArtifactPublicService/CreateKnowledgeBase",
      { namespaceId: data.expectedOwner.id, id: "test-" + data.dbIDPrefix + "clf-" + randomString(5), description: "Cleanup test", tags: ["test", "cleanup"], type: "KNOWLEDGE_BASE_TYPE_PERSISTENT" },
      data.metadata
    );
    const knowledgeBase = cRes.message && cRes.message.knowledgeBase ? cRes.message.knowledgeBase : {};

    // Upload a PDF file
    const fRes = publicClient.invoke("artifact.artifact.v1alpha.ArtifactPublicService/CreateFile",
      { namespaceId: data.expectedOwner.id, knowledgeBaseId: knowledgeBase.id, file: { filename: data.dbIDPrefix + "clf.pdf", type: "TYPE_PDF", content: constant.docSamplePdf } },
      data.metadata
    );
    const file = fRes.message && fRes.message.file ? fRes.message.file : {};

    check(fRes, {
      "DeleteFile: File uploaded": (r) => r.status === grpc.StatusOK && file.uid,
    });

    if (!file.uid) {
      publicClient.invoke("artifact.artifact.v1alpha.ArtifactPublicService/DeleteKnowledgeBase", { namespaceId: data.expectedOwner.id, knowledgeBaseId: knowledgeBase.id }, data.metadata);
      publicClient.close();
      return;
    }

    // Wait for processing to create temporary files
    sleep(5);

    // Delete file (triggers CleanupFileWorkflow)
    const dRes = publicClient.invoke("artifact.artifact.v1alpha.ArtifactPublicService/DeleteFile",
      { namespaceId: data.expectedOwner.id, knowledgeBaseId: knowledgeBase.id, fileId: file.uid },
      data.metadata
    );

    check(dRes, {
      "DeleteFile: File deleted": (r) => r.status === grpc.StatusOK,
    });

    // Wait for Temporal workflow cleanup
    sleep(10);

    // Verify cleanup
    const checkAfter = `
      SELECT
        (SELECT COUNT(*) FROM converted_file WHERE file_uid = '${file.uid}') as converted,
        (SELECT COUNT(*) FROM chunk WHERE file_uid = '${file.uid}') as chunks,
        (SELECT COUNT(*) FROM embedding WHERE file_uid = '${file.uid}') as embeddings
    `;
    try {
      const result = constant.db.exec(checkAfter);
      if (result && result.length > 0) {
        const after = result[0];
        check(after, {
          "DeleteFile: Converted files removed": () => parseInt(after.converted) === 0,
          "DeleteFile: Chunks removed": () => parseInt(after.chunks) === 0,
          "DeleteFile: Embeddings removed": () => parseInt(after.embeddings) === 0,
        });
      }
    } catch (e) {
      // Cleanup verification failed
    }

    // Cleanup knowledge base
    publicClient.invoke("artifact.artifact.v1alpha.ArtifactPublicService/DeleteKnowledgeBase", { namespaceId: data.expectedOwner.id, knowledgeBaseId: knowledgeBase.id }, data.metadata);
    publicClient.close();
  });
}

// ============================================================================
// TEST GROUP 11-13: JWT/Auth Tests
// ============================================================================
export function TEST_11_JWT_UploadFile(data) {
  const groupName = "Artifact API [gRPC/JWT]: Upload file rejects random user";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    publicClient.connect(constant.artifactGRPCPublicHost, { plaintext: true });

    // Create knowledge base with authorized metadata
    const cRes = publicClient.invoke("artifact.artifact.v1alpha.ArtifactPublicService/CreateKnowledgeBase",
      { namespaceId: data.expectedOwner.id, id: "test-" + data.dbIDPrefix + randomString(10), description: randomString(30), tags: ["test", "integration", "grpc"], type: "KNOWLEDGE_BASE_TYPE_PERSISTENT" },
      data.metadata
    );
    const knowledgeBase = cRes.message.knowledgeBase;

    const reqBody = { namespaceId: data.expectedOwner.id, knowledgeBaseId: knowledgeBase.id, file: { filename: data.dbIDPrefix + "test-file-grpc-jwt-" + randomString(5) + ".docx", type: "TYPE_DOCX", content: constant.docSampleDocx } };
    // Invoke with invalid Authorization metadata
    const resNeg = publicClient.invoke("artifact.artifact.v1alpha.ArtifactPublicService/CreateFile", reqBody, constant.paramsGRPCWithJwt);
    check(resNeg, {
      "CreateFile unauthenticated/denied": (r) => r.status === grpc.StatusPermissionDenied,
    });

    // Cleanup
    publicClient.invoke("artifact.artifact.v1alpha.ArtifactPublicService/DeleteKnowledgeBase", { namespaceId: data.expectedOwner.id, knowledgeBaseId: knowledgeBase.id }, data.metadata);
    publicClient.close();
  });
}

export function TEST_12_JWT_ListFiles(data) {
  const groupName = "Artifact API [gRPC/JWT]: List files rejects random user";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    publicClient.connect(constant.artifactGRPCPublicHost, { plaintext: true });

    // Create resources with authorized metadata
    const cRes = publicClient.invoke("artifact.artifact.v1alpha.ArtifactPublicService/CreateKnowledgeBase",
      { namespaceId: data.expectedOwner.id, id: "test-" + data.dbIDPrefix + randomString(10), description: randomString(30), tags: ["test", "integration", "grpc"], type: "KNOWLEDGE_BASE_TYPE_PERSISTENT" },
      data.metadata
    );
    const knowledgeBase = cRes.message.knowledgeBase;
    publicClient.invoke("artifact.artifact.v1alpha.ArtifactPublicService/CreateFile",
      { namespaceId: data.expectedOwner.id, knowledgeBaseId: knowledgeBase.id, file: { filename: data.dbIDPrefix + "test-file-grpc-jwt-" + randomString(5) + ".docx", type: "TYPE_DOCX", content: constant.docSampleDocx } },
      data.metadata
    );

    // Negative: list with invalid Authorization
    const resNeg = publicClient.invoke("artifact.artifact.v1alpha.ArtifactPublicService/ListFiles", { namespaceId: data.expectedOwner.id, knowledgeBaseId: knowledgeBase.id, pageSize: 10 }, constant.paramsGRPCWithJwt);
    check(resNeg, { "ListFiles unauthenticated/denied": (r) => r.status === grpc.StatusPermissionDenied });

    // Cleanup
    publicClient.invoke("artifact.artifact.v1alpha.ArtifactPublicService/DeleteKnowledgeBase", { namespaceId: data.expectedOwner.id, knowledgeBaseId: knowledgeBase.id }, data.metadata);
    publicClient.close();
  });
}

export function TEST_13_JWT_GetFile(data) {
  const groupName = "Artifact API [gRPC/JWT]: Get file rejects random user";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    publicClient.connect(constant.artifactGRPCPublicHost, { plaintext: true });

    const cRes = publicClient.invoke("artifact.artifact.v1alpha.ArtifactPublicService/CreateKnowledgeBase",
      { namespaceId: data.expectedOwner.id, id: "test-" + data.dbIDPrefix + randomString(10), description: randomString(30), tags: ["test", "integration", "grpc"], type: "KNOWLEDGE_BASE_TYPE_PERSISTENT" },
      data.metadata
    );
    const knowledgeBase = cRes.message.knowledgeBase;
    const fRes = publicClient.invoke("artifact.artifact.v1alpha.ArtifactPublicService/CreateFile",
      { namespaceId: data.expectedOwner.id, knowledgeBaseId: knowledgeBase.id, file: { filename: data.dbIDPrefix + "test-file-grpc-jwt-" + randomString(5) + ".docx", type: "TYPE_DOCX", content: constant.docSampleDocx } },
      data.metadata
    );
    const file = fRes.message.file;

    // Negative: get file with invalid Authorization
    const resNeg = publicClient.invoke("artifact.artifact.v1alpha.ArtifactPublicService/GetFile", { namespaceId: data.expectedOwner.id, knowledgeBaseId: knowledgeBase.id, fileId: file.uid }, constant.paramsGRPCWithJwt);
    check(resNeg, { "GetFile unauthenticated/denied": (r) => r.status === grpc.StatusPermissionDenied });

    // Cleanup
    publicClient.invoke("artifact.artifact.v1alpha.ArtifactPublicService/DeleteFile", { namespaceId: data.expectedOwner.id, knowledgeBaseId: knowledgeBase.id, fileId: file.uid }, data.metadata);
    publicClient.invoke("artifact.artifact.v1alpha.ArtifactPublicService/DeleteKnowledgeBase", { namespaceId: data.expectedOwner.id, knowledgeBaseId: knowledgeBase.id }, data.metadata);
    publicClient.close();
  });
}

// ============================================================================
// TEST GROUP 14-16: Private gRPC Tests (Admin only)
// ============================================================================
export function TEST_14_GetObject(data) {
  const groupName = "Artifact API: Get Object (private gRPC)";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    if (constant.apiGatewayMode) {
      check(true, { "skipped: apiGatewayMode enabled": () => true });
      return;
    }

    privateClient.connect(constant.artifactGRPCPrivateHost, { plaintext: true });

    const uid = constant.testObjectUid;
    if (!uid) {
      check(true, { "skipped: testObjectUid not provided": () => true });
      privateClient.close();
      return;
    }

    var res = privateClient.invoke("artifact.artifact.v1alpha.ArtifactPrivateService/GetObjectAdmin", { uid: uid }, data.metadata);
    check(res, {
      "GetObject returns StatusOK": (r) => r.status === grpc.StatusOK,
      "GetObject returns object": (r) => !!r.message && !!r.message.object,
    });

    privateClient.close();
  });
}

export function TEST_15_GetObjectURL(data) {
  const groupName = "Artifact API: Get Object URL (private gRPC)";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    if (constant.apiGatewayMode) {
      check(true, { "skipped: apiGatewayMode enabled": () => true });
      return;
    }

    privateClient.connect(constant.artifactGRPCPrivateHost, { plaintext: true });

    const uid = constant.testObjectUrlUid;
    const encoded = constant.testEncodedUrlPath;
    if (!uid && !encoded) {
      check(true, { "skipped: testObjectUrlUid or testEncodedUrlPath not provided": () => true });
      privateClient.close();
      return;
    }

    privateClient.close();
  });
}

export function TEST_16_UpdateObject(data) {
  const groupName = "Artifact API: Update Object (private gRPC)";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    if (constant.apiGatewayMode) {
      check(true, { "skipped: apiGatewayMode enabled": () => true });
      return;
    }

    privateClient.connect(constant.artifactGRPCPrivateHost, { plaintext: true });

    const uid = constant.testObjectUidToUpdate;
    if (!uid) {
      check(true, { "skipped: testObjectUidToUpdate not provided": () => true });
      privateClient.close();
      return;
    }

    var res = privateClient.invoke("artifact.artifact.v1alpha.ArtifactPrivateService/UpdateObjectAdmin",
      { uid: uid, isUploaded: true },
      data.metadata
    );
    check(res, {
      "UpdateObject returns a response": (r) => r && typeof r.status === grpc.StatusOK,
    });

    privateClient.close();
  });
}

// ============================================================================
// TEST 17: CreateKnowledgeBaseAdmin (Private gRPC)
// Tests creating a system knowledge base without a creator
// ============================================================================
export function TEST_17_CreateKnowledgeBaseAdmin(data) {
  const groupName = "Artifact API: Create Knowledge Base Admin (private gRPC)";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    if (constant.apiGatewayMode) {
      check(true, { "skipped: apiGatewayMode enabled": () => true });
      return;
    }

    privateClient.connect(constant.artifactGRPCPrivateHost, { plaintext: true });
    publicClient.connect(constant.artifactGRPCPublicHost, { plaintext: true });

    const systemKbId = `system-kb-${dbIDPrefix}`;

    // Create a system knowledge base without a creator using the admin endpoint
    // Admin endpoints can set reserved tags that public APIs cannot (agent:, instill-)
    var createRes = privateClient.invoke(
      "artifact.artifact.v1alpha.ArtifactPrivateService/CreateKnowledgeBaseAdmin",
      {
        namespace_id: constant.defaultUserId,
        id: systemKbId,
        description: "System knowledge base created via admin endpoint (no creator)",
        tags: ["instill-internal", "agent:system", "system-kb"],
        type: "KNOWLEDGE_BASE_TYPE_PERSISTENT",
      },
      data.metadata
    );
    check(createRes, {
      "CreateKnowledgeBaseAdmin returns StatusOK": (r) => r.status === grpc.StatusOK,
      "CreateKnowledgeBaseAdmin returns knowledge_base": (r) => !!r.message && !!r.message.knowledge_base,
      "CreateKnowledgeBaseAdmin returns correct id": (r) => r.message?.knowledge_base?.id === systemKbId,
      "CreateKnowledgeBaseAdmin has no creator_uid": (r) => !r.message?.knowledge_base?.creator_uid || r.message?.knowledge_base?.creator_uid === "",
      "CreateKnowledgeBaseAdmin can use reserved tags": (r) => {
        const tags = r.message?.knowledge_base?.tags || [];
        return tags.includes("instill-internal") && tags.includes("agent:system");
      },
    });

    // Verify the KB exists by getting it via public API
    if (createRes.status === grpc.StatusOK) {
      var getRes = publicClient.invoke(
        "artifact.artifact.v1alpha.ArtifactPublicService/GetKnowledgeBase",
        {
          namespace_id: constant.defaultUserId,
          knowledge_base_id: systemKbId,
        },
        data.metadata
      );
      check(getRes, {
        "GetKnowledgeBase returns StatusOK": (r) => r.status === grpc.StatusOK,
        "GetKnowledgeBase returns the system KB": (r) => r.message?.knowledge_base?.id === systemKbId,
        "System KB has no creator_uid (null/empty)": (r) => !r.message?.knowledge_base?.creator_uid || r.message?.knowledge_base?.creator_uid === "",
      });

      // Cleanup: Delete the test KB
      var deleteRes = publicClient.invoke(
        "artifact.artifact.v1alpha.ArtifactPublicService/DeleteKnowledgeBase",
        {
          namespace_id: constant.defaultUserId,
          knowledge_base_id: systemKbId,
        },
        data.metadata
      );
      check(deleteRes, {
        "DeleteKnowledgeBase cleanup succeeds": (r) => r.status === grpc.StatusOK,
      });
    }

    privateClient.close();
    publicClient.close();
  });
}

// ============================================================================
// TEST 18: UpdateKnowledgeBaseAdmin (Private gRPC)
// Tests updating a knowledge base with reserved tags via admin endpoint
// ============================================================================
export function TEST_18_UpdateKnowledgeBaseAdmin(data) {
  const groupName = "Artifact API: Update Knowledge Base Admin (private gRPC)";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    if (constant.apiGatewayMode) {
      check(true, { "skipped: apiGatewayMode enabled": () => true });
      return;
    }

    privateClient.connect(constant.artifactGRPCPrivateHost, { plaintext: true });
    publicClient.connect(constant.artifactGRPCPublicHost, { plaintext: true });

    const testKbId = `admin-update-kb-${dbIDPrefix}`;

    // First create a system KB using admin endpoint
    var createRes = privateClient.invoke(
      "artifact.artifact.v1alpha.ArtifactPrivateService/CreateKnowledgeBaseAdmin",
      {
        namespace_id: constant.defaultUserId,
        id: testKbId,
        description: "KB for testing UpdateKnowledgeBaseAdmin",
        tags: ["initial-tag"],
        type: "KNOWLEDGE_BASE_TYPE_PERSISTENT",
      },
      data.metadata
    );
    check(createRes, {
      "CreateKnowledgeBaseAdmin for update test succeeds": (r) => r.status === grpc.StatusOK,
    });

    if (createRes.status !== grpc.StatusOK) {
      privateClient.close();
      publicClient.close();
      return;
    }

    // Update with reserved tags using admin endpoint - should succeed
    var updateRes = privateClient.invoke(
      "artifact.artifact.v1alpha.ArtifactPrivateService/UpdateKnowledgeBaseAdmin",
      {
        namespace_id: constant.defaultUserId,
        knowledge_base_id: testKbId,
        knowledge_base: {
          tags: ["instill-internal", "agent:system", "user-tag"],
        },
        update_mask: { paths: ["tags"] },
      },
      data.metadata
    );
    check(updateRes, {
      "UpdateKnowledgeBaseAdmin returns StatusOK": (r) => r.status === grpc.StatusOK,
      "UpdateKnowledgeBaseAdmin returns knowledge_base": (r) => !!r.message?.knowledge_base,
      "UpdateKnowledgeBaseAdmin can set reserved tags": (r) => {
        const tags = r.message?.knowledge_base?.tags || [];
        return tags.includes("instill-internal") && tags.includes("agent:system");
      },
    });

    // Verify public API cannot set reserved tags
    var publicUpdateRes = publicClient.invoke(
      "artifact.artifact.v1alpha.ArtifactPublicService/UpdateKnowledgeBase",
      {
        namespace_id: constant.defaultUserId,
        knowledge_base_id: testKbId,
        knowledge_base: {
          tags: ["instill-blocked-tag"],
        },
        update_mask: { paths: ["tags"] },
      },
      data.metadata
    );
    check(publicUpdateRes, {
      "Public UpdateKnowledgeBase with reserved tag fails": (r) => r.status !== grpc.StatusOK,
    });

    // Cleanup
    var deleteRes = publicClient.invoke(
      "artifact.artifact.v1alpha.ArtifactPublicService/DeleteKnowledgeBase",
      {
        namespace_id: constant.defaultUserId,
        knowledge_base_id: testKbId,
      },
      data.metadata
    );
    check(deleteRes, {
      "DeleteKnowledgeBase cleanup succeeds": (r) => r.status === grpc.StatusOK,
    });

    privateClient.close();
    publicClient.close();
  });
}

// ============================================================================
// TEST 19: UpdateFileAdmin (Private gRPC)
// Tests updating a file with reserved tags via admin endpoint
// ============================================================================
export function TEST_19_UpdateFileAdmin(data) {
  const groupName = "Artifact API: Update File Admin (private gRPC)";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    if (constant.apiGatewayMode) {
      check(true, { "skipped: apiGatewayMode enabled": () => true });
      return;
    }

    privateClient.connect(constant.artifactGRPCPrivateHost, { plaintext: true });
    publicClient.connect(constant.artifactGRPCPublicHost, { plaintext: true });

    const testKbId = `admin-file-kb-${dbIDPrefix}`;

    // Create a KB for file testing
    var createKbRes = privateClient.invoke(
      "artifact.artifact.v1alpha.ArtifactPrivateService/CreateKnowledgeBaseAdmin",
      {
        namespace_id: constant.defaultUserId,
        id: testKbId,
        description: "KB for testing UpdateFileAdmin",
        type: "KNOWLEDGE_BASE_TYPE_PERSISTENT",
      },
      data.metadata
    );
    check(createKbRes, {
      "CreateKnowledgeBaseAdmin for file test succeeds": (r) => r.status === grpc.StatusOK,
    });

    if (createKbRes.status !== grpc.StatusOK) {
      privateClient.close();
      publicClient.close();
      return;
    }

    // Create a file using public API
    var createFileRes = publicClient.invoke(
      "artifact.artifact.v1alpha.ArtifactPublicService/CreateFile",
      {
        namespace_id: constant.defaultUserId,
        knowledge_base_id: testKbId,
        file: {
          filename: "test-admin-update.txt",
          type: "TYPE_TEXT",
        },
      },
      data.metadata
    );
    check(createFileRes, {
      "CreateFile for admin update test succeeds": (r) => r.status === grpc.StatusOK,
    });

    if (createFileRes.status !== grpc.StatusOK) {
      // Cleanup KB
      publicClient.invoke(
        "artifact.artifact.v1alpha.ArtifactPublicService/DeleteKnowledgeBase",
        { namespace_id: constant.defaultUserId, knowledge_base_id: testKbId },
        data.metadata
      );
      privateClient.close();
      publicClient.close();
      return;
    }

    const fileUid = createFileRes.message?.file?.uid;

    // Update file with reserved tags using admin endpoint - should succeed
    var updateFileRes = privateClient.invoke(
      "artifact.artifact.v1alpha.ArtifactPrivateService/UpdateFileAdmin",
      {
        namespace_id: constant.defaultUserId,
        knowledge_base_id: testKbId,
        file_id: fileUid,
        file: {
          tags: ["agent:collection:fake-uid-123", "user-tag"],
        },
        update_mask: { paths: ["tags"] },
      },
      data.metadata
    );
    check(updateFileRes, {
      "UpdateFileAdmin returns StatusOK": (r) => r.status === grpc.StatusOK,
      "UpdateFileAdmin returns file": (r) => !!r.message?.file,
      "UpdateFileAdmin can set agent: reserved tags": (r) => {
        const tags = r.message?.file?.tags || [];
        return tags.includes("agent:collection:fake-uid-123");
      },
    });

    // Verify public API cannot set reserved tags on files
    var publicUpdateRes = publicClient.invoke(
      "artifact.artifact.v1alpha.ArtifactPublicService/UpdateFile",
      {
        namespace_id: constant.defaultUserId,
        knowledge_base_id: testKbId,
        file_id: fileUid,
        file: {
          tags: ["agent:blocked-tag"],
        },
        update_mask: { paths: ["tags"] },
      },
      data.metadata
    );
    check(publicUpdateRes, {
      "Public UpdateFile with reserved tag fails": (r) => r.status !== grpc.StatusOK,
    });

    // Cleanup
    publicClient.invoke(
      "artifact.artifact.v1alpha.ArtifactPublicService/DeleteFile",
      {
        namespace_id: constant.defaultUserId,
        knowledge_base_id: testKbId,
        file_id: fileUid,
      },
      data.metadata
    );
    publicClient.invoke(
      "artifact.artifact.v1alpha.ArtifactPublicService/DeleteKnowledgeBase",
      { namespace_id: constant.defaultUserId, knowledge_base_id: testKbId },
      data.metadata
    );

    privateClient.close();
    publicClient.close();
  });
}
