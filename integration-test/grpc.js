import grpc from "k6/net/grpc";
import http from "k6/http";

import { check, group } from "k6";

import * as catalogPrivate from "./grpc-private.js";
import * as grpcPublic from "./grpc-public.js";
import * as grpcPublicWithJwt from "./grpc-public-with-jwt.js";

const publicClient = new grpc.Client();
const privateClient = new grpc.Client();
const mgmtClient = new grpc.Client();

// Load proto files using repo proto root and v1alpha dir so google/api resolves
publicClient.load(["./proto", "./proto/artifact/artifact/v1alpha"], "artifact/artifact/v1alpha/artifact_public_service.proto");
publicClient.load(["./proto", "./proto/artifact/artifact/v1alpha"], "artifact/artifact/v1alpha/artifact_private_service.proto");
privateClient.load(["./proto", "./proto/artifact/artifact/v1alpha"], "artifact/artifact/v1alpha/artifact_private_service.proto");
mgmtClient.load(["./proto", "./proto/core/mgmt/v1beta"], "core/mgmt/v1beta/mgmt_public_service.proto");

import * as constant from "./const.js";

export let options = {
  setupTimeout: "300s",
  insecureSkipTLSVerify: true,
  thresholds: {
    checks: ["rate == 1.0"],
  },
};

export function setup() {
  publicClient.connect(constant.artifactGRPCPublicHost, {
    plaintext: true,
    timeout: "300s",
  });
  mgmtClient.connect(constant.mgmtGRPCPublicHost, {
    plaintext: true,
    timeout: "300s",
  });

  // Clean up any leftover test data from previous runs
  try {
    constant.db.exec(`DELETE FROM text_chunk WHERE kb_file_uid IN (SELECT uid FROM knowledge_base_file WHERE name LIKE '${constant.dbIDPrefix}%')`);
    constant.db.exec(`DELETE FROM embedding WHERE kb_file_uid IN (SELECT uid FROM knowledge_base_file WHERE name LIKE '${constant.dbIDPrefix}%')`);
    constant.db.exec(`DELETE FROM converted_file WHERE file_uid IN (SELECT uid FROM knowledge_base_file WHERE name LIKE '${constant.dbIDPrefix}%')`);
    constant.db.exec(`DELETE FROM knowledge_base_file WHERE name LIKE '${constant.dbIDPrefix}%'`);
    constant.db.exec(`DELETE FROM knowledge_base WHERE id LIKE '${constant.dbIDPrefix}%'`);
  } catch (e) {
    console.log(`Setup cleanup warning: ${e}`);
  }

  var loginResp = http.request("POST", `${constant.mgmtPublicHost}/v1beta/auth/login`, JSON.stringify({
    "username": constant.defaultUsername,
    "password": constant.defaultPassword,
  }))

  check(loginResp, {
    [`POST ${constant.mgmtPublicHost}/v1beta/auth/login response status is 200`]: (
      r
    ) => r.status === 200,
  });

  var metadata = {
    "metadata": {
      "Authorization": `Bearer ${loginResp.json().accessToken}`,
    },
    "timeout": "600s",
  }

  var authResp = mgmtClient.invoke(
    "core.mgmt.v1beta.MgmtPublicService/GetAuthenticatedUser",
    {},
    metadata
  );

  publicClient.close();
  mgmtClient.close();
  return { metadata: metadata, expectedOwner: authResp.message.user };
}

export default function (data) {
  // Health check
  {
    const groupName = "Artifact API: Health check (gRPC)";
    group(groupName, () => {
      check(true, { [constant.banner(groupName)]: () => true });

      publicClient.connect(constant.artifactGRPCPublicHost, {
        plaintext: true,
      });
      check(
        publicClient.invoke(
          "artifact.artifact.v1alpha.ArtifactPublicService/Liveness",
          {}
        ),
        {
          "Liveness response status is StatusOK": (r) =>
            r.status === grpc.StatusOK,
        }
      );
      check(
        publicClient.invoke(
          "artifact.artifact.v1alpha.ArtifactPublicService/Readiness",
          {}
        ),
        {
          "Readiness response status is StatusOK": (r) =>
            r.status === grpc.StatusOK,
        }
      );
      publicClient.close();
    });
  }

  if (!constant.apiGatewayMode) {
    privateClient.connect(constant.artifactGRPCPrivateHost, {
      plaintext: true,
    });
    catalogPrivate.CheckListRepositoryTags(privateClient, data);
    catalogPrivate.CheckGetRepositoryTag(privateClient, data);
    catalogPrivate.CheckCreateAndDeleteRepositoryTag(privateClient, data);
    catalogPrivate.CheckGetObject(privateClient, data);
    catalogPrivate.CheckGetObjectURL(privateClient, data);
    catalogPrivate.CheckUpdateObject(privateClient, data);
    catalogPrivate.CheckGetFileAsMarkdown(privateClient, data);
    privateClient.close();
  }

  // Test public endpoints (gRPC) analogous to REST tests
  publicClient.connect(constant.artifactGRPCPublicHost, {
    plaintext: true,
  });

  // Catalog flows
  grpcPublic.CheckCreateCatalog(publicClient, data);
  grpcPublic.CheckListCatalogs(publicClient, data);
  grpcPublic.CheckGetCatalog(publicClient, data);
  grpcPublic.CheckUpdateCatalog(publicClient, data);

  // File flows
  grpcPublic.CheckUploadCatalogFile(publicClient, data);
  grpcPublic.CheckListCatalogFiles(publicClient, data);
  grpcPublic.CheckGetCatalogFile(publicClient, data);
  grpcPublic.CheckCleanupOnFileDeletion(publicClient, data);

  // JWT variants for file operations
  grpcPublicWithJwt.CheckUploadCatalogFile(publicClient, data);
  grpcPublicWithJwt.CheckListCatalogFiles(publicClient, data);
  grpcPublicWithJwt.CheckGetCatalogFile(publicClient, data);

  publicClient.close();
}

export function teardown(data) {
  const groupName = "Artifact API: Delete data created by this test";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    // Delete from child tables first, before deleting parent records
    constant.db.exec(`DELETE FROM text_chunk WHERE kb_file_uid IN (SELECT uid FROM knowledge_base_file WHERE name LIKE '${constant.dbIDPrefix}%')`);
    constant.db.exec(`DELETE FROM embedding WHERE kb_file_uid IN (SELECT uid FROM knowledge_base_file WHERE name LIKE '${constant.dbIDPrefix}%')`);
    constant.db.exec(`DELETE FROM converted_file WHERE file_uid IN (SELECT uid FROM knowledge_base_file WHERE name LIKE '${constant.dbIDPrefix}%')`);

    // Now delete parent tables
    constant.db.exec(`DELETE FROM knowledge_base_file WHERE name LIKE '${constant.dbIDPrefix}%'`);
    constant.db.exec(`DELETE FROM knowledge_base WHERE id LIKE '${constant.dbIDPrefix}%'`);

    constant.db.close();
  });
}
