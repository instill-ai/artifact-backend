import grpc from "k6/net/grpc";
import http from "k6/http";

import { check, group } from "k6";

import * as catalogPrivate from "./grpc-private.js";
import * as grpcPublic from "./grpc-public.js";
import * as grpcPublicWithJwt from "./grpc-public-with-jwt.js";

const client = new grpc.Client();
const mgmtClient = new grpc.Client();

// Load proto files using repo proto root and v1alpha dir so google/api resolves
client.load(["./proto", "./proto/artifact/artifact/v1alpha"], "artifact/artifact/v1alpha/artifact_public_service.proto");
client.load(["./proto", "./proto/artifact/artifact/v1alpha"], "artifact/artifact/v1alpha/artifact_private_service.proto");
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
  client.connect(constant.artifactGRPCPublicHost, {
    plaintext: true,
    timeout: "300s",
  });
  mgmtClient.connect(constant.mgmtGRPCPublicHost, {
    plaintext: true,
    timeout: "300s",
  });

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

  client.close();
  mgmtClient.close();
  return { metadata: metadata, expectedOwner: authResp.message.user };
}

export default function (data) {
  // Health check
  {
    const groupName = "Artifact API: Health check (gRPC)";
    group(groupName, () => {
      check(true, { [constant.banner(groupName)]: () => true });

      client.connect(constant.artifactGRPCPublicHost, {
        plaintext: true,
      });
      check(
        client.invoke(
          "artifact.artifact.v1alpha.ArtifactPublicService/Liveness",
          {}
        ),
        {
          "Liveness response status is StatusOK": (r) =>
            r.status === grpc.StatusOK,
        }
      );
      client.close();
    });
  }

  if (!constant.apiGatewayMode) {
    catalogPrivate.CheckList(client, data);
    catalogPrivate.CheckLookUp(client, data);
    return;
  }

  // Test public endpoints (gRPC) analogous to REST tests
  client.connect(constant.artifactGRPCPublicHost, { plaintext: true });

  // Catalog flows
  grpcPublic.CheckCreateCatalog(client, data);
  grpcPublic.CheckListCatalogs(client, data);
  grpcPublic.CheckGetCatalog(client, data);
  grpcPublic.CheckUpdateCatalog(client, data);

  // File flows
  grpcPublic.CheckUpload(client, data);
  grpcPublic.CheckList(client, data);
  grpcPublic.CheckGet(client, data);

  // JWT variants for file operations
  grpcPublicWithJwt.CheckUpload(client, data);
  grpcPublicWithJwt.CheckList(client, data);
  grpcPublicWithJwt.CheckGet(client, data);

  client.close();
}

export function teardown(data) {
  const groupName = "Artifact API: Delete data created by this test";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    var q = `DELETE FROM knowledge_base WHERE id LIKE '${constant.dbIDPrefix}%'`;
    constant.db.exec(q);

    q = `DELETE FROM knowledge_base_file WHERE name LIKE '${constant.dbIDPrefix}%'`;
    constant.db.exec(q);

    constant.db.close();
  });
}
