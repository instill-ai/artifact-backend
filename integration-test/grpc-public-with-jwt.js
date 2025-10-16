import grpc from "k6/net/grpc";
import { check, group } from "k6";
import { randomString } from "https://jslib.k6.io/k6-utils/1.1.0/index.js";

import * as constant from "./const.js";
import * as helper from "./helper.js";

export function CheckUploadCatalogFile(client, data) {
  const groupName = "Artifact API [gRPC/JWT]: Upload file rejects random user (Unauthenticated)";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    // Create catalog with authorized metadata
    const cRes = client.invoke("artifact.artifact.v1alpha.ArtifactPublicService/CreateCatalog", { namespaceId: data.expectedOwner.id, name: constant.dbIDPrefix + randomString(10), description: randomString(30), tags: ["test", "integration", "grpc"], type: "CATALOG_TYPE_PERSISTENT" }, data.metadata);
    const catalog = cRes.message.catalog;

    const reqBody = { namespaceId: data.expectedOwner.id, catalogId: catalog.catalogId, file: { name: constant.dbIDPrefix + "test-file-grpc-jwt-" + randomString(5) + ".docx", type: "TYPE_DOCX", content: constant.sampleDocx } };
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
    const cRes = client.invoke("artifact.artifact.v1alpha.ArtifactPublicService/CreateCatalog", { namespaceId: data.expectedOwner.id, name: constant.dbIDPrefix + randomString(10), description: randomString(30), tags: ["test", "integration", "grpc"], type: "CATALOG_TYPE_PERSISTENT" }, data.metadata);
    const catalog = cRes.message.catalog;
    client.invoke("artifact.artifact.v1alpha.ArtifactPublicService/UploadCatalogFile", { namespaceId: data.expectedOwner.id, catalogId: catalog.catalogId, file: { name: constant.dbIDPrefix + "test-file-grpc-jwt-" + randomString(5) + ".docx", type: "TYPE_DOCX", content: constant.sampleDocx } }, data.metadata);

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

    const cRes = client.invoke("artifact.artifact.v1alpha.ArtifactPublicService/CreateCatalog", { namespaceId: data.expectedOwner.id, name: constant.dbIDPrefix + randomString(10), description: randomString(30), tags: ["test", "integration", "grpc"], type: "CATALOG_TYPE_PERSISTENT" }, data.metadata);
    const catalog = cRes.message.catalog;
    const fRes = client.invoke("artifact.artifact.v1alpha.ArtifactPublicService/UploadCatalogFile", { namespaceId: data.expectedOwner.id, catalogId: catalog.catalogId, file: { name: constant.dbIDPrefix + "test-file-grpc-jwt-" + randomString(5) + ".docx", type: "TYPE_DOCX", content: constant.sampleDocx } }, data.metadata);
    const file = fRes.message.file;

    // Negative: get file with invalid Authorization
    const resNeg = client.invoke("artifact.artifact.v1alpha.ArtifactPublicService/GetCatalogFile", { namespaceId: data.expectedOwner.id, catalogId: catalog.catalogId, fileUid: file.fileUid }, constant.paramsGRPCWithJwt);
    check(resNeg, { "artifact.artifact.v1alpha.ArtifactPublicService/GetCatalogFile unauthenticated/denied": (r) => r.status === grpc.StatusPermissionDenied });

    // Cleanup (authorized)
    client.invoke("artifact.artifact.v1alpha.ArtifactPublicService/DeleteCatalogFile", { fileUid: file.fileUid }, data.metadata);
    client.invoke("artifact.artifact.v1alpha.ArtifactPublicService/DeleteCatalog", { namespaceId: data.expectedOwner.id, catalogId: catalog.catalogId }, data.metadata);
  });
}
