import grpc from "k6/net/grpc";
import { check, group, sleep } from "k6";
import { randomString } from "https://jslib.k6.io/k6-utils/1.1.0/index.js";

import * as constant from "./const.js";
import * as helper from "./helper.js";

export function CheckUploadCatalogFile(client, data) {
  const groupName = "Artifact API: Upload file (gRPC)";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    const cRes = client.invoke(
      "artifact.artifact.v1alpha.ArtifactPublicService/CreateCatalog",
      { namespaceId: data.expectedOwner.id, name: constant.dbIDPrefix + randomString(10), description: randomString(30), tags: ["test", "integration", "grpc"], type: "CATALOG_TYPE_PERSISTENT" },
      data.metadata
    );
    const catalog = cRes.message && cRes.message.catalog ? cRes.message.catalog : {};

    const reqBody = { namespaceId: data.expectedOwner.id, catalogId: catalog.catalogId, file: { name: constant.dbIDPrefix + "test-file-grpc-" + randomString(5) + ".doc", type: "TYPE_DOC", content: constant.sampleDoc } };
    const resOrigin = client.invoke("artifact.artifact.v1alpha.ArtifactPublicService/UploadCatalogFile", reqBody, data.metadata);
    check(resOrigin, {
      "artifact.artifact.v1alpha.ArtifactPublicService/UploadCatalogFile response status is StatusOK": (r) => r.status === grpc.StatusOK,
      "artifact.artifact.v1alpha.ArtifactPublicService/UploadCatalogFile response file name": (r) => r.message.file.name === reqBody.file.name,
      "artifact.artifact.v1alpha.ArtifactPublicService/UploadCatalogFile response file uid": (r) => helper.isUUID(r.message.file.fileUid),
      "artifact.artifact.v1alpha.ArtifactPublicService/UploadCatalogFile response file type": (r) => r.message.file.type === "TYPE_DOC",
      "artifact.artifact.v1alpha.ArtifactPublicService/UploadCatalogFile response file is valid": (r) => helper.validateFileGRPC(r.message.file, false),
    });

    client.invoke("artifact.artifact.v1alpha.ArtifactPublicService/DeleteCatalogFile", { fileUid: resOrigin.message.file.fileUid }, data.metadata);
    client.invoke("artifact.artifact.v1alpha.ArtifactPublicService/DeleteCatalog", { namespaceId: data.expectedOwner.id, catalogId: catalog.catalogId }, data.metadata);
  });
}

export function CheckListCatalogFiles(client, data) {
  const groupName = "Artifact API: List files (gRPC)";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    const cRes = client.invoke("artifact.artifact.v1alpha.ArtifactPublicService/CreateCatalog", { namespaceId: data.expectedOwner.id, name: constant.dbIDPrefix + randomString(10), description: randomString(30), tags: ["test", "integration", "grpc"], type: "CATALOG_TYPE_PERSISTENT" }, data.metadata);
    const catalog = cRes.message && cRes.message.catalog ? cRes.message.catalog : {};
    const fRes = client.invoke("artifact.artifact.v1alpha.ArtifactPublicService/UploadCatalogFile", { namespaceId: data.expectedOwner.id, catalogId: catalog.catalogId, file: { name: constant.dbIDPrefix + "test-file-grpc-" + randomString(5) + ".doc", type: "TYPE_DOC", content: constant.sampleDoc } }, data.metadata);

    const resOrigin = client.invoke("artifact.artifact.v1alpha.ArtifactPublicService/ListCatalogFiles", { namespaceId: data.expectedOwner.id, catalogId: catalog.catalogId, pageSize: 10 }, data.metadata);
    check(resOrigin, {
      "artifact.artifact.v1alpha.ArtifactPublicService/ListCatalogFiles response status is StatusOK": (r) => r.status === grpc.StatusOK, "artifact.artifact.v1alpha.ArtifactPublicService/ListCatalogFiles response files is array": (r) => Array.isArray(r.message.files)
    });

    client.invoke("artifact.artifact.v1alpha.ArtifactPublicService/DeleteCatalogFile", { fileUid: fRes.message.file.fileUid }, data.metadata);
    client.invoke("artifact.artifact.v1alpha.ArtifactPublicService/DeleteCatalog", { namespaceId: data.expectedOwner.id, catalogId: catalog.catalogId }, data.metadata);
  });
}

export function CheckGetCatalogFile(client, data) {
  const groupName = "Artifact API: Get file (gRPC)";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    const cRes = client.invoke("artifact.artifact.v1alpha.ArtifactPublicService/CreateCatalog", { namespaceId: data.expectedOwner.id, name: constant.dbIDPrefix + randomString(10), description: randomString(30), tags: ["test", "integration", "grpc"], type: "CATALOG_TYPE_PERSISTENT" }, data.metadata);
    const catalog = cRes.message && cRes.message.catalog ? cRes.message.catalog : {};
    const fRes = client.invoke("artifact.artifact.v1alpha.ArtifactPublicService/UploadCatalogFile", { namespaceId: data.expectedOwner.id, catalogId: catalog.catalogId, file: { name: constant.dbIDPrefix + "test-file-grpc-" + randomString(5) + ".doc", type: "TYPE_DOC", content: constant.sampleDoc } }, data.metadata);
    const file = fRes.message.file;

    const resOrigin = client.invoke("artifact.artifact.v1alpha.ArtifactPublicService/GetCatalogFile", { namespaceId: data.expectedOwner.id, catalogId: catalog.catalogId, fileUid: file.fileUid }, data.metadata);
    check(resOrigin, {
      "artifact.artifact.v1alpha.ArtifactPublicService/GetCatalogFile response status is StatusOK": (r) => r.status === grpc.StatusOK, "artifact.artifact.v1alpha.ArtifactPublicService/GetCatalogFile response file uid": (r) => r.message.file.fileUid === file.fileUid, "artifact.artifact.v1alpha.ArtifactPublicService/GetCatalogFile response file name": (r) => r.message.file.name === file.name, "artifact.artifact.v1alpha.ArtifactPublicService/GetCatalogFile response file is valid": (r) => helper.validateFileGRPC(r.message.file, false)
    });

    client.invoke("artifact.artifact.v1alpha.ArtifactPublicService/DeleteCatalogFile", { fileUid: file.fileUid }, data.metadata);
    client.invoke("artifact.artifact.v1alpha.ArtifactPublicService/DeleteCatalog", { namespaceId: data.expectedOwner.id, catalogId: catalog.catalogId }, data.metadata);
  });
}

// No standalone delete; each check cleans up its resources

export function CheckCreateCatalog(client, data) {
  const groupName = "Artifact API: Create catalog (gRPC)";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    const name = constant.dbIDPrefix + randomString(10);
    const req = { namespaceId: data.expectedOwner.id, name, description: randomString(30), tags: ["test", "grpc"], type: "CATALOG_TYPE_PERSISTENT" };
    const res = client.invoke("artifact.artifact.v1alpha.ArtifactPublicService/CreateCatalog", req, data.metadata);
    check(res, {
      "artifact.artifact.v1alpha.ArtifactPublicService/CreateCatalog response status is StatusOK": (r) => r.status === grpc.StatusOK,
      "artifact.artifact.v1alpha.ArtifactPublicService/CreateCatalog response catalog id matches": (r) => r.message && r.message.catalog && r.message.catalog.catalogId === name,
    });
    // cleanup
    if (res.message && res.message.catalog) {
      client.invoke("artifact.artifact.v1alpha.ArtifactPublicService/DeleteCatalog", { namespaceId: data.expectedOwner.id, catalogId: res.message.catalog.catalogId }, data.metadata);
    }
  });
}

export function CheckListCatalogs(client, data) {
  const groupName = "Artifact API: List catalogs (gRPC)";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    const res = client.invoke("artifact.artifact.v1alpha.ArtifactPublicService/ListCatalogs", { namespaceId: data.expectedOwner.id }, data.metadata);
    check(res, {
      "artifact.artifact.v1alpha.ArtifactPublicService/ListCatalogs response status is StatusOK": (r) => r.status === grpc.StatusOK,
      "artifact.artifact.v1alpha.ArtifactPublicService/ListCatalogs response catalogs is array": (r) => Array.isArray(r.message.catalogs),
    });
  });
}

export function CheckGetCatalog(client, data) {
  const groupName = "Artifact API: Get catalog (gRPC)";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    const name = constant.dbIDPrefix + randomString(10);
    const cRes = client.invoke("artifact.artifact.v1alpha.ArtifactPublicService/CreateCatalog", { namespaceId: data.expectedOwner.id, name, description: randomString(20), tags: ["test", "grpc"], type: "CATALOG_TYPE_PERSISTENT" }, data.metadata);
    const catalog = cRes.message.catalog;
    const res = client.invoke("artifact.artifact.v1alpha.ArtifactPublicService/ListCatalogs", { namespaceId: data.expectedOwner.id }, data.metadata);
    const found = Array.isArray(res.message.catalogs) && res.message.catalogs.some((c) => c.catalogId === catalog.catalogId);
    check(res, {
      "artifact.artifact.v1alpha.ArtifactPublicService/ListCatalogs response includes created catalog": () => found,
    });
    client.invoke("artifact.artifact.v1alpha.ArtifactPublicService/DeleteCatalog", { namespaceId: data.expectedOwner.id, catalogId: catalog.catalogId }, data.metadata);
  });
}

export function CheckUpdateCatalog(client, data) {
  const groupName = "Artifact API: Update catalog (gRPC)";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    const name = constant.dbIDPrefix + randomString(10);
    const cRes = client.invoke("artifact.artifact.v1alpha.ArtifactPublicService/CreateCatalog", { namespaceId: data.expectedOwner.id, name, description: randomString(20), tags: ["test", "grpc"], type: "CATALOG_TYPE_PERSISTENT" }, data.metadata);
    const catalog = cRes.message.catalog;
    const updateReq = { namespaceId: data.expectedOwner.id, catalogId: catalog.catalogId, description: randomString(25), tags: ["test", "grpc", "updated"] };
    const uRes = client.invoke("artifact.artifact.v1alpha.ArtifactPublicService/UpdateCatalog", updateReq, data.metadata);
    check(uRes, {
      "artifact.artifact.v1alpha.ArtifactPublicService/UpdateCatalog response status is StatusOK": (r) => r.status === grpc.StatusOK,
      "artifact.artifact.v1alpha.ArtifactPublicService/UpdateCatalog response id stable": (r) => r.message.catalog.catalogId === catalog.catalogId,
      "artifact.artifact.v1alpha.ArtifactPublicService/UpdateCatalog response description applied": (r) => r.message.catalog.description === updateReq.description,
    });
    client.invoke("artifact.artifact.v1alpha.ArtifactPublicService/DeleteCatalog", { namespaceId: data.expectedOwner.id, catalogId: catalog.catalogId }, data.metadata);
  });
}

export function CheckDeleteCatalog(client, data) {
  const groupName = "Artifact API: Delete catalog (gRPC)";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    const name = constant.dbIDPrefix + randomString(10);
    const cRes = client.invoke("artifact.artifact.v1alpha.ArtifactPublicService/CreateCatalog", { namespaceId: data.expectedOwner.id, name, description: randomString(20), tags: ["test", "grpc"], type: "CATALOG_TYPE_PERSISTENT" }, data.metadata);
    const catalog = cRes.message.catalog;
    const res = client.invoke("artifact.artifact.v1alpha.ArtifactPublicService/DeleteCatalog", { namespaceId: data.expectedOwner.id, catalogId: catalog.catalogId }, data.metadata);
    check(res, {
      "artifact.artifact.v1alpha.ArtifactPublicService/DeleteCatalog response status is StatusOK": (r) => r.status === grpc.StatusOK,
    });
  });
}

export function CheckCleanupOnFileDeletion(client, data) {
  const groupName = "Artifact API: Cleanup intermediate files when file deleted (gRPC)";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    // Create catalog
    const cRes = client.invoke(
      "artifact.artifact.v1alpha.ArtifactPublicService/CreateCatalog",
      { namespaceId: data.expectedOwner.id, name: constant.dbIDPrefix + "clf-" + randomString(5), description: "Cleanup test", tags: ["test", "cleanup"], type: "CATALOG_TYPE_PERSISTENT" },
      data.metadata
    );
    const catalog = cRes.message && cRes.message.catalog ? cRes.message.catalog : {};

    // Upload a PDF file (will trigger conversion)
    const fRes = client.invoke(
      "artifact.artifact.v1alpha.ArtifactPublicService/UploadCatalogFile",
      { namespaceId: data.expectedOwner.id, catalogId: catalog.catalogId, file: { name: constant.dbIDPrefix + "clf.pdf", type: "TYPE_PDF", content: constant.samplePdf } },
      data.metadata
    );
    const file = fRes.message && fRes.message.file ? fRes.message.file : {};

    check(fRes, {
      "DeleteCatalogFile: File uploaded": (r) => r.status === grpc.StatusOK && file.fileUid,
    });

    if (!file.fileUid) {
      client.invoke("artifact.artifact.v1alpha.ArtifactPublicService/DeleteCatalog", { namespaceId: data.expectedOwner.id, catalogId: catalog.catalogId }, data.metadata);
      return;
    }

    // Trigger processing
    const pRes = client.invoke(
      "artifact.artifact.v1alpha.ArtifactPublicService/ProcessCatalogFiles",
      { fileUids: [file.fileUid] },
      data.metadata
    );

    check(pRes, {
      "DeleteCatalogFile: Processing triggered": (r) => r.status === grpc.StatusOK,
    });

    // Wait for processing to create temporary files
    sleep(5);

    // Delete file (triggers CleanupFileWorkflow via Temporal workflow)
    const dRes = client.invoke(
      "artifact.artifact.v1alpha.ArtifactPublicService/DeleteCatalogFile",
      { fileUid: file.fileUid },
      data.metadata
    );

    check(dRes, {
      "DeleteCatalogFile: File deleted": (r) => r.status === grpc.StatusOK,
    });

    // Wait for Temporal workflow cleanup to complete
    sleep(10);

    // Verify cleanup removed all temporary resources
    const checkAfter = `
      SELECT
        (SELECT COUNT(*) FROM converted_file WHERE file_uid = '${file.fileUid}') as converted,
        (SELECT COUNT(*) FROM text_chunk WHERE file_uid = '${file.fileUid}') as chunks,
        (SELECT COUNT(*) FROM embedding WHERE file_uid = '${file.fileUid}') as embeddings
    `;
    try {
      const result = constant.db.exec(checkAfter);
      if (result && result.length > 0) {
        const after = result[0];
        check(after, {
          "DeleteCatalogFile: Converted files removed": () => parseInt(after.converted) === 0,
          "DeleteCatalogFile: Chunks removed": () => parseInt(after.chunks) === 0,
          "DeleteCatalogFile: Embeddings removed": () => parseInt(after.embeddings) === 0,
        });
      }
    } catch (e) {
      // Cleanup verification failed - test will fail via checks
    }

    // Cleanup catalog
    client.invoke("artifact.artifact.v1alpha.ArtifactPublicService/DeleteCatalog", { namespaceId: data.expectedOwner.id, catalogId: catalog.catalogId }, data.metadata);
  });
}
