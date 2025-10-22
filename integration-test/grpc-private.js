import grpc from "k6/net/grpc";
import { check, group } from "k6";
import * as constant from "./const.js";

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
    const tagId = `${constant.dbIDPrefix}tag-${Date.now()}`;
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
