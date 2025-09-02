import grpc from "k6/net/grpc";
import { check, group } from "k6";

export function CheckList(client, data) {
  const groupName = "Artifact API: List catalogs (private gRPC)";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    var resOrigin = client.invoke(
      "artifact.artifact.v1alpha.ArtifactPrivateService/ListRepositoryTags",
      {
        parent: `repositories/${data.expectedOwner.id}`,
        pageSize: 10
      },
      data.metadata
    );
    check(resOrigin, {
      "artifact.artifact.v1alpha.ArtifactPrivateService/ListRepositoryTags response status is StatusOK": (r) => r.status === grpc.StatusOK,
      "artifact.artifact.v1alpha.ArtifactPrivateService/ListRepositoryTags response tags is array": (r) =>
        Array.isArray(r.message.tags),
    });
    return resOrigin.message.tags;
  });
}

export function CheckLookUp(client, data) {
  const groupName = "Artifact API: Look up catalogs (private gRPC)";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    var resOrigin = client.invoke(
      "artifact.artifact.v1alpha.ArtifactPrivateService/ListRepositoryTags",
      {
        parent: `repositories/${data.expectedOwner.id}`,
        pageSize: 10
      },
      data.metadata
    );
    check(resOrigin, {
      "artifact.artifact.v1alpha.ArtifactPrivateService/ListRepositoryTags response status is StatusOK": (r) => r.status === grpc.StatusOK,
    });
  });
}
