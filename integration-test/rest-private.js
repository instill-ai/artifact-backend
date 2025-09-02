import http from "k6/http";
import { check, group, sleep } from "k6";

import { artifactPrivateHost } from "./const.js";

import * as constant from "./const.js";

export function CheckList(data) {
  const groupName = "Artifact API: List catalogs (private)";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    var resOrigin = http.request(
      "GET",
      `${artifactPrivateHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`,
      null,
      data.header
    );
    check(resOrigin, {
      "GET /v1alpha/namespaces/{namespace_id}/catalogs response status is 200": (r) => r.status === 200,
      "GET /v1alpha/namespaces/{namespace_id}/catalogs response catalogs is array": (r) =>
        Array.isArray(r.json().catalogs),
    });
    return resOrigin.json().catalogs;
  });
}

export function CheckLookUp(data) {
  const groupName = "Artifact API: Look up catalogs (private)";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    var resOrigin = http.request(
      "GET",
      `${artifactPrivateHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`,
      null,
      data.header
    );
    check(resOrigin, {
      "GET /v1alpha/namespaces/{namespace_id}/catalogs response status is 200": (r) => r.status === 200,
    });
  });
}
