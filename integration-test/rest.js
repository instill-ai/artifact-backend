import http from "k6/http";

import {
  check,
  group,
} from "k6";

import { artifactPublicHost } from "./const.js";

import * as constant from "./const.js";
import * as restPublic from './rest-public.js';
import * as restPublicWithJwt from './rest-public-with-jwt.js';

export let options = {
  setupTimeout: '300s',
  insecureSkipTLSVerify: true,
  thresholds: {
    checks: ["rate == 1.0"],
  },
};

export function setup() {

  check(true, { [constant.banner('Artifact API: Setup')]: () => true });
  var loginResp = http.request("POST", `${constant.mgmtPublicHost}/v1beta/auth/login`, JSON.stringify({
    "username": constant.defaultUsername,
    "password": constant.defaultPassword,
  }))

  check(loginResp, {
    [`POST ${constant.mgmtPublicHost}/v1beta/auth/login response status is 200`]: (
      r
    ) => r.status === 200,
  });

  var header = {
    "headers": {
      "Authorization": `Bearer ${loginResp.json().accessToken}`,
      "Content-Type": "application/json",
    },
    "timeout": "600s",
  }

  var resp = http.request("GET", `${constant.mgmtPublicHost}/v1beta/user`, {}, { headers: { "Authorization": `Bearer ${loginResp.json().accessToken}` } })
  return { header: header, expectedOwner: resp.json().user }
}

export default function (data) {

  /*
   * Artifact API - API CALLS
   */

  // Health check
  {
    const groupName = "Artifact API: Health check";
    group(groupName, () => {
      check(true, { [constant.banner(groupName)]: () => true });

      check(http.request("GET", `${artifactPublicHost}/v1alpha/health/artifact`), {
        "GET /health/artifact response status is 200": (r) => r.status === 200,
      });
    });
  }

  restPublic.CheckCreateCatalog(data);
  restPublic.CheckListCatalogs(data);
  restPublic.CheckGetCatalog(data);
  restPublic.CheckUpdateCatalog(data);
  restPublic.CheckDeleteCatalog(data);
  restPublic.CheckCatalog(data);

  restPublicWithJwt.CheckCreateCatalogUnauthenticated(data);
  restPublicWithJwt.CheckListCatalogsUnauthenticated(data);
  restPublicWithJwt.CheckGetCatalogUnauthenticated(data);
  restPublicWithJwt.CheckUpdateCatalogUnauthenticated(data);
  restPublicWithJwt.CheckCreateFileUnauthenticated(data);
  restPublicWithJwt.CheckListFilesUnauthenticated(data);
  restPublicWithJwt.CheckGetFileUnauthenticated(data);
  restPublicWithJwt.CheckProcessFilesUnauthorized(data);
  restPublicWithJwt.CheckGetFileSourceUnauthorized(data);
  restPublicWithJwt.CheckGetFileSummaryUnauthenticated(data);
  restPublicWithJwt.CheckListChunksUnauthenticated(data);
  restPublicWithJwt.CheckSearchChunksUnauthenticated(data);
}

export function teardown(data) {
  const groupName = "Artifact API: Delete all files, catalogs and data created by this test";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    var listResp = http.request("GET", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`, null, data.header)
    var catalogs = (listResp.status === 200 && Array.isArray(listResp.json().catalogs)) ? listResp.json().catalogs : []

    for (const catalog of catalogs) {
      var listFilesResp = http.request("GET", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalog.catalog_id}/files`, null, data.header)
      var files = (listFilesResp.status === 200 && Array.isArray(listFilesResp.json().files)) ? listFilesResp.json().files : []
      for (const file of files) {
        check(http.request("DELETE", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalog.catalog_id}/files/${file.file_id}`, null, data.header), {
          [`DELETE /v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalog.catalog_id}/files/${file.file_id} response status is 200`]: (r) => r.status === 200,
        });
      }
    }

    for (const catalog of catalogs) {
      if (catalog.catalog_id && catalog.catalog_id.startsWith(constant.dbIDPrefix)) {
        check(http.request("DELETE", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalog.catalog_id}`, null, data.header), {
          [`DELETE /v1alpha/namespaces/${data.expectedOwner.id}/catalogs response status is 200`]: (r) => r.status === 200,
        });
      }
    }

    var q = `DELETE FROM knowledge_base WHERE id LIKE '${constant.dbIDPrefix}%'`;
    constant.db.exec(q);

    q = `DELETE FROM knowledge_base_file WHERE name LIKE '${constant.dbIDPrefix}%'`;
    constant.db.exec(q);

    constant.db.close();
  });
}
