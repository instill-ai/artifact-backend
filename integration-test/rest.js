import http from "k6/http";

import {
  check,
  group,
} from "k6";

import { artifactRESTPublicHost } from "./const.js";

import * as constant from "./const.js";
import * as helper from "./helper.js";
import * as restPublic from './rest-public.js';
import * as restPublicWithJwt from './rest-public-with-jwt.js';

export let options = {
  setupTimeout: '30s',
  teardownTimeout: '180s',
  iterations: 1,
  duration: '120m',
  insecureSkipTLSVerify: true,
  thresholds: {
    checks: ["rate == 1.0"],
  },
};

export function setup() {

  check(true, { [constant.banner('Artifact API: Setup')]: () => true });

  // Add stagger to reduce parallel resource contention
  helper.staggerTestExecution(2);

  // CRITICAL: Generate unique prefix for THIS test file to avoid parallel test conflicts
  const testPrefix = constant.generateDBIDPrefix();
  console.log(`rest.js: Using unique test prefix: ${testPrefix}`);

  var loginResp = http.request("POST", `${constant.mgmtRESTPublicHost}/v1beta/auth/login`, JSON.stringify({
    "username": constant.defaultUsername,
    "password": constant.defaultPassword,
  }))

  check(loginResp, {
    [`POST ${constant.mgmtRESTPublicHost}/v1beta/auth/login response status is 200`]: (
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

  var resp = http.request("GET", `${constant.mgmtRESTPublicHost}/v1beta/user`, {}, { headers: { "Authorization": `Bearer ${loginResp.json().accessToken}` } })
  return { header: header, expectedOwner: resp.json().user, testPrefix: testPrefix }
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

      check(http.request("GET", `${artifactRESTPublicHost}/v1alpha/health/artifact`), {
        "GET /health/artifact response status is 200": (r) => r.status === 200,
      });
    });
  }

  restPublic.CheckCreateCatalog(data);
  restPublic.CheckListCatalogs(data);
  restPublic.CheckGetCatalog(data);
  restPublic.CheckUpdateCatalog(data);
  restPublic.CheckDeleteCatalog(data);

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
}

export function teardown(data) {
  const groupName = "Artifact API: Delete all files, catalogs and data created by this test";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    // CRITICAL: Wait for THIS TEST's file processing to complete before deleting catalogs
    // Deleting catalogs triggers cleanup workflows that drop vector DB collections
    // If we delete while files are still processing, we get "collection does not exist" errors
    console.log("Teardown: Waiting for this test's file processing to complete...");
    const allProcessingComplete = helper.waitForAllFileProcessingComplete(120, data.testPrefix);
    if (!allProcessingComplete) {
      console.warn("Teardown: Some files still processing after 120s, proceeding anyway");
    }

    // Note: testPrefix is unique per test file, so this only cleans up resources from THIS test file
    // This prevents parallel tests from interfering with each other
    console.log(`rest.js teardown: Cleaning up resources with prefix: ${data.testPrefix}`);
    var listResp = http.request("GET", `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`, null, data.header)
    if (listResp.status === 200) {
      var catalogs = Array.isArray(listResp.json().catalogs) ? listResp.json().catalogs : []

      for (const catalog of catalogs) {
        // API returns catalogId (camelCase), not catalog_id
        const catId = catalog.catalogId || catalog.catalog_id;
        if (catId && catId.startsWith(data.testPrefix)) {
          var delResp = http.request("DELETE", `${artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catId}`, null, data.header);
          check(delResp, {
            [`DELETE /v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catId} response status is 200 or 404`]: (r) => r.status === 200 || r.status === 404,
          });
        }
      }
    }
  });
}
