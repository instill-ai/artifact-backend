import http from "k6/http";
import { check, group, sleep } from "k6";
import { randomString } from "https://jslib.k6.io/k6-utils/1.1.0/index.js";

import { artifactPublicHost } from "./const.js";

import * as constant from "./const.js";
import * as helper from "./helper.js";

function logUnexpected(res, label) {
  if (res && res.status !== 200) {
    try {
      console.log(`${label} status=${res.status} body=${JSON.stringify(res.json())}`);
    } catch (e) {
      console.log(`${label} status=${res.status}`);
    }
  }
}

export let options = {
  setupTimeout: '300s',
  insecureSkipTLSVerify: true,
  thresholds: {
    checks: ["rate == 1.0"],
  },
  // Parallel source scenarios per file type (exec functions are defined below)
  scenarios: {
    test_type_text: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_TEXT' },
    test_type_markdown: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_MARKDOWN' },
    test_type_csv: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_CSV' },
    test_type_html: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_HTML' },
    test_type_pdf: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_PDF' },
    test_type_ppt: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_PPT' },
    test_type_pptx: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_PPTX' },
    test_type_xls: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_XLS' },
    test_type_xlsx: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_XLSX' },
    test_type_doc: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_DOC' },
    test_type_docx: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_DOCX' },
    test_type_doc_uppercase: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_DOC_UPPERCASE' },
    test_type_docx_uppercase: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_DOCX_UPPERCASE' },
  },
};

export function setup() {

  check(true, { [constant.banner('Artifact API: Setup')]: () => true });

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

export function teardown(data) {
  const groupName = "Artifact API: Delete all catalogs and data created by this test";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    // Delete catalogs via API (which triggers cleanup workflows)
    var listResp = http.request("GET", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`, null, data.header)
    if (listResp.status === 200) {
      var catalogs = Array.isArray(listResp.json().catalogs) ? listResp.json().catalogs : []
      for (const catalog of catalogs) {
        if (catalog.catalog_id && catalog.catalog_id.startsWith(constant.dbIDPrefix)) {
          var delResp = http.request("DELETE", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalog.catalog_id}`, null, data.header);
          check(delResp, {
            [`DELETE /v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalog.catalog_id} response status is 200 or 404`]: (r) => r.status === 200 || r.status === 404,
          });
        }
      }
    }

    // Final DB cleanup (defensive - in case workflows didn't complete)
    // Delete from child tables first, before deleting parent records
    try {
      constant.db.exec(`DELETE FROM text_chunk WHERE kb_file_uid IN (SELECT uid FROM knowledge_base_file WHERE name LIKE '${constant.dbIDPrefix}%')`);
      constant.db.exec(`DELETE FROM embedding WHERE kb_file_uid IN (SELECT uid FROM knowledge_base_file WHERE name LIKE '${constant.dbIDPrefix}%')`);
      constant.db.exec(`DELETE FROM converted_file WHERE file_uid IN (SELECT uid FROM knowledge_base_file WHERE name LIKE '${constant.dbIDPrefix}%')`);
      constant.db.exec(`DELETE FROM knowledge_base_file WHERE name LIKE '${constant.dbIDPrefix}%'`);
      constant.db.exec(`DELETE FROM knowledge_base WHERE id LIKE '${constant.dbIDPrefix}%'`);
    } catch (e) {
      console.log(`Teardown DB cleanup warning: ${e}`);
    }

    constant.db.close();
  });
}


// Scenario execs per file type
export function TEST_TYPE_TEXT(data) { runCatalogFileTest(data, { originalName: "sample.txt", fileType: "TYPE_TEXT" }); }
export function TEST_TYPE_MARKDOWN(data) { runCatalogFileTest(data, { originalName: "sample.md", fileType: "TYPE_MARKDOWN" }); }
export function TEST_TYPE_CSV(data) { runCatalogFileTest(data, { originalName: "sample.csv", fileType: "TYPE_CSV" }); }
export function TEST_TYPE_HTML(data) { runCatalogFileTest(data, { originalName: "sample.html", fileType: "TYPE_HTML" }); }
export function TEST_TYPE_PDF(data) { runCatalogFileTest(data, { originalName: "sample.pdf", fileType: "TYPE_PDF" }); }
export function TEST_TYPE_PPT(data) { runCatalogFileTest(data, { originalName: "sample.ppt", fileType: "TYPE_PPT" }); }
export function TEST_TYPE_PPTX(data) { runCatalogFileTest(data, { originalName: "sample.pptx", fileType: "TYPE_PPTX" }); }
export function TEST_TYPE_XLS(data) { runCatalogFileTest(data, { originalName: "sample.xls", fileType: "TYPE_XLS" }); }
export function TEST_TYPE_XLSX(data) { runCatalogFileTest(data, { originalName: "sample.xlsx", fileType: "TYPE_XLSX" }); }
export function TEST_TYPE_DOC(data) { runCatalogFileTest(data, { originalName: "sample.doc", fileType: "TYPE_DOC" }); }
export function TEST_TYPE_DOCX(data) { runCatalogFileTest(data, { originalName: "sample.docx", fileType: "TYPE_DOCX" }); }
export function TEST_TYPE_DOC_UPPERCASE(data) { runCatalogFileTest(data, { originalName: "SAMPLE-UPPERCASE-FILENAME.DOC", fileType: "TYPE_DOC" }); }
export function TEST_TYPE_DOCX_UPPERCASE(data) { runCatalogFileTest(data, { originalName: "SAMPLE-UPPERCASE-FILENAME.DOCX", fileType: "TYPE_DOCX" }); }

// Internal helper to run catalog file test for each file type
function runCatalogFileTest(data, opts) {
  const groupName = "Artifact API: Catalog file type test";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    const { fileType, originalName } = opts || {};

    // Create catalog (name must be < 32 chars: test-{4}-src-{8} = 23 chars)
    const cRes = http.request("POST", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`, JSON.stringify({ name: constant.dbIDPrefix + "src-" + randomString(8) }), data.header);
    logUnexpected(cRes, 'POST /v1alpha/namespaces/{namespace_id}/catalogs');
    const catalog = ((() => { try { return cRes.json(); } catch (e) { return {}; } })()).catalog || {};
    const catalogId = catalog.catalogId;
    check(cRes, { [`POST /v1alpha/namespaces/{namespace_id}/catalogs 200 (${catalogId})`]: (r) => r.status === 200 });

    // List catalogs and ensure our catalog is present
    const listCatalogRes = http.request("GET", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`, null, data.header);
    logUnexpected(listCatalogRes, 'GET /v1alpha/namespaces/{namespace_id}/catalogs');
    let listCatalogJson; try { listCatalogJson = listCatalogRes.json(); } catch (e) { listCatalogJson = {}; }
    const catalogsArr = Array.isArray(listCatalogJson.catalogs) ? listCatalogJson.catalogs : [];
    const containsUploadedCatalog = catalogsArr.some((c) => c.catalogId === catalogId);
    check(listCatalogRes, {
      [`GET /v1alpha/namespaces/{namespace_id}/catalogs 200 (${catalogId})`]: (r) => r.status === 200,
      [`List contains uploaded catalog (${catalogId})`]: () => containsUploadedCatalog,
    });

    // Upload file
    const selector = originalName
      ? ((x) => x.originalName === originalName)
      : ((x) => x.type === fileType);
    const s = (constant.sampleFiles.find(selector) || {});
    const fileName = constant.dbIDPrefix + (s.originalName || ("sample-" + randomString(6)));
    const fReq = { name: fileName, type: fileType, content: s.content || "" };
    const uRes = http.request("POST", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files`, JSON.stringify(fReq), data.header);
    logUnexpected(uRes, 'POST /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/files');
    const file = ((() => { try { return uRes.json(); } catch (e) { return {}; } })()).file || {};
    const fileUid = file.fileUid;
    check(uRes, { [`POST /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/files 200 (${fileUid})`]: (r) => r.status === 200 });

    // List catalog files and ensure our file is present
    const listCatalogFilesRes = http.request(
      "GET",
      `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files`,
      null,
      data.header
    );
    logUnexpected(listCatalogFilesRes, 'GET /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/files');
    let listCatalogFilesJson; try { listCatalogFilesJson = listCatalogFilesRes.json(); } catch (e) { listCatalogFilesJson = {}; }
    const catalogFilesArr = Array.isArray(listCatalogFilesJson.files) ? listCatalogFilesJson.files : [];
    const containsUploadedCatalogFile = catalogFilesArr.some((f) => f.fileUid === fileUid);
    check(listCatalogFilesRes, {
      [`GET /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/files 200 (${fileType})`]: (r) => r.status === 200,
      [`List contains uploaded file (${fileType})`]: () => containsUploadedCatalogFile,
    });

    // GET single catalog file and validate
    const getCatalogFileRes = http.request(
      "GET",
      `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files/${fileUid}`,
      null,
      data.header
    );
    logUnexpected(getCatalogFileRes, 'GET /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/files/{file_uid}');
    let getCatalogFileJson; try { getCatalogFileJson = getCatalogFileRes.json(); } catch (e) { getCatalogFileJson = {}; }
    check(getCatalogFileRes, {
      [`GET /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/files/{file_uid} 200 (${fileType})`]: (r) => r.status === 200,
      [`GET /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/files/{file_uid} uid matches (${fileType})`]: () => getCatalogFileJson.file && getCatalogFileJson.file.fileUid === fileUid,
      [`GET /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/files/{file_uid} name matches (${fileType})`]: () => getCatalogFileJson.file && getCatalogFileJson.file.name === fileName,
      [`GET /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/files/{file_uid} is valid (${fileType})`]: () => getCatalogFileJson.file && helper.validateFile(getCatalogFileJson.file, false),
    });

    // Process file and wait for completion
    const getProcessRes = http.request("POST", `${artifactPublicHost}/v1alpha/catalogs/files/processAsync`, JSON.stringify({ fileUids: [fileUid] }), data.header);
    logUnexpected(getProcessRes, 'POST /v1alpha/catalogs/files/processAsync');
    check(getProcessRes, { [`POST /v1alpha/catalogs/files/processAsync 200 (${fileUid})`]: (r) => r.status === 200 });

    let getProcessStatusRes; let completed = false; let failed = false; let failureReason = "";
    for (let i = 0; i < 3600; i++) {
      getProcessStatusRes = http.request("GET", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files/${fileUid}`, null, data.header);
      try {
        const body = getProcessStatusRes.json();
        const st = (body.file && body.file.processStatus) || "";
        if (getProcessStatusRes.status === 200 && st === "FILE_PROCESS_STATUS_COMPLETED") {
          completed = true;
          break;
        } else if (getProcessStatusRes.status === 200 && st === "FILE_PROCESS_STATUS_FAILED") {
          failed = true;
          failureReason = (body.file && body.file.processOutcome) || "Unknown error";
          console.log(`✗ File processing failed for ${fileType}: ${failureReason}`);
          break;
        }
        // Log progress every 30 seconds for slow processing files
        if (i > 0 && i % 60 === 0) {
          console.log(`⏳ Still processing ${fileType} after ${i * 0.5}s, status: ${st}`);
        }
      } catch (e) { }
      sleep(0.5);
    }
    check(getProcessStatusRes, {
      [`GET /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/files/{file_uid} 200 and Process Status Reached COMPLETED (${fileType})`]: () => completed === true,
      [`File processing did not fail (${fileType})`]: () => !failed,
    });

    if (failed) {
      console.log(`✗ Skipping remaining checks for ${fileType} due to processing failure: ${failureReason}`);
      // Don't delete here - let teardown handle cleanup
      return;
    }

    // Get the single-source-of-truth processed file source
    const getCatalogFileSource = http.request("GET", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files/${fileUid}/source`, null, data.header);
    logUnexpected(getCatalogFileSource, 'GET /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/files/{file_uid}/source');
    check(getCatalogFileSource, { [`GET /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/files/{file_uid}/source 200 (${fileType})`]: (r) => r.status === 200 });

    // Get file summary
    const getSummaryRes = http.request("GET", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files/${fileUid}/summary`, null, data.header);
    logUnexpected(getSummaryRes, 'GET /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/files/{file_uid}/summary');
    check(getSummaryRes, { [`GET /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/files/{file_uid}/summary 200 (${fileType})`]: (r) => r.status === 200 });

    // List chunks for this file
    const listChunksUrl = `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/chunks?file_uid=${fileUid}&fileUid=${fileUid}`;
    const listChunksRes = http.request("GET", listChunksUrl, null, data.header);
    logUnexpected(listChunksRes, 'GET /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/chunks');
    let listChunksJson; try { listChunksJson = listChunksRes.json(); } catch (e) { listChunksJson = {}; }
    check(listChunksRes, {
      [`GET /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/chunks 200 (${fileType})`]: (r) => r.status === 200,
    });
  });
}
