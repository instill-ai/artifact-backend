import http from "k6/http";
import { check, group, sleep } from "k6";
import { randomString } from "https://jslib.k6.io/k6-utils/1.1.0/index.js";

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
  teardownTimeout: '180s',
  insecureSkipTLSVerify: true,
  thresholds: {
    checks: ["rate == 1.0"],
  },
  // Parallel source scenarios per file type (exec functions are defined below)
  // startTime staggers HTTP requests to prevent k6 HTTP client overload (13 parallel connections)
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
    // Regression tests: Type inference from filename (type field omitted)
    // These tests ensure the backend correctly infers file type from extension when type is not provided
    test_type_text_inferred: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_TEXT_INFERRED' },
    test_type_markdown_inferred: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_MARKDOWN_INFERRED' },
    test_type_csv_inferred: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_CSV_INFERRED' },
    test_type_html_inferred: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_HTML_INFERRED' },
    test_type_pdf_inferred: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_PDF_INFERRED' },
    test_type_ppt_inferred: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_PPT_INFERRED' },
    test_type_pptx_inferred: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_PPTX_INFERRED' },
    test_type_xls_inferred: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_XLS_INFERRED' },
    test_type_xlsx_inferred: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_XLSX_INFERRED' },
    test_type_doc_inferred: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_DOC_INFERRED' },
    test_type_docx_inferred: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_DOCX_INFERRED' },
  },
};

export function setup() {

  check(true, { [constant.banner('Artifact API: Setup')]: () => true });

  // Authenticate FIRST (required for API calls and cleanup)
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

  // Generate THIS test run's unique prefix
  // Each test run gets a unique prefix to avoid conflicts with parallel tests
  const dbIDPrefix = constant.generateDBIDPrefix();
  console.log(`rest-file-type.js: Using unique test prefix: ${dbIDPrefix}`);

  return { header: header, expectedOwner: resp.json().user, dbIDPrefix: dbIDPrefix }
}

export function teardown(data) {
  const groupName = "Artifact API: Delete all catalogs and data created by this test";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    // CRITICAL: Wait for THIS TEST's file processing to complete before deleting catalogs
    // Deleting catalogs triggers cleanup workflows that drop vector DB collections
    // If we delete while files are still processing, we get "collection does not exist" errors
    // IMPORTANT: We MUST NOT proceed with deletion if files are still processing, as this creates
    // zombie workflows that continue running after the KB/files are deleted.
    console.log("Teardown: Waiting for this test's file processing to complete...");
    const allProcessingComplete = helper.waitForAllFileProcessingComplete(300, data.dbIDPrefix); // Increased to 5 minutes

    check({ allProcessingComplete }, {
      "Teardown: All files processed before cleanup (no zombie workflows)": () => allProcessingComplete === true,
    });

    if (!allProcessingComplete) {
      console.error("Teardown: Files still processing after timeout - CANNOT safely delete catalogs");
      console.error("Teardown: Leaving catalogs in place to avoid zombie workflows");
      console.error("Teardown: Manual cleanup may be required or increase timeout");
      // CRITICAL: Do NOT proceed with deletion - this would create zombie workflows
      // Better to leave test artifacts than to create workflows that fail with "collection does not exist"
      return;
    }

    // Delete catalogs via API (which triggers cleanup workflows)
    var listResp = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`, null, data.header)
    if (listResp.status === 200) {
      var catalogs = Array.isArray(listResp.json().catalogs) ? listResp.json().catalogs : []
      for (const catalog of catalogs) {
        // API returns id (AIP-compliant)
        const catId = catalog.id;
        if (catId && catId.startsWith(data.dbIDPrefix)) {
          var delResp = http.request("DELETE", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catId}`, null, data.header);
          check(delResp, {
            [`DELETE /v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catId} response status is 200 or 404`]: (r) => r.status === 200 || r.status === 404,
          });
        }
      }
    }
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

// Regression tests: Type inference from filename (type field omitted)
// These tests ensure the backend correctly infers file type from extension
export function TEST_TYPE_TEXT_INFERRED(data) { runCatalogFileTest(data, { originalName: "sample.txt", fileType: "TYPE_TEXT", omitType: true }); }
export function TEST_TYPE_MARKDOWN_INFERRED(data) { runCatalogFileTest(data, { originalName: "sample.md", fileType: "TYPE_MARKDOWN", omitType: true }); }
export function TEST_TYPE_CSV_INFERRED(data) { runCatalogFileTest(data, { originalName: "sample.csv", fileType: "TYPE_CSV", omitType: true }); }
export function TEST_TYPE_HTML_INFERRED(data) { runCatalogFileTest(data, { originalName: "sample.html", fileType: "TYPE_HTML", omitType: true }); }
export function TEST_TYPE_PDF_INFERRED(data) { runCatalogFileTest(data, { originalName: "sample.pdf", fileType: "TYPE_PDF", omitType: true }); }
export function TEST_TYPE_PPT_INFERRED(data) { runCatalogFileTest(data, { originalName: "sample.ppt", fileType: "TYPE_PPT", omitType: true }); }
export function TEST_TYPE_PPTX_INFERRED(data) { runCatalogFileTest(data, { originalName: "sample.pptx", fileType: "TYPE_PPTX", omitType: true }); }
export function TEST_TYPE_XLS_INFERRED(data) { runCatalogFileTest(data, { originalName: "sample.xls", fileType: "TYPE_XLS", omitType: true }); }
export function TEST_TYPE_XLSX_INFERRED(data) { runCatalogFileTest(data, { originalName: "sample.xlsx", fileType: "TYPE_XLSX", omitType: true }); }
export function TEST_TYPE_DOC_INFERRED(data) { runCatalogFileTest(data, { originalName: "sample.doc", fileType: "TYPE_DOC", omitType: true }); }
export function TEST_TYPE_DOCX_INFERRED(data) { runCatalogFileTest(data, { originalName: "sample.docx", fileType: "TYPE_DOCX", omitType: true }); }

// Internal helper to run catalog file test for each file type
function runCatalogFileTest(data, opts) {
  const groupName = "Artifact API: Catalog file type test";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    const { fileType, originalName, omitType } = opts || {};

    // Create catalog (id must be < 32 chars: test-{4}-src-{8} = 23 chars)
    const cRes = http.request("POST", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`, JSON.stringify({ id: data.dbIDPrefix + "src-" + randomString(8) }), data.header);
    logUnexpected(cRes, 'POST /v1alpha/namespaces/{namespace_id}/catalogs');
    const catalog = ((() => { try { return cRes.json(); } catch (e) { return {}; } })()).catalog || {};
    const catalogId = catalog.id;
    check(cRes, { [`POST /v1alpha/namespaces/{namespace_id}/catalogs 200 (${catalogId})`]: (r) => r.status === 200 });

    // List catalogs and ensure our catalog is present
    const listCatalogRes = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`, null, data.header);
    logUnexpected(listCatalogRes, 'GET /v1alpha/namespaces/{namespace_id}/catalogs');
    let listCatalogJson; try { listCatalogJson = listCatalogRes.json(); } catch (e) { listCatalogJson = {}; }
    const catalogsArr = Array.isArray(listCatalogJson.catalogs) ? listCatalogJson.catalogs : [];
    const containsUploadedCatalog = catalogsArr.some((c) => c.id === catalogId);
    check(listCatalogRes, {
      [`GET /v1alpha/namespaces/{namespace_id}/catalogs 200 (${catalogId})`]: (r) => r.status === 200,
      [`List contains uploaded catalog (${catalogId})`]: () => containsUploadedCatalog,
    });

    // Upload file
    const selector = originalName
      ? ((x) => x.originalName === originalName)
      : ((x) => x.type === fileType);
    const s = (constant.sampleFiles.find(selector) || {});
    const filename = data.dbIDPrefix + (s.originalName || ("sample-" + randomString(6)));

    // If omitType is true, don't include the type field to test backend type inference
    const fReq = omitType
      ? { filename: filename, content: s.content || "" }
      : { filename: filename, type: fileType, content: s.content || "" };

    if (omitType) {
      console.log(`Testing type inference for ${fileType}: uploading with filename only (no type field)`);
    }

    const uRes = http.request("POST", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files`, JSON.stringify(fReq), data.header);
    logUnexpected(uRes, 'POST /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/files');
    const file = ((() => { try { return uRes.json(); } catch (e) { return {}; } })()).file || {};
    const fileUid = file.uid;
    const testLabel = omitType ? `${fileType} [TYPE INFERRED]` : fileType;
    if (!fileUid) {
      console.log(`WARN ${testLabel}: No fileUid in response. Response status: ${uRes.status}, body: ${uRes.body}`);
    }
    check(uRes, { [`POST /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/files 200 (${testLabel})`]: (r) => r.status === 200 });

    // List catalog files and ensure our file is present
    const listCatalogFilesRes = http.request(
      "GET",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files`,
      null,
      data.header
    );
    logUnexpected(listCatalogFilesRes, 'GET /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/files');
    let listCatalogFilesJson; try { listCatalogFilesJson = listCatalogFilesRes.json(); } catch (e) { listCatalogFilesJson = {}; }
    const catalogFilesArr = Array.isArray(listCatalogFilesJson.files) ? listCatalogFilesJson.files : [];
    const containsUploadedCatalogFile = catalogFilesArr.some((f) => f.uid === fileUid);
    check(listCatalogFilesRes, {
      [`GET /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/files 200 (${testLabel})`]: (r) => r.status === 200,
      [`List contains uploaded file (${testLabel})`]: () => containsUploadedCatalogFile,
    });

    // GET single catalog file and validate
    const getCatalogFileRes = http.request(
      "GET",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files/${fileUid}`,
      null,
      data.header
    );
    logUnexpected(getCatalogFileRes, 'GET /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/files/{file_uid}');
    let getCatalogFileJson; try { getCatalogFileJson = getCatalogFileRes.json(); } catch (e) { getCatalogFileJson = {}; }
    check(getCatalogFileRes, {
      [`GET /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/files/{file_uid} 200 (${testLabel})`]: (r) => r.status === 200,
      [`GET /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/files/{file_uid} uid matches (${testLabel})`]: () => getCatalogFileJson.file && getCatalogFileJson.file.uid === fileUid,
      [`GET /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/files/{file_uid} filename matches (${testLabel})`]: () => getCatalogFileJson.file && getCatalogFileJson.file.filename === filename,
      [`GET /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/files/{file_uid} is valid (${testLabel})`]: () => getCatalogFileJson.file && helper.validateFile(getCatalogFileJson.file, false),
    });

    // Auto-trigger: Processing starts automatically on upload (no manual trigger needed)
    // Wait for file processing to complete using robust helper function
    console.log(`⏳ Waiting for file processing: ${testLabel} (fileUid: ${fileUid})...`);
    const result = helper.waitForFileProcessingComplete(
      data.expectedOwner.id,
      catalogId,
      fileUid,
      data.header,
      600, // Max 600 seconds (10 minutes) for large file processing
      120   // Fast-fail after 120s if stuck in NOTSTARTED
    );

    const completed = result.completed && result.status === "COMPLETED";
    const failed = result.status === "FAILED";
    const failureReason = result.error || "";

    if (failed) {
      console.log(`✗ File processing failed for ${testLabel}: ${failureReason}`);
    } else if (!completed) {
      console.log(`✗ File processing timed out for ${testLabel}: Status ${result.status}`);
    }

    check({ completed, failed }, {
      [`GET /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/files/{file_uid} 200 and Process Status Reached COMPLETED (${testLabel})`]: () => completed === true,
      [`File processing did not fail (${testLabel})`]: () => !failed,
    });

    if (failed) {
      console.log(`✗ Skipping remaining checks for ${testLabel} due to processing failure: ${failureReason}`);
      // Don't delete here - let teardown handle cleanup
      return;
    }

    // Get file content (using VIEW_CONTENT)
    const getCatalogFileContent = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files/${fileUid}?view=VIEW_CONTENT`, null, data.header);
    logUnexpected(getCatalogFileContent, 'GET /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/files/{file_uid}?view=VIEW_CONTENT');
    let contentData; try { contentData = getCatalogFileContent.json(); } catch (e) { contentData = {}; }
    const contentUri = contentData.derivedResourceUri || ""; // derivedResourceUri is at top level, not inside .file
    check(getCatalogFileContent, {
      [`GET /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/files/{file_uid}?view=VIEW_CONTENT 200 (${testLabel})`]: (r) => r.status === 200,
      [`VIEW_CONTENT returns derivedResourceUri (${testLabel})`]: () => contentUri && contentUri.length > 0,
    });

    // Get file summary (using VIEW_SUMMARY)
    const getSummaryRes = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files/${fileUid}?view=VIEW_SUMMARY`, null, data.header);
    logUnexpected(getSummaryRes, 'GET /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/files/{file_uid}?view=VIEW_SUMMARY');
    let summaryData; try { summaryData = getSummaryRes.json(); } catch (e) { summaryData = {}; }
    const summaryUri = summaryData.derivedResourceUri || ""; // derivedResourceUri is at top level, not inside .file
    check(getSummaryRes, {
      [`GET /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/files/{file_uid}?view=VIEW_SUMMARY 200 (${testLabel})`]: (r) => r.status === 200,
      [`VIEW_SUMMARY returns derivedResourceUri (${testLabel})`]: () => summaryUri && summaryUri.length > 0,
    });

    // Get file standardized format (using VIEW_STANDARD_FILE_TYPE)
    // Standardized view returns:
    // - Documents → PDF (e.g., DOC, DOCX, PPT, PPTX, XLS, XLSX, HTML, TEXT, MARKDOWN, CSV)
    // - Images → PNG (e.g., GIF, BMP, TIFF, AVIF)
    // - Audio → OGG (e.g., M4A, WMA)
    // - Video → MP4 (e.g., MKV)
    // AI-native formats (PDF, PNG, JPEG, MP3, WAV, MP4, etc.) are also accessible as standardized files
    const standardizableTypes = ["TYPE_PDF", "TYPE_DOC", "TYPE_DOCX", "TYPE_PPT", "TYPE_PPTX", "TYPE_XLS", "TYPE_XLSX", "TYPE_HTML", "TYPE_TEXT", "TYPE_MARKDOWN", "TYPE_CSV"];
    const isStandardizable = standardizableTypes.includes(fileType);

    const getStandardRes = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files/${fileUid}?view=VIEW_STANDARD_FILE_TYPE`, null, data.header);
    logUnexpected(getStandardRes, 'GET /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/files/{file_uid}?view=VIEW_STANDARD_FILE_TYPE');
    let standardData; try { standardData = getStandardRes.json(); } catch (e) { standardData = {}; }
    const standardUri = standardData.derivedResourceUri || ""; // derivedResourceUri is at top level, not inside .file

    if (isStandardizable) {
      check(getStandardRes, {
        [`GET /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/files/{file_uid}?view=VIEW_STANDARD_FILE_TYPE 200 (${testLabel})`]: (r) => r.status === 200,
        [`VIEW_STANDARD_FILE_TYPE returns derivedResourceUri for standardizable type (${testLabel})`]: () => standardUri && standardUri.length > 0,
      });

      // Verify the URL is accessible (basic check)
      if (standardUri) {
        console.log(`✓ VIEW_STANDARD_FILE_TYPE returned URL for ${testLabel}: ${standardUri.substring(0, 50)}...`);
      }
    } else {
      // For non-standardizable types (TEXT, MARKDOWN, CSV, HTML), VIEW_STANDARD_FILE_TYPE should still return 200 but may not have a standardized file
      check(getStandardRes, {
        [`GET /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/files/{file_uid}?view=VIEW_STANDARD_FILE_TYPE 200 (${testLabel})`]: (r) => r.status === 200,
      });
      if (standardUri) {
        console.log(`INFO: Unexpected standardized file available for non-standardizable type ${testLabel}`);
      }
    }

    // Get original file (using VIEW_ORIGINAL_FILE_TYPE)
    // This should return the original uploaded file for ALL file types
    const getOriginalRes = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files/${fileUid}?view=VIEW_ORIGINAL_FILE_TYPE`, null, data.header);
    logUnexpected(getOriginalRes, 'GET /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/files/{file_uid}?view=VIEW_ORIGINAL_FILE_TYPE');
    let originalData; try { originalData = getOriginalRes.json(); } catch (e) { originalData = {}; }
    const originalUri = originalData.derivedResourceUri || ""; // derivedResourceUri is at top level, not inside .file

    check(getOriginalRes, {
      [`GET /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/files/{file_uid}?view=VIEW_ORIGINAL_FILE_TYPE 200 (${testLabel})`]: (r) => r.status === 200,
      [`VIEW_ORIGINAL_FILE_TYPE returns derivedResourceUri (${testLabel})`]: () => originalUri && originalUri.length > 0,
    });

    if (originalUri) {
      console.log(`✓ VIEW_ORIGINAL_FILE_TYPE returned URL for ${testLabel}: ${originalUri.substring(0, 50)}...`);
    }

    // Compare VIEW_STANDARD_FILE_TYPE and VIEW_ORIGINAL_FILE_TYPE URLs
    // For PDF files, they may point to the same file (since PDF is already standardized)
    // For convertible documents, they should be different
    if (isStandardizable && standardUri && originalUri) {
      const sameFile = standardUri === originalUri;
      const isPdfType = fileType === "TYPE_PDF";

      if (isPdfType && sameFile) {
        console.log(`✓ ${testLabel}: Standard and original URLs are same (expected for native PDF)`);
      } else if (!isPdfType && !sameFile) {
        console.log(`✓ ${testLabel}: Standard and original URLs are different (expected for converted documents)`);
      } else {
        console.log(`⚠ ${testLabel}: URL comparison unexpected - isPDF:${isPdfType}, same:${sameFile}`);
      }
    }

    // List chunks for this file
    const listChunksUrl = `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files/${fileUid}/chunks`;
    const listChunksRes = http.request("GET", listChunksUrl, null, data.header);
    logUnexpected(listChunksRes, 'GET /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/files/{file_id}/chunks');
    let listChunksJson; try { listChunksJson = listChunksRes.json(); } catch (e) { listChunksJson = {}; }
    check(listChunksRes, {
      [`GET /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/files/{file_id}/chunks 200 (${testLabel})`]: (r) => r.status === 200,
    });
  });
}
