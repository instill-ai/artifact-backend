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
  teardownTimeout: '300s', // Increased to match longer processing times with AI rate limiting
  insecureSkipTLSVerify: true,
  thresholds: {
    checks: ["rate == 1.0"],
  },
  // Parallel source scenarios per file type (exec functions are defined below)
  // CRITICAL: Staggered start times prevent AI service rate limiting
  // 23 parallel tests hitting Gemini API simultaneously causes "no content in candidate" errors
  // Stagger by 3s intervals to spread load: 0s, 3s, 6s, 9s, ... (69s max delay)
  scenarios: {
    test_type_text: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_TEXT', startTime: '0s' },
    test_type_markdown: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_MARKDOWN', startTime: '3s' },
    test_type_csv: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_CSV', startTime: '6s' },
    test_type_html: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_HTML', startTime: '9s' },
    test_type_pdf: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_PDF', startTime: '12s' },
    test_type_ppt: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_PPT', startTime: '15s' },
    test_type_pptx: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_PPTX', startTime: '18s' },
    test_type_xls: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_XLS', startTime: '21s' },
    test_type_xlsx: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_XLSX', startTime: '24s' },
    test_type_doc: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_DOC', startTime: '27s' },
    test_type_docx: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_DOCX', startTime: '30s' },
    test_type_doc_uppercase: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_DOC_UPPERCASE', startTime: '33s' },
    test_type_docx_uppercase: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_DOCX_UPPERCASE', startTime: '36s' },
    // Regression tests: Type inference from filename (type field omitted)
    // These tests ensure the backend correctly infers file type from extension when type is not provided
    test_type_text_inferred: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_TEXT_INFERRED', startTime: '39s' },
    test_type_markdown_inferred: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_MARKDOWN_INFERRED', startTime: '42s' },
    test_type_csv_inferred: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_CSV_INFERRED', startTime: '45s' },
    test_type_html_inferred: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_HTML_INFERRED', startTime: '48s' },
    test_type_pdf_inferred: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_PDF_INFERRED', startTime: '51s' },
    test_type_ppt_inferred: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_PPT_INFERRED', startTime: '54s' },
    test_type_pptx_inferred: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_PPTX_INFERRED', startTime: '57s' },
    test_type_xls_inferred: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_XLS_INFERRED', startTime: '60s' },
    test_type_xlsx_inferred: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_XLSX_INFERRED', startTime: '63s' },
    test_type_doc_inferred: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_DOC_INFERRED', startTime: '66s' },
    test_type_docx_inferred: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_DOCX_INFERRED', startTime: '69s' },
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
  const groupName = "Artifact API: Delete all knowledge bases and data created by this test";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    // CRITICAL: Wait for THIS TEST's file processing to complete before deleting knowledge bases
    // Deleting knowledge bases triggers cleanup workflows that drop vector DB collections
    // If we delete while files are still processing, we get "collection does not exist" errors
    // IMPORTANT: We MUST NOT proceed with deletion if files are still processing, as this creates
    // zombie workflows that continue running after the KB/files are deleted.
    console.log("Teardown: Waiting for this test's file processing to complete...");
    const allProcessingComplete = helper.waitForAllFileProcessingComplete(600, data.dbIDPrefix); // Increased to 10 minutes for AI-intensive operations

    check({ allProcessingComplete }, {
      "Teardown: All files processed before cleanup (no zombie workflows)": () => allProcessingComplete === true,
    });

    if (!allProcessingComplete) {
      console.error("Teardown: Files still processing after timeout - CANNOT safely delete knowledge bases");
      console.error("Teardown: Leaving knowledge bases in place to avoid zombie workflows");
      console.error("Teardown: Manual cleanup may be required or increase timeout");
      // CRITICAL: Do NOT proceed with deletion - this would create zombie workflows
      // Better to leave test artifacts than to create workflows that fail with "collection does not exist"
      return;
    }

    // Delete knowledge bases via API (which triggers cleanup workflows)
    var listResp = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases`, null, data.header)
    if (listResp.status === 200) {
      var knowledgeBases = Array.isArray(listResp.json().knowledgeBases) ? listResp.json().knowledgeBases : []
      for (const kb of knowledgeBases) {
        // API returns id (AIP-compliant)
        const kbId = kb.id;
        if (kbId && kbId.startsWith(data.dbIDPrefix)) {
          var delResp = http.request("DELETE", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${kbId}`, null, data.header);
          check(delResp, {
            [`DELETE /v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${kbId} response status is 200 or 404`]: (r) => r.status === 200 || r.status === 404,
          });
        }
      }
    }
  });
}


// Scenario execs per file type
export function TEST_TYPE_TEXT(data) { runKnowledgeBaseFileTest(data, { originalName: "sample.txt", fileType: "TYPE_TEXT" }); }
export function TEST_TYPE_MARKDOWN(data) { runKnowledgeBaseFileTest(data, { originalName: "sample.md", fileType: "TYPE_MARKDOWN" }); }
export function TEST_TYPE_CSV(data) { runKnowledgeBaseFileTest(data, { originalName: "sample.csv", fileType: "TYPE_CSV" }); }
export function TEST_TYPE_HTML(data) { runKnowledgeBaseFileTest(data, { originalName: "sample.html", fileType: "TYPE_HTML" }); }
export function TEST_TYPE_PDF(data) { runKnowledgeBaseFileTest(data, { originalName: "sample.pdf", fileType: "TYPE_PDF" }); }
export function TEST_TYPE_PPT(data) { runKnowledgeBaseFileTest(data, { originalName: "sample.ppt", fileType: "TYPE_PPT" }); }
export function TEST_TYPE_PPTX(data) { runKnowledgeBaseFileTest(data, { originalName: "sample.pptx", fileType: "TYPE_PPTX" }); }
export function TEST_TYPE_XLS(data) { runKnowledgeBaseFileTest(data, { originalName: "sample.xls", fileType: "TYPE_XLS" }); }
export function TEST_TYPE_XLSX(data) { runKnowledgeBaseFileTest(data, { originalName: "sample.xlsx", fileType: "TYPE_XLSX" }); }
export function TEST_TYPE_DOC(data) { runKnowledgeBaseFileTest(data, { originalName: "sample.doc", fileType: "TYPE_DOC" }); }
export function TEST_TYPE_DOCX(data) { runKnowledgeBaseFileTest(data, { originalName: "sample.docx", fileType: "TYPE_DOCX" }); }
export function TEST_TYPE_DOC_UPPERCASE(data) { runKnowledgeBaseFileTest(data, { originalName: "SAMPLE-UPPERCASE-FILENAME.DOC", fileType: "TYPE_DOC" }); }
export function TEST_TYPE_DOCX_UPPERCASE(data) { runKnowledgeBaseFileTest(data, { originalName: "SAMPLE-UPPERCASE-FILENAME.DOCX", fileType: "TYPE_DOCX" }); }

// Regression tests: Type inference from filename (type field omitted)
// These tests ensure the backend correctly infers file type from extension
export function TEST_TYPE_TEXT_INFERRED(data) { runKnowledgeBaseFileTest(data, { originalName: "sample.txt", fileType: "TYPE_TEXT", omitType: true }); }
export function TEST_TYPE_MARKDOWN_INFERRED(data) { runKnowledgeBaseFileTest(data, { originalName: "sample.md", fileType: "TYPE_MARKDOWN", omitType: true }); }
export function TEST_TYPE_CSV_INFERRED(data) { runKnowledgeBaseFileTest(data, { originalName: "sample.csv", fileType: "TYPE_CSV", omitType: true }); }
export function TEST_TYPE_HTML_INFERRED(data) { runKnowledgeBaseFileTest(data, { originalName: "sample.html", fileType: "TYPE_HTML", omitType: true }); }
export function TEST_TYPE_PDF_INFERRED(data) { runKnowledgeBaseFileTest(data, { originalName: "sample.pdf", fileType: "TYPE_PDF", omitType: true }); }
export function TEST_TYPE_PPT_INFERRED(data) { runKnowledgeBaseFileTest(data, { originalName: "sample.ppt", fileType: "TYPE_PPT", omitType: true }); }
export function TEST_TYPE_PPTX_INFERRED(data) { runKnowledgeBaseFileTest(data, { originalName: "sample.pptx", fileType: "TYPE_PPTX", omitType: true }); }
export function TEST_TYPE_XLS_INFERRED(data) { runKnowledgeBaseFileTest(data, { originalName: "sample.xls", fileType: "TYPE_XLS", omitType: true }); }
export function TEST_TYPE_XLSX_INFERRED(data) { runKnowledgeBaseFileTest(data, { originalName: "sample.xlsx", fileType: "TYPE_XLSX", omitType: true }); }
export function TEST_TYPE_DOC_INFERRED(data) { runKnowledgeBaseFileTest(data, { originalName: "sample.doc", fileType: "TYPE_DOC", omitType: true }); }
export function TEST_TYPE_DOCX_INFERRED(data) { runKnowledgeBaseFileTest(data, { originalName: "sample.docx", fileType: "TYPE_DOCX", omitType: true }); }

// Internal helper to run knowledge base file test for each file type
function runKnowledgeBaseFileTest(data, opts) {
  const groupName = "Artifact API: Knowledge base file type test";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    const { fileType, originalName, omitType } = opts || {};

    // Create knowledge base (id must be < 32 chars: test-{4}-src-{8} = 23 chars)
    const cRes = http.request("POST", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases`, JSON.stringify({ id: data.dbIDPrefix + "src-" + randomString(8) }), data.header);
    logUnexpected(cRes, 'POST /v1alpha/namespaces/{namespace_id}/knowledge-bases');
    const kb = ((() => { try { return cRes.json(); } catch (e) { return {}; } })()).knowledgeBase || {};
    const knowledgeBaseId = kb.id;
    check(cRes, { [`POST /v1alpha/namespaces/{namespace_id}/knowledge-bases 200 (${knowledgeBaseId})`]: (r) => r.status === 200 });

    // List knowledge bases and ensure our knowledge base is present
    const listKBRes = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases`, null, data.header);
    logUnexpected(listKBRes, 'GET /v1alpha/namespaces/{namespace_id}/knowledge-bases');
    let listKBJson; try { listKBJson = listKBRes.json(); } catch (e) { listKBJson = {}; }
    const kbArr = Array.isArray(listKBJson.knowledgeBases) ? listKBJson.knowledgeBases : [];
    const containsUploadedKB = kbArr.some((c) => c.id === knowledgeBaseId);
    check(listKBRes, {
      [`GET /v1alpha/namespaces/{namespace_id}/knowledge-bases 200 (${knowledgeBaseId})`]: (r) => r.status === 200,
      [`List contains uploaded knowledge base (${knowledgeBaseId})`]: () => containsUploadedKB,
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

    const uRes = http.request("POST", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files`, JSON.stringify(fReq), data.header);
    logUnexpected(uRes, 'POST /v1alpha/namespaces/{namespace_id}/knowledge-bases/{knowledge_base_id}/files');
    const file = ((() => { try { return uRes.json(); } catch (e) { return {}; } })()).file || {};
    const fileUid = file.uid;
    const testLabel = omitType ? `${fileType} [TYPE INFERRED]` : fileType;
    if (!fileUid) {
      console.log(`WARN ${testLabel}: No fileUid in response. Response status: ${uRes.status}, body: ${uRes.body}`);
    }
    check(uRes, { [`POST /v1alpha/namespaces/{namespace_id}/knowledge-bases/{knowledge_base_id}/files 200 (${testLabel})`]: (r) => r.status === 200 });

    // List knowledge base files and ensure our file is present
    const listKBFilesRes = http.request(
      "GET",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files`,
      null,
      data.header
    );
    logUnexpected(listKBFilesRes, 'GET /v1alpha/namespaces/{namespace_id}/knowledge-bases/{knowledge_base_id}/files');
    let listKBFilesJson; try { listKBFilesJson = listKBFilesRes.json(); } catch (e) { listKBFilesJson = {}; }
    const kbFilesArr = Array.isArray(listKBFilesJson.files) ? listKBFilesJson.files : [];
    const containsUploadedKBFile = kbFilesArr.some((f) => f.uid === fileUid);
    check(listKBFilesRes, {
      [`GET /v1alpha/namespaces/{namespace_id}/knowledge-bases/{knowledge_base_id}/files 200 (${testLabel})`]: (r) => r.status === 200,
      [`List contains uploaded file (${testLabel})`]: () => containsUploadedKBFile,
    });

    // GET single knowledge base file and validate
    const getKBFileRes = http.request(
      "GET",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files/${fileUid}`,
      null,
      data.header
    );
    logUnexpected(getKBFileRes, 'GET /v1alpha/namespaces/{namespace_id}/knowledge-bases/{knowledge_base_id}/files/{file_uid}');
    let getKBFileJson; try { getKBFileJson = getKBFileRes.json(); } catch (e) { getKBFileJson = {}; }
    check(getKBFileRes, {
      [`GET /v1alpha/namespaces/{namespace_id}/knowledge-bases/{knowledge_base_id}/files/{file_uid} 200 (${testLabel})`]: (r) => r.status === 200,
      [`GET /v1alpha/namespaces/{namespace_id}/knowledge-bases/{knowledge_base_id}/files/{file_uid} uid matches (${testLabel})`]: () => getKBFileJson.file && getKBFileJson.file.uid === fileUid,
      [`GET /v1alpha/namespaces/{namespace_id}/knowledge-bases/{knowledge_base_id}/files/{file_uid} filename matches (${testLabel})`]: () => getKBFileJson.file && getKBFileJson.file.filename === filename,
      [`GET /v1alpha/namespaces/{namespace_id}/knowledge-bases/{knowledge_base_id}/files/{file_uid} is valid (${testLabel})`]: () => getKBFileJson.file && helper.validateFile(getKBFileJson.file, false),
    });

    // Auto-trigger: Processing starts automatically on upload (no manual trigger needed)
    // Wait for file processing to complete using robust helper function
    console.log(`⏳ Waiting for file processing: ${testLabel} (fileUid: ${fileUid})...`);
    const result = helper.waitForFileProcessingComplete(
      data.expectedOwner.id,
      knowledgeBaseId,
      fileUid,
      data.header,
      900, // Max 900 seconds (15 minutes) for AI-intensive conversions with potential rate limiting
      240  // Fast-fail after 240s if stuck in NOTSTARTED (increased for AI service delays in CI)
    );

    const completed = result.completed && result.status === "COMPLETED";
    const failed = result.status === "FAILED";
    const failureReason = result.error || "";

    if (failed) {
      console.error(`✗ File processing failed for ${testLabel}: ${failureReason}`);
      console.error(`   File UID: ${fileUid}`);
      console.error(`   Knowledge Base ID: ${knowledgeBaseId}`);

      // Check if failure is due to AI service issues (rate limiting or instability)
      if (failureReason.includes("no content in candidate") ||
        failureReason.includes("rate limit") ||
        failureReason.includes("AI service") ||
        failureReason.includes("temporarily unavailable")) {
        console.error(`   ⚠ AI SERVICE ISSUE: This appears to be a transient AI service failure.`);
        console.error(`   The backend already retries (3 AI client + 8 Temporal activity attempts).`);
        console.error(`   With exponential backoff, this allows up to ~3 minutes for recovery.`);
        console.error(`   If failures persist, consider:`);
        console.error(`     1. Reduce parallel test execution (current: 23 scenarios with 3s stagger)`);
        console.error(`     2. Check AI service quota/rate limits`);
        console.error(`     3. Verify AI service is healthy and responsive`);
      }
    } else if (!completed) {
      console.error(`✗ File processing did not complete for ${testLabel}`);
      console.error(`   Status: ${result.status}`);
      console.error(`   Error: ${failureReason || 'None'}`);
      console.error(`   File UID: ${fileUid}`);
      console.error(`   Knowledge Base ID: ${knowledgeBaseId}`);

      // On resource-constrained systems, provide helpful troubleshooting info
      if (result.status === "TIMEOUT") {
        console.error(`   TIMEOUT - Possible causes:`);
        console.error(`     1. Worker overloaded - too many parallel files processing`);
        console.error(`     2. File too large/complex for available resources`);
        console.error(`     3. External dependencies (Milvus, MinIO) slow to respond`);
        console.error(`   Consider: Increase maxWaitSeconds or reduce parallel test execution`);
      } else if (result.status === "WORKFLOW_NOT_STARTED") {
        console.error(`   WORKFLOW_NOT_STARTED - Workflow never triggered:`);
        console.error(`     1. Temporal worker may be down or overloaded`);
        console.error(`     2. Worker queue may be full`);
        console.error(`     3. Auto-trigger mechanism may have failed`);
      } else if (result.status === "API_ERROR") {
        console.error(`   API_ERROR - API consistently returning errors:`);
        console.error(`     1. Backend may be overloaded or restarting`);
        console.error(`     2. Network/connectivity issues`);
        console.error(`     3. Rate limiting or resource exhaustion`);
      }
    }

    check({ completed, failed }, {
      [`GET /v1alpha/namespaces/{namespace_id}/knowledge-bases/{knowledge_base_id}/files/{file_uid} 200 and Process Status Reached COMPLETED (${testLabel})`]: () => completed === true,
      [`File processing did not fail (${testLabel})`]: () => !failed,
    });

    if (failed) {
      console.log(`✗ Skipping remaining checks for ${testLabel} due to processing failure: ${failureReason}`);
      // Don't delete here - let teardown handle cleanup
      return;
    }

    // Get file content (using VIEW_CONTENT)
    const getKBFileContent = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files/${fileUid}?view=VIEW_CONTENT`, null, data.header);
    logUnexpected(getKBFileContent, 'GET /v1alpha/namespaces/{namespace_id}/knowledge-bases/{knowledge_base_id}/files/{file_uid}?view=VIEW_CONTENT');
    let contentData; try { contentData = getKBFileContent.json(); } catch (e) { contentData = {}; }
    const contentUri = contentData.derivedResourceUri || ""; // derivedResourceUri is at top level, not inside .file
    check(getKBFileContent, {
      [`GET /v1alpha/namespaces/{namespace_id}/knowledge-bases/{knowledge_base_id}/files/{file_uid}?view=VIEW_CONTENT 200 (${testLabel})`]: (r) => r.status === 200,
      [`VIEW_CONTENT returns derivedResourceUri (${testLabel})`]: () => contentUri && contentUri.length > 0,
    });

    // Get file summary (using VIEW_SUMMARY)
    const getSummaryRes = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files/${fileUid}?view=VIEW_SUMMARY`, null, data.header);
    logUnexpected(getSummaryRes, 'GET /v1alpha/namespaces/{namespace_id}/knowledge-bases/{knowledge_base_id}/files/{file_uid}?view=VIEW_SUMMARY');
    let summaryData; try { summaryData = getSummaryRes.json(); } catch (e) { summaryData = {}; }
    const summaryUri = summaryData.derivedResourceUri || ""; // derivedResourceUri is at top level, not inside .file
    check(getSummaryRes, {
      [`GET /v1alpha/namespaces/{namespace_id}/knowledge-bases/{knowledge_base_id}/files/{file_uid}?view=VIEW_SUMMARY 200 (${testLabel})`]: (r) => r.status === 200,
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

    const getStandardRes = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files/${fileUid}?view=VIEW_STANDARD_FILE_TYPE`, null, data.header);
    logUnexpected(getStandardRes, 'GET /v1alpha/namespaces/{namespace_id}/knowledge-bases/{knowledge_base_id}/files/{file_uid}?view=VIEW_STANDARD_FILE_TYPE');
    let standardData; try { standardData = getStandardRes.json(); } catch (e) { standardData = {}; }
    const standardUri = standardData.derivedResourceUri || ""; // derivedResourceUri is at top level, not inside .file

    if (isStandardizable) {
      check(getStandardRes, {
        [`GET /v1alpha/namespaces/{namespace_id}/knowledge-bases/{knowledge_base_id}/files/{file_uid}?view=VIEW_STANDARD_FILE_TYPE 200 (${testLabel})`]: (r) => r.status === 200,
        [`VIEW_STANDARD_FILE_TYPE returns derivedResourceUri for standardizable type (${testLabel})`]: () => standardUri && standardUri.length > 0,
      });

      // Verify the URL is accessible (basic check)
      if (standardUri) {
        console.log(`✓ VIEW_STANDARD_FILE_TYPE returned URL for ${testLabel}: ${standardUri.substring(0, 50)}...`);
      }
    } else {
      // For non-standardizable types (TEXT, MARKDOWN, CSV, HTML), VIEW_STANDARD_FILE_TYPE should still return 200 but may not have a standardized file
      check(getStandardRes, {
        [`GET /v1alpha/namespaces/{namespace_id}/knowledge-bases/{knowledge_base_id}/files/{file_uid}?view=VIEW_STANDARD_FILE_TYPE 200 (${testLabel})`]: (r) => r.status === 200,
      });
      if (standardUri) {
        console.log(`INFO: Unexpected standardized file available for non-standardizable type ${testLabel}`);
      }
    }

    // Get original file (using VIEW_ORIGINAL_FILE_TYPE)
    // This should return the original uploaded file for ALL file types
    const getOriginalRes = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files/${fileUid}?view=VIEW_ORIGINAL_FILE_TYPE`, null, data.header);
    logUnexpected(getOriginalRes, 'GET /v1alpha/namespaces/{namespace_id}/knowledge-bases/{knowledge_base_id}/files/{file_uid}?view=VIEW_ORIGINAL_FILE_TYPE');
    let originalData; try { originalData = getOriginalRes.json(); } catch (e) { originalData = {}; }
    const originalUri = originalData.derivedResourceUri || ""; // derivedResourceUri is at top level, not inside .file

    check(getOriginalRes, {
      [`GET /v1alpha/namespaces/{namespace_id}/knowledge-bases/{knowledge_base_id}/files/{file_uid}?view=VIEW_ORIGINAL_FILE_TYPE 200 (${testLabel})`]: (r) => r.status === 200,
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
    const listChunksUrl = `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files/${fileUid}/chunks`;
    const listChunksRes = http.request("GET", listChunksUrl, null, data.header);
    logUnexpected(listChunksRes, 'GET /v1alpha/namespaces/{namespace_id}/knowledge-bases/{knowledge_base_id}/files/{file_id}/chunks');
    let listChunksJson; try { listChunksJson = listChunksRes.json(); } catch (e) { listChunksJson = {}; }
    check(listChunksRes, {
      [`GET /v1alpha/namespaces/{namespace_id}/knowledge-bases/{knowledge_base_id}/files/{file_id}/chunks 200 (${testLabel})`]: (r) => r.status === 200,
    });
  });
}
