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
    // Document file types
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
    // Regression tests: Type inference from filename (type field omitted)
    // These tests ensure the backend correctly infers file type from extension when type is not provided
    test_type_markdown_inferred: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_MARKDOWN_INFERRED' },
    test_type_text_inferred: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_TEXT_INFERRED' },
    test_type_csv_inferred: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_CSV_INFERRED' },
    test_type_html_inferred: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_HTML_INFERRED' },
    test_type_pdf_inferred: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_PDF_INFERRED' },
    test_type_ppt_inferred: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_PPT_INFERRED' },
    test_type_pptx_inferred: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_PPTX_INFERRED' },
    test_type_xls_inferred: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_XLS_INFERRED' },
    test_type_xlsx_inferred: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_XLSX_INFERRED' },
    test_type_doc_inferred: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_DOC_INFERRED' },
    test_type_docx_inferred: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_DOCX_INFERRED' },
    // Image file types
    test_type_png: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_PNG' },
    test_type_jpeg: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_JPEG' },
    test_type_jpg: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_JPG' },
    test_type_gif: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_GIF' },
    test_type_webp: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_WEBP' },
    test_type_tiff: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_TIFF' },
    test_type_tif: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_TIF' },
    test_type_heic: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_HEIC' },
    test_type_heif: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_HEIF' },
    test_type_avif: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_AVIF' },
    test_type_bmp: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_BMP' },
    // test_type_mp3: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_MP3' },
    // Audio file types
    test_type_wav: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_WAV' },
    test_type_aac: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_AAC' },
    test_type_ogg: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_OGG' },
    test_type_flac: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_FLAC' },
    test_type_aiff: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_AIFF' },
    test_type_m4a: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_M4A' },
    test_type_wma: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_WMA' },
    test_type_webm_audio: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_WEBM_AUDIO' },
    // Video file types
    test_type_mp4: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_MP4' },
    test_type_mkv: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_MKV' },
    test_type_avi: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_AVI' },
    test_type_mov: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_MOV' },
    test_type_flv: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_FLV' },
    test_type_wmv: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_WMV' },
    test_type_mpeg: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_MPEG' },
    test_type_webm_video: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_TYPE_WEBM_VIDEO' },
    // Regression test: Verify process_status is always string enum, never integer
    test_process_status_format: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_PROCESS_STATUS_FORMAT' },
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
export function TEST_TYPE_TEXT(data) { runKnowledgeBaseFileTest(data, { originalName: "doc-sample.txt", fileType: "TYPE_TEXT" }); }
export function TEST_TYPE_MARKDOWN(data) { runKnowledgeBaseFileTest(data, { originalName: "doc-sample.md", fileType: "TYPE_MARKDOWN" }); }
export function TEST_TYPE_CSV(data) { runKnowledgeBaseFileTest(data, { originalName: "doc-sample.csv", fileType: "TYPE_CSV" }); }
export function TEST_TYPE_HTML(data) { runKnowledgeBaseFileTest(data, { originalName: "doc-sample.html", fileType: "TYPE_HTML" }); }
export function TEST_TYPE_PDF(data) { runKnowledgeBaseFileTest(data, { originalName: "doc-sample.pdf", fileType: "TYPE_PDF" }); }
export function TEST_TYPE_PPT(data) { runKnowledgeBaseFileTest(data, { originalName: "doc-sample.ppt", fileType: "TYPE_PPT" }); }
export function TEST_TYPE_PPTX(data) { runKnowledgeBaseFileTest(data, { originalName: "doc-sample.pptx", fileType: "TYPE_PPTX" }); }
export function TEST_TYPE_XLS(data) { runKnowledgeBaseFileTest(data, { originalName: "doc-sample.xls", fileType: "TYPE_XLS" }); }
export function TEST_TYPE_XLSX(data) { runKnowledgeBaseFileTest(data, { originalName: "doc-sample.xlsx", fileType: "TYPE_XLSX" }); }
export function TEST_TYPE_DOC(data) { runKnowledgeBaseFileTest(data, { originalName: "doc-sample.doc", fileType: "TYPE_DOC" }); }
export function TEST_TYPE_DOCX(data) { runKnowledgeBaseFileTest(data, { originalName: "doc-sample.docx", fileType: "TYPE_DOCX" }); }

// Regression tests: Type inference from filename (type field omitted)
// These tests ensure the backend correctly infers file type from extension
export function TEST_TYPE_TEXT_INFERRED(data) { runKnowledgeBaseFileTest(data, { originalName: "doc-sample.txt", fileType: "TYPE_TEXT", omitType: true }); }
export function TEST_TYPE_MARKDOWN_INFERRED(data) { runKnowledgeBaseFileTest(data, { originalName: "doc-sample.md", fileType: "TYPE_MARKDOWN", omitType: true }); }
export function TEST_TYPE_CSV_INFERRED(data) { runKnowledgeBaseFileTest(data, { originalName: "doc-sample.csv", fileType: "TYPE_CSV", omitType: true }); }
export function TEST_TYPE_HTML_INFERRED(data) { runKnowledgeBaseFileTest(data, { originalName: "doc-sample.html", fileType: "TYPE_HTML", omitType: true }); }
export function TEST_TYPE_PDF_INFERRED(data) { runKnowledgeBaseFileTest(data, { originalName: "doc-sample.pdf", fileType: "TYPE_PDF", omitType: true }); }
export function TEST_TYPE_PPT_INFERRED(data) { runKnowledgeBaseFileTest(data, { originalName: "doc-sample.ppt", fileType: "TYPE_PPT", omitType: true }); }
export function TEST_TYPE_PPTX_INFERRED(data) { runKnowledgeBaseFileTest(data, { originalName: "doc-sample.pptx", fileType: "TYPE_PPTX", omitType: true }); }
export function TEST_TYPE_XLS_INFERRED(data) { runKnowledgeBaseFileTest(data, { originalName: "doc-sample.xls", fileType: "TYPE_XLS", omitType: true }); }
export function TEST_TYPE_XLSX_INFERRED(data) { runKnowledgeBaseFileTest(data, { originalName: "doc-sample.xlsx", fileType: "TYPE_XLSX", omitType: true }); }
export function TEST_TYPE_DOC_INFERRED(data) { runKnowledgeBaseFileTest(data, { originalName: "doc-sample.doc", fileType: "TYPE_DOC", omitType: true }); }
export function TEST_TYPE_DOCX_INFERRED(data) { runKnowledgeBaseFileTest(data, { originalName: "doc-sample.docx", fileType: "TYPE_DOCX", omitType: true }); }

// Image file types
export function TEST_TYPE_PNG(data) { runKnowledgeBaseFileTest(data, { originalName: "img-sample.png", fileType: "TYPE_PNG" }); }
export function TEST_TYPE_JPEG(data) { runKnowledgeBaseFileTest(data, { originalName: "img-sample.jpeg", fileType: "TYPE_JPEG" }); }
export function TEST_TYPE_JPG(data) { runKnowledgeBaseFileTest(data, { originalName: "img-sample.jpg", fileType: "TYPE_JPEG" }); }
export function TEST_TYPE_JPG_INFERRED(data) { runKnowledgeBaseFileTest(data, { originalName: "img-sample.jpg", fileType: "TYPE_JPEG", omitType: true }); }
export function TEST_TYPE_GIF(data) { runKnowledgeBaseFileTest(data, { originalName: "img-sample.gif", fileType: "TYPE_GIF" }); }
export function TEST_TYPE_WEBP(data) { runKnowledgeBaseFileTest(data, { originalName: "img-sample.webp", fileType: "TYPE_WEBP" }); }
export function TEST_TYPE_TIFF(data) { runKnowledgeBaseFileTest(data, { originalName: "img-sample.tiff", fileType: "TYPE_TIFF" }); }
export function TEST_TYPE_TIF(data) { runKnowledgeBaseFileTest(data, { originalName: "img-sample.tif", fileType: "TYPE_TIFF" }); }
export function TEST_TYPE_TIF_INFERRED(data) { runKnowledgeBaseFileTest(data, { originalName: "img-sample.tif", fileType: "TYPE_TIFF", omitType: true }); }
export function TEST_TYPE_HEIC(data) { runKnowledgeBaseFileTest(data, { originalName: "img-sample.heic", fileType: "TYPE_HEIC" }); }
export function TEST_TYPE_HEIF(data) { runKnowledgeBaseFileTest(data, { originalName: "img-sample.heif", fileType: "TYPE_HEIF" }); }
export function TEST_TYPE_AVIF(data) { runKnowledgeBaseFileTest(data, { originalName: "img-sample.avif", fileType: "TYPE_AVIF" }); }
export function TEST_TYPE_BMP(data) { runKnowledgeBaseFileTest(data, { originalName: "img-sample.bmp", fileType: "TYPE_BMP" }); }

// Audio file types
export function TEST_TYPE_MP3(data) { runKnowledgeBaseFileTest(data, { originalName: "audio-sample.mp3", fileType: "TYPE_MP3" }); }
export function TEST_TYPE_WAV(data) { runKnowledgeBaseFileTest(data, { originalName: "audio-sample.wav", fileType: "TYPE_WAV" }); }
export function TEST_TYPE_AAC(data) { runKnowledgeBaseFileTest(data, { originalName: "audio-sample.aac", fileType: "TYPE_AAC" }); }
export function TEST_TYPE_OGG(data) { runKnowledgeBaseFileTest(data, { originalName: "audio-sample.ogg", fileType: "TYPE_OGG" }); }
export function TEST_TYPE_FLAC(data) { runKnowledgeBaseFileTest(data, { originalName: "audio-sample.flac", fileType: "TYPE_FLAC" }); }
export function TEST_TYPE_AIFF(data) { runKnowledgeBaseFileTest(data, { originalName: "audio-sample.aiff", fileType: "TYPE_AIFF" }); }
export function TEST_TYPE_M4A(data) { runKnowledgeBaseFileTest(data, { originalName: "audio-sample.m4a", fileType: "TYPE_M4A" }); }
export function TEST_TYPE_WMA(data) { runKnowledgeBaseFileTest(data, { originalName: "audio-sample.wma", fileType: "TYPE_WMA" }); }
export function TEST_TYPE_WEBM_AUDIO(data) { runKnowledgeBaseFileTest(data, { originalName: "audio-sample.webm", fileType: "TYPE_WEBM_AUDIO" }); }

// Video file types
export function TEST_TYPE_MP4(data) { runKnowledgeBaseFileTest(data, { originalName: "video-sample.mp4", fileType: "TYPE_MP4" }); }
export function TEST_TYPE_MKV(data) { runKnowledgeBaseFileTest(data, { originalName: "video-sample.mkv", fileType: "TYPE_MKV" }); }
export function TEST_TYPE_AVI(data) { runKnowledgeBaseFileTest(data, { originalName: "video-sample.avi", fileType: "TYPE_AVI" }); }
export function TEST_TYPE_MOV(data) { runKnowledgeBaseFileTest(data, { originalName: "video-sample.mov", fileType: "TYPE_MOV" }); }
export function TEST_TYPE_FLV(data) { runKnowledgeBaseFileTest(data, { originalName: "video-sample.flv", fileType: "TYPE_FLV" }); }
export function TEST_TYPE_WMV(data) { runKnowledgeBaseFileTest(data, { originalName: "video-sample.wmv", fileType: "TYPE_WMV" }); }
export function TEST_TYPE_MPEG(data) { runKnowledgeBaseFileTest(data, { originalName: "video-sample.mpeg", fileType: "TYPE_MPEG" }); }
export function TEST_TYPE_WEBM_VIDEO(data) { runKnowledgeBaseFileTest(data, { originalName: "video-sample.webm", fileType: "TYPE_WEBM_VIDEO" }); }

// Regression test: Verify all enum fields are always stored as string enums, never as integers
// This test guards against a bug where protobuf enum values could be accidentally stored as integers
// Covered enums: file_type, process_status, update_status, converted_type, chunk_type
export function TEST_PROCESS_STATUS_FORMAT(data) {
  const groupName = "Artifact API: Enum format validation (string enum, not integer)";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    // Create KB and upload a simple file
    const cRes = http.request("POST", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases`, JSON.stringify({ id: data.dbIDPrefix + "src-" + randomString(8) }), data.header);
    logUnexpected(cRes, 'POST /v1alpha/namespaces/{namespace_id}/knowledge-bases');
    const kb = ((() => { try { return cRes.json(); } catch (e) { return {}; } })()).knowledgeBase || {};
    const knowledgeBaseId = kb.id;
    check(cRes, { [`POST /v1alpha/namespaces/{namespace_id}/knowledge-bases 200 (${knowledgeBaseId})`]: (r) => r.status === 200 });

    // Upload a simple text file for fast processing
    const s = constant.sampleFiles.find((x) => x.originalName === "doc-sample.txt") || {};
    const filename = data.dbIDPrefix + "process-status-test.txt";

    const uRes = http.request("POST", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files`, JSON.stringify({ filename: filename, type: "TYPE_TEXT", content: s.content || "" }), data.header);
    logUnexpected(uRes, 'POST /v1alpha/namespaces/{namespace_id}/knowledge-bases/{knowledge_base_id}/files');
    const file = ((() => { try { return uRes.json(); } catch (e) { return {}; } })()).file || {};
    const fileUid = file.uid;
    check(uRes, { [`POST /v1alpha/namespaces/{namespace_id}/knowledge-bases/{knowledge_base_id}/files 200`]: (r) => r.status === 200 });

    // CRITICAL CHECK: Verify enum fields in API response are string enums, not integers
    // The API should return the enum NAMES (e.g., "TYPE_TEXT"), not the values (e.g., 1)
    const fileType = file.type;
    const initialProcessStatus = file.processStatus;
    const isValidFileType = typeof fileType === 'string' && fileType.startsWith('TYPE_');
    const isValidProcessStatus = typeof initialProcessStatus === 'string' && initialProcessStatus.startsWith('FILE_PROCESS_STATUS_');

    check({ fileType, initialProcessStatus }, {
      "file_type is a string (not integer)": () => typeof fileType === 'string',
      "file_type starts with 'TYPE_'": () => isValidFileType,
      "file_type is TYPE_TEXT": () => fileType === 'TYPE_TEXT',
      "process_status is a string (not integer)": () => typeof initialProcessStatus === 'string',
      "process_status starts with 'FILE_PROCESS_STATUS_'": () => isValidProcessStatus,
      "process_status is NOTSTARTED": () => initialProcessStatus === 'FILE_PROCESS_STATUS_NOTSTARTED',
    });

    if (!isValidFileType) {
      console.error(`❌ CRITICAL BUG: file_type is '${fileType}' (type: ${typeof fileType})`);
      console.error(`   Expected: String starting with 'TYPE_' (e.g., 'TYPE_TEXT')`);
      console.error(`   Got: ${JSON.stringify(fileType)}`);
    }
    if (!isValidProcessStatus) {
      console.error(`❌ CRITICAL BUG: process_status is '${initialProcessStatus}' (type: ${typeof initialProcessStatus})`);
      console.error(`   Expected: String starting with 'FILE_PROCESS_STATUS_' (e.g., 'FILE_PROCESS_STATUS_NOTSTARTED')`);
      console.error(`   Got: ${JSON.stringify(initialProcessStatus)}`);
    }

    // Wait for file to start processing and check status again
    sleep(2);
    const processingRes = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files/${fileUid}`, null, data.header);
    let processingFile; try { processingFile = processingRes.json(); } catch (e) { processingFile = {}; }
    const processingStatus = processingFile.file ? processingFile.file.processStatus : "";

    check({ processingStatus }, {
      "Processing status is a string (not integer)": () => typeof processingStatus === 'string',
      "Processing status starts with 'FILE_PROCESS_STATUS_'": () => typeof processingStatus === 'string' && processingStatus.startsWith('FILE_PROCESS_STATUS_'),
    });

    // Wait for file processing to complete
    console.log(`Waiting for file processing to complete to verify final status...`);
    const result = helper.waitForFileProcessingComplete(
      data.expectedOwner.id,
      knowledgeBaseId,
      fileUid,
      data.header,
      120, // 2 minutes should be enough for a simple text file
      30
    );

    if (result.completed && result.status === "COMPLETED") {
      // Final check: Verify completed status is also a string enum
      const completedRes = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files/${fileUid}`, null, data.header);
      let completedFile; try { completedFile = completedRes.json(); } catch (e) { completedFile = {}; }
      const completedStatus = completedFile.file ? completedFile.file.processStatus : "";

      check({ completedStatus }, {
        "Completed status is a string (not integer)": () => typeof completedStatus === 'string',
        "Completed status starts with 'FILE_PROCESS_STATUS_'": () => typeof completedStatus === 'string' && completedStatus.startsWith('FILE_PROCESS_STATUS_'),
        "Completed status is COMPLETED": () => completedStatus === 'FILE_PROCESS_STATUS_COMPLETED',
      });

      console.log(`✓ Process status format validation passed: ${initialProcessStatus} → ${processingStatus} → ${completedStatus}`);
    } else if (result.status === "FAILED") {
      // Even for failed files, status should be a string enum
      const failedRes = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files/${fileUid}`, null, data.header);
      let failedFile; try { failedFile = failedRes.json(); } catch (e) { failedFile = {}; }
      const failedStatus = failedFile.file ? failedFile.file.processStatus : "";

      check({ failedStatus }, {
        "Failed status is a string (not integer)": () => typeof failedStatus === 'string',
        "Failed status starts with 'FILE_PROCESS_STATUS_'": () => typeof failedStatus === 'string' && failedStatus.startsWith('FILE_PROCESS_STATUS_'),
        "Failed status is FAILED": () => failedStatus === 'FILE_PROCESS_STATUS_FAILED',
      });

      console.log(`✓ Process status format validation passed (even for failed file): ${failedStatus}`);
    }
  });
}

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
      [`File has total_tokens field (${testLabel})`]: () => getKBFileJson.file && typeof getKBFileJson.file.totalTokens === 'number',
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

    // After processing completes, verify token count is populated
    // Re-fetch file to get updated token counts from usage metadata
    const getFileAfterProcessing = http.request(
      "GET",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${knowledgeBaseId}/files/${fileUid}`,
      null,
      data.header
    );
    let fileAfterProcessing; try { fileAfterProcessing = getFileAfterProcessing.json(); } catch (e) { fileAfterProcessing = {}; }
    const totalTokens = fileAfterProcessing.file ? fileAfterProcessing.file.totalTokens : 0;
    const totalChunks = fileAfterProcessing.file ? fileAfterProcessing.file.totalChunks : 0;

    check(getFileAfterProcessing, {
      [`File has positive total_tokens after processing (${testLabel})`]: () => totalTokens > 0,
      [`File has positive total_chunks after processing (${testLabel})`]: () => totalChunks > 0,
    });

    if (totalTokens > 0) {
      console.log(`✓ Token count populated for ${testLabel}: ${totalTokens} tokens across ${totalChunks} chunks`);
    } else {
      console.log(`⚠ Token count is zero for ${testLabel} - usage metadata may not be populated yet`);
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
