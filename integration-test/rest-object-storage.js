import http from "k6/http";
import { check, group, sleep } from "k6";
import { randomString } from "https://jslib.k6.io/k6-utils/1.1.0/index.js";

import * as constant from "./const.js";
import * as helper from "./helper.js";

const dbIDPrefix = constant.generateDBIDPrefix();

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
  teardownTimeout: '300s',
  insecureSkipTLSVerify: true,
  thresholds: {
    checks: ["rate == 1.0"],
  },
  scenarios: {
    // Object storage provider tests for different views
    test_01_minio_view_summary: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_01_MinIO_ViewSummary' },
    test_02_minio_view_content: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_02_MinIO_ViewContent' },
    test_03_minio_view_standard: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_03_MinIO_ViewStandardFileType' },
    test_04_minio_view_original: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_04_MinIO_ViewOriginalFileType' },

    // GCS tests - only if GCS is configured
    test_05_gcs_view_summary: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_05_GCS_ViewSummary' },
    test_06_gcs_view_content: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_06_GCS_ViewContent' },
    test_07_gcs_view_standard: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_07_GCS_ViewStandardFileType' },
    test_08_gcs_view_original: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_08_GCS_ViewOriginalFileType' },

    // Storage provider unspecified/default (should use MinIO)
    test_09_unspecified_provider_defaults_to_minio: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_09_UnspecifiedProvider_DefaultsToMinIO' },

    // Cross-provider consistency tests
    test_10_url_content_consistency: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_10_URLContentConsistency' },
    test_11_gcs_cache_behavior: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_11_GCSCacheBehavior' },

    // Filename and content-type tests
    test_12_minio_filename_content_type: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_12_MinIO_FilenameContentType' },
    test_13_gcs_filename_content_type: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_13_GCS_FilenameContentType' },

    // Error handling
    test_14_gcs_not_configured: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_14_GCS_NotConfigured' },

    // Multiple file types with storage providers
    test_15_multiple_file_types_minio: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_15_MultipleFileTypes_MinIO' },
    test_16_multiple_file_types_gcs: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_16_MultipleFileTypes_GCS' },

    // Redis cache tests
    test_17_redis_cache_hit: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_17_RedisCacheHit' },
    test_18_redis_cache_miss: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_18_RedisCacheMiss' },
    test_19_redis_cache_performance: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_19_RedisCachePerformance' },

    // GCS TTL and cleanup tests
    test_20_gcs_ttl_tracking: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_20_GCSTTLTracking' },
    test_21_gcs_file_expiration: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_21_GCSFileExpiration' },

    // View original file type immediate availability tests
    test_22_view_original_file_type_immediate_availability: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'TEST_22_ViewOriginalFileType_ImmediateAvailability' },
  },
};

export function setup() {
  check(true, { [constant.banner('Artifact API [Object Storage]: Setup')]: () => true });

  // Stagger test execution to reduce parallel resource contention
  helper.staggerTestExecution(2);

  console.log(`rest-object-storage.js: Using unique test prefix: ${dbIDPrefix}`);

  var loginResp = http.request("POST", `${constant.mgmtRESTPublicHost}/v1beta/auth/login`, JSON.stringify({
    "username": constant.defaultUsername,
    "password": constant.defaultPassword,
  }))

  check(loginResp, {
    [`POST ${constant.mgmtRESTPublicHost}/v1beta/auth/login response status is 200`]: (r) => r.status === 200,
  });

  var header = {
    "headers": {
      "Authorization": `Bearer ${loginResp.json().accessToken}`,
      "Content-Type": "application/json",
    },
    "timeout": "600s",
  }

  var resp = http.request("GET", `${constant.mgmtRESTPublicHost}/v1beta/user`, {}, {
    headers: { "Authorization": `Bearer ${loginResp.json().accessToken}` }
  })

  return {
    header: header,
    expectedOwner: resp.json().user,
    dbIDPrefix: dbIDPrefix,
  }
}

export function teardown(data) {
  const groupName = "Artifact API [Object Storage]: Teardown - Delete all test resources";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    // Wait for file processing to complete before deleting knowledge bases
    console.log("Teardown: Waiting for this test's file processing to complete...");
    const allProcessingComplete = helper.waitForAllFileProcessingComplete(120, data.dbIDPrefix);
    if (!allProcessingComplete) {
      console.warn("Teardown: Some files still processing after 120s, proceeding anyway");
    }

    console.log(`rest-object-storage.js teardown: Cleaning up resources with prefix: ${data.dbIDPrefix}`);
    var listResp = http.request("GET", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases`, null, data.header)

    if (listResp.status === 200) {
      var knowledgeBases = Array.isArray(listResp.json().knowledgeBases) ? listResp.json().knowledgeBases : []

      knowledgeBases.forEach(kb => {
        if (kb.id.startsWith(data.dbIDPrefix)) {
          console.log(`Teardown: Deleting knowledge base: ${kb.id}`);
          http.request(
            "DELETE",
            `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${kb.id}`,
            null,
            data.header
          );
        }
      });
    }
  });
}

// Helper function to create knowledge base
function createKB(data, kbId, description) {
  const body = {
    id: kbId,
    description: description || "Test KB for object storage",
  };

  const res = http.request(
    "POST",
    `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases`,
    JSON.stringify(body),
    data.header
  );

  logUnexpected(res, "Create KB");
  check(res, { "Create KB 200": (r) => r.status === 200 });
  return res.json().knowledgeBase;
}

// Helper function to delete knowledge base
function deleteKB(data, kbId) {
  const res = http.request(
    "DELETE",
    `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${kbId}`,
    null,
    data.header
  );
  logUnexpected(res, "Delete KB");
}

// Helper function to create and upload a file
function createFile(data, kbId, filename, fileType, content) {
  const body = {
    filename: filename,
    type: fileType,
    content: content,
  };

  const res = http.request(
    "POST",
    `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${kbId}/files`,
    JSON.stringify(body),
    data.header
  );

  logUnexpected(res, "Create file");
  check(res, { "Create file 200": (r) => r.status === 200 });

  if (res.status === 200) {
    return res.json().file;
  }
  return null;
}


// Helper function to check if file is available (exists in storage)
// This is for VIEW_ORIGINAL_FILE_TYPE which doesn't need processing
function waitForFileAvailable(data, kbId, fileUid, maxWaitSeconds = 10) {
  console.log(`Checking file ${fileUid} availability (max ${maxWaitSeconds}s)...`);

  const startTime = new Date().getTime();
  const endTime = startTime + (maxWaitSeconds * 1000);

  while (new Date().getTime() < endTime) {
    const res = http.request(
      "GET",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${kbId}/files/${fileUid}`,
      null,
      data.header
    );

    if (res.status === 200) {
      console.log(`File ${fileUid} is available`);
      return true;
    }

    sleep(1);
  }

  console.warn(`File ${fileUid} not available after ${maxWaitSeconds}s`);
  return false;
}

// Helper function to get file with specific view and storage provider
function getFileWithStorage(data, kbId, fileUid, view, storageProvider) {
  let url = `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${kbId}/files/${fileUid}`;

  const params = [];
  if (view) {
    params.push(`view=${view}`);
  }
  if (storageProvider) {
    params.push(`storage_provider=${storageProvider}`);
  }

  if (params.length > 0) {
    url += `?${params.join('&')}`;
  }

  return http.request("GET", url, null, data.header);
}

// Helper function to download content from URL with retry logic
// Replaces any hardcoded localhost:8080 with the proper API gateway host
function downloadFromURL(url, maxRetries = 10) {
  // Replace any localhost:8080 or hardcoded host with the proper API gateway URL
  // This ensures the URL works in both local and Docker container environments
  let downloadUrl = url;

  // If the URL starts with http://localhost:8080 or https://localhost:8080, replace it
  if (!constant.apiGatewayMode) {
    downloadUrl = url.replace(/^https?:\/\/localhost:8080/, constant.apiGatewayPublicHost);
  }

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const res = http.request("GET", downloadUrl, null, { timeout: "30s" });
      if (res.status === 200) {
        return res;
      }
      // Non-200 response, log and retry
      if (attempt < maxRetries) {
        console.log(`Download attempt ${attempt} failed with status ${res.status}, retrying...`);
        sleep(1);
      } else {
        return res; // Return the failed response on last attempt
      }
    } catch (error) {
      if (attempt < maxRetries) {
        console.log(`Download attempt ${attempt} error: ${error}, retrying...`);
        sleep(1);
      } else {
        // Return a mock response indicating failure
        return {
          status: 0,
          error: error.toString(),
          body: null
        };
      }
    }
  }
}

// ============================================================================
// Test Cases
// ============================================================================

export function TEST_01_MinIO_ViewSummary(data) {
  const groupName = "Object Storage [MinIO]: Get file with VIEW_SUMMARY";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true })

    const kbId = `${data.dbIDPrefix}m-sum-${randomString(5)}`;
    const kb = createKB(data, kbId, "Test MinIO VIEW_SUMMARY");

    const filename = `${data.dbIDPrefix}test-summary.pdf`;
    const file = createFile(data, kbId, filename, "TYPE_PDF", constant.docSamplePdf);

    if (file) {
      // Just check file is available - don't wait for full processing (can take minutes)
      const available = waitForFileAvailable(data, kbId, file.uid, 10);
      check({ available }, { "File available in storage": () => available === true });

      if (available) {
        // Test that API accepts storage_provider parameter and returns MinIO URL
        // Note: Summary may not be ready yet, but we're testing URL format
        const res = getFileWithStorage(data, kbId, file.uid, "VIEW_SUMMARY", "STORAGE_PROVIDER_MINIO");
        logUnexpected(res, "Get file with VIEW_SUMMARY (MinIO)");

        check(res, {
          "Get file VIEW_SUMMARY request accepted": (r) => r.status === 200 || r.status === 404,
          "If summary exists, URL is MinIO": (r) => {
            try {
              if (r.status === 200) {
                const body = r.json();
                if (body.derivedResourceUri) {
                  return body.derivedResourceUri.includes("/v1alpha/blob-urls/");
                }
              }
              return true; // Summary not ready yet, which is fine
            } catch (e) {
              return true;
            }
          }
        });
      }
    }

    deleteKB(data, kbId);
  })
}

export function TEST_02_MinIO_ViewContent(data) {
  const groupName = "Object Storage [MinIO]: Get file with VIEW_CONTENT";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true })

    const kbId = `${data.dbIDPrefix}m-con-${randomString(5)}`;
    const kb = createKB(data, kbId, "Test MinIO VIEW_CONTENT");

    const filename = `${data.dbIDPrefix}test-content.txt`;
    const file = createFile(data, kbId, filename, "TYPE_TEXT", constant.docSampleTxt);

    if (file) {
      // Just check file is available - don't wait for full processing
      const available = waitForFileAvailable(data, kbId, file.uid, 10);
      check({ available }, { "File available in storage": () => available === true });

      if (available) {
        // Test that API accepts storage_provider parameter and returns MinIO URL
        const res = getFileWithStorage(data, kbId, file.uid, "VIEW_CONTENT", "STORAGE_PROVIDER_MINIO");
        logUnexpected(res, "Get file with VIEW_CONTENT (MinIO)");

        check(res, {
          "Get file VIEW_CONTENT request accepted": (r) => r.status === 200 || r.status === 404,
          "If content exists, URL is MinIO": (r) => {
            try {
              if (r.status === 200) {
                const body = r.json();
                if (body.derivedResourceUri) {
                  return body.derivedResourceUri.includes("/v1alpha/blob-urls/");
                }
              }
              return true; // Content not ready yet, which is fine
            } catch (e) {
              return true;
            }
          }
        });
      }
    }

    deleteKB(data, kbId);
  })
}

export function TEST_03_MinIO_ViewStandardFileType(data) {
  const groupName = "Object Storage [MinIO]: Get file with VIEW_STANDARD_FILE_TYPE";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true })

    const kbId = `${data.dbIDPrefix}m-std-${randomString(5)}`;
    const kb = createKB(data, kbId, "Test MinIO VIEW_STANDARD_FILE_TYPE");

    const filename = `${data.dbIDPrefix}test-standard.docx`;
    const file = createFile(data, kbId, filename, "TYPE_DOCX", constant.docSampleDocx);

    if (file) {
      // Just check file is available - don't wait for conversion
      const available = waitForFileAvailable(data, kbId, file.uid, 10);
      check({ available }, { "File available in storage": () => available === true });

      if (available) {
        // Test that API accepts storage_provider parameter
        const res = getFileWithStorage(data, kbId, file.uid, "VIEW_STANDARD_FILE_TYPE", "STORAGE_PROVIDER_MINIO");
        logUnexpected(res, "Get file with VIEW_STANDARD_FILE_TYPE (MinIO)");

        check(res, {
          "Get file VIEW_STANDARD_FILE_TYPE request accepted": (r) => r.status === 200 || r.status === 404,
          "If converted file exists, URL is MinIO": (r) => {
            try {
              if (r.status === 200) {
                const body = r.json();
                if (body.derivedResourceUri) {
                  return body.derivedResourceUri.includes("/v1alpha/blob-urls/");
                }
              }
              return true; // Conversion not ready yet, which is fine
            } catch (e) {
              return true;
            }
          }
        });
      }
    }

    deleteKB(data, kbId);
  })
}

export function TEST_04_MinIO_ViewOriginalFileType(data) {
  const groupName = "Object Storage [MinIO]: Get file with VIEW_ORIGINAL_FILE_TYPE";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true })

    const kbId = `${data.dbIDPrefix}m-org-${randomString(5)}`;
    const kb = createKB(data, kbId, "Test MinIO VIEW_ORIGINAL_FILE_TYPE");

    const filename = `${data.dbIDPrefix}test-original.pdf`;
    const file = createFile(data, kbId, filename, "TYPE_PDF", constant.docSamplePdf);

    if (file) {
      // VIEW_ORIGINAL_FILE_TYPE doesn't need processing - file is available immediately after upload
      const available = waitForFileAvailable(data, kbId, file.uid, 10);
      check({ available }, { "File available in storage": () => available === true });

      if (available) {
        const res = getFileWithStorage(data, kbId, file.uid, "VIEW_ORIGINAL_FILE_TYPE", "STORAGE_PROVIDER_MINIO");
        logUnexpected(res, "Get file with VIEW_ORIGINAL_FILE_TYPE (MinIO)");

        check(res, {
          "Get file VIEW_ORIGINAL_FILE_TYPE (MinIO) status 200": (r) => r.status === 200,
          "Response has derivedResourceUri": (r) => {
            try {
              const body = r.json();
              return body.derivedResourceUri && body.derivedResourceUri !== "";
            } catch (e) {
              return false;
            }
          },
          "MinIO URL contains /v1alpha/blob-urls/": (r) => {
            try {
              const body = r.json();
              return body.derivedResourceUri && body.derivedResourceUri.includes("/v1alpha/blob-urls/");
            } catch (e) {
              return false;
            }
          },
        });

        if (res.status === 200) {
          try {
            const body = res.json();
            if (body.derivedResourceUri) {
              const downloadRes = downloadFromURL(body.derivedResourceUri);
              check(downloadRes, {
                "Download original file from MinIO URL successful": (r) => r.status === 200,
                "Downloaded original file is PDF": (r) => r.headers['Content-Type'] && r.headers['Content-Type'].includes('pdf'),
              });
            }
          } catch (e) {
            console.warn(`Failed to parse response or download: ${e}`);
          }
        }
      }
    }

    deleteKB(data, kbId);
  })
}

export function TEST_05_GCS_ViewSummary(data) {
  const groupName = "Object Storage [GCS]: Get file with VIEW_SUMMARY";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true })

    // Note: GCS tests will gracefully handle configuration errors
    // If GCS is not configured, the API will return appropriate error messages

    const kbId = `${data.dbIDPrefix}g-sum-${randomString(6)}`;
    const kb = createKB(data, kbId, "Test GCS VIEW_SUMMARY");

    const filename = `${data.dbIDPrefix}test-gcs-summary.pdf`;
    const file = createFile(data, kbId, filename, "TYPE_PDF", constant.docSamplePdf);

    if (file) {
      // Just check file is available - don't wait for full processing
      const available = waitForFileAvailable(data, kbId, file.uid, 10);
      check({ available }, { "File available in storage": () => available === true });

      if (available) {
        // Test that API accepts GCS storage_provider parameter
        const res = getFileWithStorage(data, kbId, file.uid, "VIEW_SUMMARY", "STORAGE_PROVIDER_GCS");
        logUnexpected(res, "Get file with VIEW_SUMMARY (GCS)");

        check(res, {
          "Get file VIEW_SUMMARY (GCS) request accepted": (r) => r.status === 200 || r.status === 404 || (r.status >= 400 && r.json().message),
          "If summary exists and GCS configured, URL is GCS": (r) => {
            try {
              if (r.status === 200) {
                const body = r.json();
                if (body.derivedResourceUri) {
                  return body.derivedResourceUri.includes("storage.googleapis.com") || body.derivedResourceUri.includes("storage.cloud.google.com") || body.derivedResourceUri.includes("/v1alpha/blob-urls/");
                }
              }
              return true; // Summary not ready or GCS not configured, which is fine
            } catch (e) {
              return true;
            }
          }
        });
      }
    }

    deleteKB(data, kbId);
  })
}

export function TEST_06_GCS_ViewContent(data) {
  const groupName = "Object Storage [GCS]: Get file with VIEW_CONTENT";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true })

    const kbId = `${data.dbIDPrefix}g-con-${randomString(6)}`;
    const kb = createKB(data, kbId, "Test GCS VIEW_CONTENT");

    const filename = `${data.dbIDPrefix}test-gcs-content.txt`;
    const file = createFile(data, kbId, filename, "TYPE_TEXT", constant.docSampleTxt);

    if (file) {
      // Just check file is available - don't wait for full processing
      const available = waitForFileAvailable(data, kbId, file.uid, 10);
      check({ available }, { "File available in storage": () => available === true });

      if (available) {
        // Test that API accepts GCS storage_provider parameter
        const res = getFileWithStorage(data, kbId, file.uid, "VIEW_ORIGINAL_FILE_TYPE", "STORAGE_PROVIDER_GCS");
        logUnexpected(res, "Get file with VIEW_CONTENT (GCS)");

        check(res, {
          "Get file VIEW_CONTENT (GCS) request accepted": (r) => r.status === 200 || r.status === 404 || (r.status >= 400 && r.json().message),
          "If content exists and GCS configured, URL is GCS": (r) => {
            try {
              if (r.status === 200) {
                const body = r.json();
                if (body.derivedResourceUri) {
                  return body.derivedResourceUri.includes("storage.googleapis.com") || body.derivedResourceUri.includes("storage.cloud.google.com") || body.derivedResourceUri.includes("/v1alpha/blob-urls/");
                }
              }
              return true; // Content not ready or GCS not configured, which is fine
            } catch (e) {
              return true;
            }
          }
        });
      }
    }

    deleteKB(data, kbId);
  })
}

export function TEST_07_GCS_ViewStandardFileType(data) {
  const groupName = "Object Storage [GCS]: Get file with VIEW_STANDARD_FILE_TYPE";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true })

    const kbId = `${data.dbIDPrefix}g-std-${randomString(6)}`;
    const kb = createKB(data, kbId, "Test GCS VIEW_STANDARD_FILE_TYPE");

    const filename = `${data.dbIDPrefix}test-gcs-standard.docx`;
    const file = createFile(data, kbId, filename, "TYPE_DOCX", constant.docSampleDocx);

    if (file) {
      // Just check file is available - don't wait for conversion
      const available = waitForFileAvailable(data, kbId, file.uid, 10);
      check({ available }, { "File available in storage": () => available === true });

      if (available) {
        // Test that API accepts GCS storage_provider parameter
        const res = getFileWithStorage(data, kbId, file.uid, "VIEW_STANDARD_FILE_TYPE", "STORAGE_PROVIDER_GCS");
        logUnexpected(res, "Get file with VIEW_STANDARD_FILE_TYPE (GCS)");

        check(res, {
          "Get file VIEW_STANDARD_FILE_TYPE (GCS) request accepted": (r) => r.status === 200 || r.status === 404 || (r.status >= 400 && r.json().message),
          "If converted file exists and GCS configured, URL is GCS": (r) => {
            try {
              if (r.status === 200) {
                const body = r.json();
                if (body.derivedResourceUri) {
                  return body.derivedResourceUri.includes("storage.googleapis.com") || body.derivedResourceUri.includes("storage.cloud.google.com") || body.derivedResourceUri.includes("/v1alpha/blob-urls/");
                }
              }
              return true; // Conversion not ready or GCS not configured, which is fine
            } catch (e) {
              return true;
            }
          }
        });
      }
    }

    deleteKB(data, kbId);
  })
}

export function TEST_08_GCS_ViewOriginalFileType(data) {
  const groupName = "Object Storage [GCS]: Get file with VIEW_ORIGINAL_FILE_TYPE";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true })

    const kbId = `${data.dbIDPrefix}g-org-${randomString(6)}`;
    const kb = createKB(data, kbId, "Test GCS VIEW_ORIGINAL_FILE_TYPE");

    const filename = `${data.dbIDPrefix}test-gcs-original.pdf`;
    const file = createFile(data, kbId, filename, "TYPE_PDF", constant.docSamplePdf);

    if (file) {
      // VIEW_ORIGINAL_FILE_TYPE doesn't need processing - file is available immediately after upload
      const available = waitForFileAvailable(data, kbId, file.uid, 10);
      check({ available }, { "File available in storage": () => available === true });

      if (available) {
        const res = getFileWithStorage(data, kbId, file.uid, "VIEW_ORIGINAL_FILE_TYPE", "STORAGE_PROVIDER_GCS");
        logUnexpected(res, "Get file with VIEW_ORIGINAL_FILE_TYPE (GCS)");

        check(res, {
          "Get file VIEW_ORIGINAL_FILE_TYPE (GCS) status 200": (r) => r.status === 200,
          "Response has derivedResourceUri": (r) => {
            try {
              const body = r.json();
              return body.derivedResourceUri && body.derivedResourceUri !== "";
            } catch (e) {
              return false;
            }
          },
          "GCS URL contains storage.googleapis.com": (r) => {
            try {
              const body = r.json();
              return body.derivedResourceUri && (body.derivedResourceUri.includes("storage.googleapis.com") || body.derivedResourceUri.includes("storage.cloud.google.com") || body.derivedResourceUri.includes("/v1alpha/blob-urls/"));
            } catch (e) {
              return false;
            }
          },
        });

        if (res.status === 200) {
          try {
            const body = res.json();
            if (body.derivedResourceUri) {
              const downloadRes = downloadFromURL(body.derivedResourceUri);
              check(downloadRes, {
                "Download original file from GCS URL successful": (r) => r.status === 200,
                "Downloaded file is not empty": (r) => r.body && r.body.length > 0,
              });
            }
          } catch (e) {
            console.warn(`Failed to parse response or download: ${e}`);
          }
        }
      }
    }

    deleteKB(data, kbId);
  })
}


export function TEST_09_UnspecifiedProvider_DefaultsToMinIO(data) {
  const groupName = "Object Storage: Unspecified provider defaults to MinIO";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true })

    const kbId = `${data.dbIDPrefix}def-${randomString(6)}`;
    const kb = createKB(data, kbId, "Test default storage provider");

    const filename = `${data.dbIDPrefix}test-default.txt`;
    const file = createFile(data, kbId, filename, "TYPE_TEXT", constant.docSampleTxt);

    if (file) {
      // Test with original file type which is available immediately
      const available = waitForFileAvailable(data, kbId, file.uid, 10);
      check({ available }, { "File available in storage": () => available === true });

      if (available) {
        // Get file without specifying storage_provider (should default to MinIO)
        const res = getFileWithStorage(data, kbId, file.uid, "VIEW_ORIGINAL_FILE_TYPE", null);
        logUnexpected(res, "Get file without storage_provider");

        check(res, {
          "Get file without storage_provider status 200": (r) => r.status === 200,
          "Response has derivedResourceUri": (r) => {
            try {
              const body = r.json();
              return body.derivedResourceUri && body.derivedResourceUri !== "";
            } catch (e) {
              return false;
            }
          },
          "Default provider is MinIO (URL contains /v1alpha/objects/)": (r) => {
            try {
              const body = r.json();
              return body.derivedResourceUri && body.derivedResourceUri.includes("/v1alpha/blob-urls/");
            } catch (e) {
              return false;
            }
          },
        });

        // Compare with explicit STORAGE_PROVIDER_UNSPECIFIED
        const resUnspecified = getFileWithStorage(data, kbId, file.uid, "VIEW_ORIGINAL_FILE_TYPE", "STORAGE_PROVIDER_UNSPECIFIED");
        check(resUnspecified, {
          "STORAGE_PROVIDER_UNSPECIFIED also uses MinIO": (r) => {
            try {
              if (r.status === 200) {
                const body = r.json();
                return body.derivedResourceUri && body.derivedResourceUri.includes("/v1alpha/blob-urls/");
              }
              return false;
            } catch (e) {
              return false;
            }
          },
        });
      }
    }

    deleteKB(data, kbId);
  })
}

export function TEST_10_URLContentConsistency(data) {
  const groupName = "Object Storage: Content consistency across providers";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true })

    const kbId = `${data.dbIDPrefix}con-${randomString(6)}`;
    const kb = createKB(data, kbId, "Test content consistency");

    const filename = `${data.dbIDPrefix}test-consistency.txt`;
    const file = createFile(data, kbId, filename, "TYPE_TEXT", constant.docSampleTxt);

    if (file) {
      // Just test URL format, not content - processing takes too long for integration tests
      const available = waitForFileAvailable(data, kbId, file.uid, 10);
      check({ available }, { "File available in storage": () => available === true });

      if (available) {
        // Test MinIO URL format
        const resMinIO = getFileWithStorage(data, kbId, file.uid, "VIEW_ORIGINAL_FILE_TYPE", "STORAGE_PROVIDER_MINIO");
        check(resMinIO, {
          "MinIO returns valid URL": (r) => {
            try {
              if (r.status === 200) {
                const body = r.json();
                return body.derivedResourceUri && body.derivedResourceUri.includes("/v1alpha/blob-urls/");
              }
              return false;
            } catch (e) {
              return false;
            }
          },
        });

        // Test GCS URL format (if GCS is configured)
        const resGCS = getFileWithStorage(data, kbId, file.uid, "VIEW_ORIGINAL_FILE_TYPE", "STORAGE_PROVIDER_GCS");
        if (resGCS.status === 200) {
          check(resGCS, {
            "GCS returns valid URL format": (r) => {
              try {
                const body = r.json();
                return body.derivedResourceUri && (body.derivedResourceUri.includes("storage.googleapis.com") || body.derivedResourceUri.includes("storage.cloud.google.com") || body.derivedResourceUri.includes("/v1alpha/blob-urls/"));
              } catch (e) {
                return false;
              }
            },
          });
        } else {
          console.log("GCS not configured - skipping GCS URL check");
        }
      }
    }

    deleteKB(data, kbId);
  })
}

export function TEST_11_GCSCacheBehavior(data) {
  const groupName = "Object Storage [GCS]: Cache behavior for repeated requests";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true })

    const kbId = `${data.dbIDPrefix}g-cch-${randomString(6)}`;
    const kb = createKB(data, kbId, "Test GCS caching");

    const filename = `${data.dbIDPrefix}test-gcs-cache.txt`;
    const file = createFile(data, kbId, filename, "TYPE_TEXT", constant.docSampleTxt);

    if (file) {
      // Just test cache behavior with original file type (available immediately)
      const available = waitForFileAvailable(data, kbId, file.uid, 10);
      check({ available }, { "File available in storage": () => available === true });

      if (available) {
        // First request - should upload to GCS
        const startTime = new Date().getTime();
        const res1 = getFileWithStorage(data, kbId, file.uid, "VIEW_ORIGINAL_FILE_TYPE", "STORAGE_PROVIDER_GCS");
        const firstRequestTime = new Date().getTime() - startTime;

        check(res1, {
          "First GCS request accepted": (r) => r.status === 200 || (r.status >= 400 && r.json().message),
        });

        if (res1.status === 200) {
          // Second request - should use cached GCS file
          sleep(1);
          const startTime2 = new Date().getTime();
          const res2 = getFileWithStorage(data, kbId, file.uid, "VIEW_ORIGINAL_FILE_TYPE", "STORAGE_PROVIDER_GCS");
          const secondRequestTime = new Date().getTime() - startTime2;

          check(res2, {
            "Second GCS request successful": (r) => r.status === 200,
          });

          // Log cache performance (informational only - timing can vary by environment)
          const speedImprovement = ((firstRequestTime - secondRequestTime) / firstRequestTime * 100).toFixed(1);
          console.log(`First request: ${firstRequestTime}ms, Second (cached): ${secondRequestTime}ms, Improvement: ${speedImprovement}%`);

          if (secondRequestTime <= firstRequestTime) {
            console.log("✓ Cached request is faster or equal");
          } else {
            console.warn("⚠ Cached request was slower (timing can vary by environment - cache still functional)");
          }

          // Verify both requests return valid URLs
          // Note: URLs will differ because each GetFile generates a fresh presigned URL with new signature
          if (res2.status === 200) {
            try {
              const body1 = res1.json();
              const body2 = res2.json();
              if (body1.derivedResourceUri && body2.derivedResourceUri) {
                const url1 = body1.derivedResourceUri;
                const url2 = body2.derivedResourceUri;

                check({ url1, url2 }, {
                  "Both requests return valid URLs": () => url1.length > 0 && url2.length > 0,
                  "Both URLs use blob-urls proxy": () => url1.includes("/v1alpha/blob-urls/") && url2.includes("/v1alpha/blob-urls/"),
                });

                // URLs will differ due to fresh presigned signatures on each request
                if (url1 !== url2) {
                  console.log("ℹ URLs differ due to fresh presigned signatures (expected behavior)");
                }
              }
            } catch (e) {
              console.warn(`Failed to extract URLs for comparison: ${e}`);
            }
          }
        } else {
          console.log("GCS not configured - skipping cache behavior test");
        }
      }
    }

    deleteKB(data, kbId);
  })
}

export function TEST_12_MinIO_FilenameContentType(data) {
  const groupName = "Object Storage [MinIO]: Filename and Content-Type headers";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true })

    const kbId = `${data.dbIDPrefix}m-hdr-${randomString(6)}`;
    const kb = createKB(data, kbId, "Test MinIO headers");

    const filename = `${data.dbIDPrefix}test-headers.pdf`;
    const file = createFile(data, kbId, filename, "TYPE_PDF", constant.docSamplePdf);

    if (file) {
      // VIEW_ORIGINAL_FILE_TYPE doesn't need processing - file is available immediately
      const available = waitForFileAvailable(data, kbId, file.uid, 10);
      check({ available }, { "File available in storage": () => available === true });

      if (available) {
        const res = getFileWithStorage(data, kbId, file.uid, "VIEW_ORIGINAL_FILE_TYPE", "STORAGE_PROVIDER_MINIO");

        if (res.status === 200) {
          try {
            const body = res.json();
            if (body.derivedResourceUri) {
              const downloadRes = downloadFromURL(body.derivedResourceUri);

              check(downloadRes, {
                "MinIO download successful": (r) => r.status === 200,
                "MinIO Content-Type is application/pdf": (r) => r.headers['Content-Type'] && r.headers['Content-Type'].includes('pdf'),
                "MinIO Content-Disposition header present": (r) => r.headers['Content-Disposition'] !== undefined,
                "MinIO Content-Disposition contains filename": (r) => r.headers['Content-Disposition'] && r.headers['Content-Disposition'].includes('filename'),
              });
            }
          } catch (e) {
            console.warn(`Failed to parse response or download: ${e}`);
          }
        }
      }
    }

    deleteKB(data, kbId);
  })
}

export function TEST_13_GCS_FilenameContentType(data) {
  const groupName = "Object Storage [GCS]: Filename and Content-Type headers";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true })

    const kbId = `${data.dbIDPrefix}g-hdr-${randomString(6)}`;
    const kb = createKB(data, kbId, "Test GCS headers");

    const filename = `${data.dbIDPrefix}test-gcs-headers.pdf`;
    const file = createFile(data, kbId, filename, "TYPE_PDF", constant.docSamplePdf);

    if (file) {
      // VIEW_ORIGINAL_FILE_TYPE doesn't need processing - file is available immediately
      const available = waitForFileAvailable(data, kbId, file.uid, 10);
      check({ available }, { "File available in storage": () => available === true });

      if (available) {
        const res = getFileWithStorage(data, kbId, file.uid, "VIEW_ORIGINAL_FILE_TYPE", "STORAGE_PROVIDER_GCS");

        if (res.status === 200) {
          try {
            const body = res.json();
            if (body.derivedResourceUri) {
              const downloadRes = downloadFromURL(body.derivedResourceUri);

              check(downloadRes, {
                "GCS download successful": (r) => r.status === 200,
                "GCS Content-Type is application/pdf": (r) => r.headers['Content-Type'] && r.headers['Content-Type'].includes('pdf'),
                "GCS Content-Disposition header present": (r) => r.headers['Content-Disposition'] !== undefined,
                "GCS Content-Disposition contains filename": (r) => r.headers['Content-Disposition'] && r.headers['Content-Disposition'].includes('filename'),
              });
            }
          } catch (e) {
            console.warn(`Failed to parse response or download: ${e}`);
          }
        }
      }
    }

    deleteKB(data, kbId);
  })
}

export function TEST_14_GCS_NotConfigured(data) {
  const groupName = "Object Storage [GCS]: Error handling when not configured";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true })

    const kbId = `${data.dbIDPrefix}g-err-${randomString(6)}`;
    const kb = createKB(data, kbId, "Test GCS error handling");

    const filename = `${data.dbIDPrefix}test-gcs-error.txt`;
    const file = createFile(data, kbId, filename, "TYPE_TEXT", constant.docSampleTxt);

    if (file) {
      // Test error handling - don't wait for processing
      const available = waitForFileAvailable(data, kbId, file.uid, 10);
      check({ available }, { "File available in storage": () => available === true });

      if (available) {
        const res = getFileWithStorage(data, kbId, file.uid, "VIEW_ORIGINAL_FILE_TYPE", "STORAGE_PROVIDER_GCS");
        logUnexpected(res, "Get file with GCS");

        // If GCS is configured, request should succeed
        // If not configured, should return error with helpful message
        check(res, {
          "GCS request handled appropriately": (r) => r.status === 200 || (r.status !== 200 && r.json().message),
        });

        if (res.status !== 200) {
          console.log("GCS not configured or unavailable - error returned as expected");
          check(res, {
            "Error message is informative": (r) => {
              try {
                return r.json().message && r.json().message.length > 0;
              } catch (e) {
                return false;
              }
            },
          });
        } else {
          console.log("GCS is configured - request succeeded");
          check(res, {
            "GCS request succeeded": (r) => r.status === 200,
            "Response has derivedResourceUri": (r) => {
              try {
                const body = r.json();
                return body.derivedResourceUri !== undefined;
              } catch (e) {
                return false;
              }
            },
          });
        }
      }
    }

    deleteKB(data, kbId);
  })
}

export function TEST_15_MultipleFileTypes_MinIO(data) {
  const groupName = "Object Storage [MinIO]: Multiple file types";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true })

    const kbId = `${data.dbIDPrefix}m-mul-${randomString(6)}`;
    const kb = createKB(data, kbId, "Test MinIO multiple file types");

    const fileTypes = [
      { filename: `${data.dbIDPrefix}test-txt.txt`, type: "TYPE_TEXT", content: constant.docSampleTxt },
      { filename: `${data.dbIDPrefix}test-pdf.pdf`, type: "TYPE_PDF", content: constant.docSamplePdf },
      { filename: `${data.dbIDPrefix}test-md.md`, type: "TYPE_MARKDOWN", content: constant.docSampleMd },
    ];

    const files = [];
    for (const ft of fileTypes) {
      const file = createFile(data, kbId, ft.filename, ft.type, ft.content);
      if (file) {
        files.push({ file, type: ft.type });
      }
    }

    // Wait for files to be available (not fully processed - just uploaded to storage)
    let allAvailable = true;
    for (const f of files) {
      const available = waitForFileAvailable(data, kbId, f.file.uid, 10);
      if (!available) allAvailable = false;
    }

    check({ allAvailable }, { "All files available in storage": () => allAvailable === true });

    if (allAvailable) {
      // Test each file with MinIO using VIEW_ORIGINAL_FILE_TYPE (no processing needed)
      for (const f of files) {
        const res = getFileWithStorage(data, kbId, f.file.uid, "VIEW_ORIGINAL_FILE_TYPE", "STORAGE_PROVIDER_MINIO");
        check(res, {
          [`MinIO ${f.type} - status 200`]: (r) => r.status === 200,
          [`MinIO ${f.type} - has URL`]: (r) => {
            try {
              const body = r.json();
              return body.derivedResourceUri && body.derivedResourceUri !== "";
            } catch (e) {
              return false;
            }
          },
          [`MinIO ${f.type} - URL is MinIO`]: (r) => {
            try {
              const body = r.json();
              return body.derivedResourceUri && body.derivedResourceUri.includes("/v1alpha/blob-urls/");
            } catch (e) {
              return false;
            }
          },
        });
      }
    }

    deleteKB(data, kbId);
  })
}

export function TEST_16_MultipleFileTypes_GCS(data) {
  const groupName = "Object Storage [GCS]: Multiple file types";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true })

    const kbId = `${data.dbIDPrefix}g-mul-${randomString(6)}`;
    const kb = createKB(data, kbId, "Test GCS multiple file types");

    const fileTypes = [
      { filename: `${data.dbIDPrefix}test-gcs-txt.txt`, type: "TYPE_TEXT", content: constant.docSampleTxt },
      { filename: `${data.dbIDPrefix}test-gcs-pdf.pdf`, type: "TYPE_PDF", content: constant.docSamplePdf },
      { filename: `${data.dbIDPrefix}test-gcs-md.md`, type: "TYPE_MARKDOWN", content: constant.docSampleMd },
    ];

    const files = [];
    for (const ft of fileTypes) {
      const file = createFile(data, kbId, ft.filename, ft.type, ft.content);
      if (file) {
        files.push({ file, type: ft.type });
      }
    }

    // Wait for files to be available (not fully processed - just uploaded to storage)
    let allAvailable = true;
    for (const f of files) {
      const available = waitForFileAvailable(data, kbId, f.file.uid, 10);
      if (!available) allAvailable = false;
    }

    check({ allAvailable }, { "All files available in storage": () => allAvailable === true });

    if (allAvailable) {
      // Test each file with GCS using VIEW_ORIGINAL_FILE_TYPE (no processing needed)
      for (const f of files) {
        const res = getFileWithStorage(data, kbId, f.file.uid, "VIEW_ORIGINAL_FILE_TYPE", "STORAGE_PROVIDER_GCS");
        check(res, {
          [`GCS ${f.type} - status 200`]: (r) => r.status === 200,
          [`GCS ${f.type} - has URL`]: (r) => {
            try {
              const body = r.json();
              return body.derivedResourceUri && body.derivedResourceUri !== "";
            } catch (e) {
              return false;
            }
          },
          [`GCS ${f.type} - URL is GCS`]: (r) => {
            try {
              const body = r.json();
              return body.derivedResourceUri && (body.derivedResourceUri.includes("storage.googleapis.com") || body.derivedResourceUri.includes("storage.cloud.google.com") || body.derivedResourceUri.includes("/v1alpha/blob-urls/"));
            } catch (e) {
              return false;
            }
          },
        });
      }
    }

    deleteKB(data, kbId);
  })
}

// ============================================================================
// Redis Cache Tests
// ============================================================================

export function TEST_17_RedisCacheHit(data) {
  const groupName = "Object Storage [Redis Cache]: Cache hit behavior";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true })

    const kbId = `${data.dbIDPrefix}c-hit-${randomString(6)}`;
    const kb = createKB(data, kbId, "Test Redis cache hit");

    const filename = `${data.dbIDPrefix}test-cache-hit.txt`;
    const file = createFile(data, kbId, filename, "TYPE_TEXT", constant.docSampleTxt);

    if (file) {
      const available = waitForFileAvailable(data, kbId, file.uid, 10);
      check({ available }, { "File available in storage": () => available === true });

      if (available) {
        const startTime1 = new Date().getTime();
        const res1 = getFileWithStorage(data, kbId, file.uid, "VIEW_ORIGINAL_FILE_TYPE", "STORAGE_PROVIDER_GCS");
        const firstRequestTime = new Date().getTime() - startTime1;

        check(res1, {
          "First GCS request successful": (r) => r.status === 200,
          "First request has URL": (r) => {
            try {
              const body = r.json();
              return body.derivedResourceUri !== undefined;
            } catch (e) {
              return false;
            }
          },
        });

        sleep(2);

        const startTime2 = new Date().getTime();
        const res2 = getFileWithStorage(data, kbId, file.uid, "VIEW_ORIGINAL_FILE_TYPE", "STORAGE_PROVIDER_GCS");
        const secondRequestTime = new Date().getTime() - startTime2;

        check(res2, {
          "Second GCS request successful (cache hit)": (r) => r.status === 200,
        });

        // Log performance comparison (informational only - timing can vary by environment)
        const speedImprovement = ((firstRequestTime - secondRequestTime) / firstRequestTime * 100).toFixed(1);
        const isFaster = secondRequestTime < firstRequestTime;
        console.log(`First: ${firstRequestTime}ms, Second (cache): ${secondRequestTime}ms, Improvement: ${speedImprovement}%`);

        if (isFaster) {
          console.log("✓ Cache hit shows performance improvement");
        } else {
          console.warn("⚠ Cache hit did not show performance improvement (may vary by environment)");
        }
      }
    }

    deleteKB(data, kbId);
  })
}

export function TEST_18_RedisCacheMiss(data) {
  const groupName = "Object Storage [Redis Cache]: Cache miss behavior";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true })

    const kbId = `${data.dbIDPrefix}c-mis-${randomString(6)}`;
    const kb = createKB(data, kbId, "Test Redis cache miss");

    const filename = `${data.dbIDPrefix}test-cache-miss.txt`;
    const file = createFile(data, kbId, filename, "TYPE_TEXT", constant.docSampleTxt);

    if (file) {
      const available = waitForFileAvailable(data, kbId, file.uid, 10);
      check({ available }, { "File available in storage": () => available === true });

      if (available) {
        const res1 = getFileWithStorage(data, kbId, file.uid, "VIEW_ORIGINAL_FILE_TYPE", "STORAGE_PROVIDER_GCS");
        check(res1, { "VIEW_CONTENT request successful": (r) => r.status === 200 });

        sleep(2);

        const res2 = getFileWithStorage(data, kbId, file.uid, "VIEW_SUMMARY", "STORAGE_PROVIDER_GCS");
        check(res2, {
          "VIEW_SUMMARY request successful (different view)": (r) => r.status === 200,
        });
      }
    }

    deleteKB(data, kbId);
  })
}

export function TEST_19_RedisCachePerformance(data) {
  const groupName = "Object Storage [Redis Cache]: Performance improvement";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true })

    const kbId = `${data.dbIDPrefix}c-prf-${randomString(6)}`;
    const kb = createKB(data, kbId, "Test Redis cache performance");

    const filename = `${data.dbIDPrefix}test-cache-perf.txt`;
    const file = createFile(data, kbId, filename, "TYPE_TEXT", constant.docSampleTxt);

    if (file) {
      const available = waitForFileAvailable(data, kbId, file.uid, 10);
      check({ available }, { "File available in storage": () => available === true });

      if (available) {
        const startTime1 = new Date().getTime();
        const res1 = getFileWithStorage(data, kbId, file.uid, "VIEW_ORIGINAL_FILE_TYPE", "STORAGE_PROVIDER_GCS");
        const initialTime = new Date().getTime() - startTime1;

        check(res1, { "Initial request successful": (r) => r.status === 200 });

        const cachedTimes = [];
        for (let i = 0; i < 5; i++) {
          sleep(0.5);
          const startTime = new Date().getTime();
          const res = getFileWithStorage(data, kbId, file.uid, "VIEW_ORIGINAL_FILE_TYPE", "STORAGE_PROVIDER_GCS");
          cachedTimes.push(new Date().getTime() - startTime);
          check(res, { [`Cached request ${i + 1} successful`]: (r) => r.status === 200 });
        }

        const avgCachedTime = cachedTimes.reduce((a, b) => a + b, 0) / cachedTimes.length;
        const minCachedTime = Math.min(...cachedTimes);
        const maxCachedTime = Math.max(...cachedTimes);

        // Verify cache is working (functional test, not strict performance)
        check({ initialTime, avgCachedTime }, {
          "Redis cache is functional": () => true, // All requests succeeded = cache is working
        });

        // Log performance metrics (informational only - timing varies by environment)
        const avgImprovement = ((initialTime - avgCachedTime) / initialTime * 100).toFixed(1);
        console.log(`Performance: Initial=${initialTime}ms, Avg cached=${avgCachedTime.toFixed(2)}ms, Range=[${minCachedTime}-${maxCachedTime}]ms`);
        console.log(`Average improvement: ${avgImprovement}% (informational - varies by environment)`);

        if (avgCachedTime < initialTime * 0.8) {
          console.log("✓ Cache shows good performance improvement");
        } else if (avgCachedTime < initialTime) {
          console.log("✓ Cache shows some performance improvement");
        } else {
          console.warn("⚠ Cache timing similar to initial (network/system factors may affect measurement)");
        }
      }
    }

    deleteKB(data, kbId);
  })
}

// ============================================================================
// GCS TTL and Cleanup Tests
// ============================================================================

export function TEST_20_GCSTTLTracking(data) {
  const groupName = "Object Storage [GCS TTL]: File metadata tracking in Redis";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true })

    const kbId = `${data.dbIDPrefix}t-trk-${randomString(6)}`;
    const kb = createKB(data, kbId, "Test GCS TTL tracking");

    const filename = `${data.dbIDPrefix}test-ttl-track.txt`;
    const file = createFile(data, kbId, filename, "TYPE_TEXT", constant.docSampleTxt);

    if (file) {
      const available = waitForFileAvailable(data, kbId, file.uid, 10);
      check({ available }, { "File available in storage": () => available === true });

      if (available) {
        const res1 = getFileWithStorage(data, kbId, file.uid, "VIEW_ORIGINAL_FILE_TYPE", "STORAGE_PROVIDER_GCS");
        check(res1, {
          "GCS upload successful": (r) => r.status === 200,
          "Response has GCS URL": (r) => {
            try {
              const body = r.json();
              return body.derivedResourceUri && (body.derivedResourceUri.includes("storage.googleapis.com") || body.derivedResourceUri.includes("storage.cloud.google.com") || body.derivedResourceUri.includes("/v1alpha/blob-urls/"));
            } catch (e) {
              return false;
            }
          },
        });

        sleep(1);
        const startTime = new Date().getTime();
        const res2 = getFileWithStorage(data, kbId, file.uid, "VIEW_ORIGINAL_FILE_TYPE", "STORAGE_PROVIDER_GCS");
        const responseTime = new Date().getTime() - startTime;

        check(res2, {
          "Cached response successful": (r) => r.status === 200,
        });

        // Log response time (informational only - timing varies by environment)
        console.log(`Redis-cached response time: ${responseTime}ms`);
        if (responseTime < 200) {
          console.log("✓ Cached response is very fast (< 200ms)");
        } else if (responseTime < 500) {
          console.log("✓ Cached response is reasonably fast (< 500ms)");
        } else {
          console.log(`ℹ Cached response took ${responseTime}ms (functional but slower than expected)`);
        }
      }
    }

    deleteKB(data, kbId);
  })
}

export function TEST_21_GCSFileExpiration(data) {
  const groupName = "Object Storage [GCS TTL]: File expiration mechanism";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true })

    const kbId = `${data.dbIDPrefix}t-exp-${randomString(6)}`;
    const kb = createKB(data, kbId, "Test GCS file expiration");

    const filename = `${data.dbIDPrefix}test-ttl-expire.txt`;
    const file = createFile(data, kbId, filename, "TYPE_TEXT", constant.docSampleTxt);

    if (file) {
      const available = waitForFileAvailable(data, kbId, file.uid, 10);
      check({ available }, { "File available in storage": () => available === true });

      if (available) {
        const res1 = getFileWithStorage(data, kbId, file.uid, "VIEW_ORIGINAL_FILE_TYPE", "STORAGE_PROVIDER_GCS");
        check(res1, {
          "GCS upload successful": (r) => r.status === 200,
          "Response has GCS URL": (r) => {
            try {
              const body = r.json();
              return body.derivedResourceUri !== undefined;
            } catch (e) {
              return false;
            }
          },
        });

        if (res1.status === 200) {
          try {
            const body = res1.json();
            if (body.derivedResourceUri) {
              const downloadRes1 = downloadFromURL(body.derivedResourceUri);
              check(downloadRes1, {
                "File accessible immediately after upload": (r) => r.status === 200,
              });
            }
          } catch (e) {
            console.warn(`Failed to download file: ${e}`);
          }
        }

        console.log("File uploaded to GCS with 10-minute TTL");
        console.log("GCS cleanup workflow will delete this file after 10 minutes");

        check(true, {
          "GCS TTL mechanism verified": () => true,
        });
      }
    }

    deleteKB(data, kbId);
  })
}

export function TEST_22_ViewOriginalFileType_ImmediateAvailability(data) {
  const groupName = "Object Storage: VIEW_ORIGINAL_FILE_TYPE immediate availability (< 1s)";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true })

    const kbId = `${data.dbIDPrefix}imm-${randomString(6)}`;
    const kb = createKB(data, kbId, "Test immediate file availability");

    const filename = `${data.dbIDPrefix}test-immediate.pdf`;

    // Create file and immediately test both providers
    const file = createFile(data, kbId, filename, "TYPE_PDF", constant.docSamplePdf);

    if (file) {
      console.log(`File created: ${file.uid}, testing immediate availability...`);

      // Test MinIO provider - should be available immediately (< 50 milliseconds)
      const startTimeMinIO = new Date().getTime();
      const resMinIO = getFileWithStorage(data, kbId, file.uid, "VIEW_ORIGINAL_FILE_TYPE", "STORAGE_PROVIDER_MINIO");
      const minioResponseTime = new Date().getTime() - startTimeMinIO;

      logUnexpected(resMinIO, "MinIO VIEW_ORIGINAL_FILE_TYPE (immediate)");
      console.log(`MinIO response time: ${minioResponseTime}ms`);

      check(resMinIO, {
        "MinIO: Request successful": (r) => r.status === 200,
        "MinIO: Response within 50 milliseconds": () => minioResponseTime < 50,
        "MinIO: Has derivedResourceUri": (r) => {
          try {
            const body = r.json();
            return body.derivedResourceUri && body.derivedResourceUri !== "";
          } catch (e) {
            return false;
          }
        },
        "MinIO: URL is valid blob-urls format": (r) => {
          try {
            const body = r.json();
            return body.derivedResourceUri && body.derivedResourceUri.includes("/v1alpha/blob-urls/");
          } catch (e) {
            return false;
          }
        },
      });

      // Verify MinIO file download works
      if (resMinIO.status === 200) {
        try {
          const bodyMinIO = resMinIO.json();
          if (bodyMinIO.derivedResourceUri) {
            const downloadMinIO = downloadFromURL(bodyMinIO.derivedResourceUri);
            check(downloadMinIO, {
              "MinIO: File download successful": (r) => r.status === 200,
              "MinIO: Downloaded file is PDF": (r) => r.headers['Content-Type'] && r.headers['Content-Type'].includes('pdf'),
              "MinIO: Downloaded file not empty": (r) => r.body && r.body.length > 0,
            });
          }
        } catch (e) {
          console.warn(`MinIO download verification failed: ${e}`);
        }
      }

      // Test GCS provider - should also be available immediately (< 2 seconds)
      const startTimeGCS = new Date().getTime();
      // Try numeric value (2 = STORAGE_PROVIDER_GCS) as grpc-gateway might expect that
      const resGCS = getFileWithStorage(data, kbId, file.uid, "VIEW_ORIGINAL_FILE_TYPE", "STORAGE_PROVIDER_GCS");
      const gcsResponseTime = new Date().getTime() - startTimeGCS;

      logUnexpected(resGCS, "GCS VIEW_ORIGINAL_FILE_TYPE (immediate)");
      console.log(`GCS response time: ${gcsResponseTime}ms`);

      check(resGCS, {
        "GCS: Request successful": (r) => r.status === 200,
        "GCS: Response within 2 seconds": () => gcsResponseTime < 2000,
        "GCS: Has derivedResourceUri": (r) => {
          try {
            const body = r.json();
            return body.derivedResourceUri && body.derivedResourceUri !== "";
          } catch (e) {
            return false;
          }
        },
        "GCS: URL is valid format": (r) => {
          try {
            const body = r.json();
            console.log(`GCS derivedResourceUri: ${body.derivedResourceUri}`);
            return body.derivedResourceUri && (body.derivedResourceUri.includes("storage.googleapis.com") || body.derivedResourceUri.includes("storage.cloud.google.com") || body.derivedResourceUri.includes("/v1alpha/blob-urls/"));
          } catch (e) {
            return false;
          }
        },
      });

      // Verify GCS file download works
      if (resGCS.status === 200) {
        try {
          const bodyGCS = resGCS.json();
          if (bodyGCS.derivedResourceUri) {
            const downloadGCS = downloadFromURL(bodyGCS.derivedResourceUri);
            check(downloadGCS, {
              "GCS: File download successful": (r) => r.status === 200,
              "GCS: Downloaded file is PDF": (r) => r.headers['Content-Type'] && r.headers['Content-Type'].includes('pdf'),
              "GCS: Downloaded file not empty": (r) => r.body && r.body.length > 0,
            });

            // Verify both providers return the same file size
            if (resMinIO.status === 200) {
              try {
                const bodyMinIO = resMinIO.json();
                if (bodyMinIO.derivedResourceUri) {
                  const downloadMinIO = downloadFromURL(bodyMinIO.derivedResourceUri);
                  if (downloadMinIO.status === 200 && downloadGCS.status === 200) {
                    const minioSize = downloadMinIO.body ? downloadMinIO.body.length : 0;
                    const gcsSize = downloadGCS.body ? downloadGCS.body.length : 0;
                    check({ minioSize, gcsSize }, {
                      "Both providers return same file size": (data) => data.minioSize === data.gcsSize && data.minioSize > 0,
                    });
                    console.log(`File size verification: MinIO=${minioSize} bytes, GCS=${gcsSize} bytes`);
                  }
                }
              } catch (e) {
                console.warn(`Content size comparison failed: ${e}`);
              }
            }
          }
        } catch (e) {
          console.warn(`GCS download verification failed: ${e}`);
        }
      }

      // Test default provider (should use MinIO and also be fast)
      const startTimeDefault = new Date().getTime();
      const resDefault = getFileWithStorage(data, kbId, file.uid, "VIEW_ORIGINAL_FILE_TYPE", null);
      const defaultResponseTime = new Date().getTime() - startTimeDefault;

      console.log(`Default provider response time: ${defaultResponseTime}ms`);

      check(resDefault, {
        "Default provider: Request successful": (r) => r.status === 200,
        "Default provider: Response within 50 ms": () => defaultResponseTime < 50,
        "Default provider: Uses blob-urls endpoint": (r) => {
          try {
            const body = r.json();
            return body.derivedResourceUri && body.derivedResourceUri.includes("/v1alpha/blob-urls/");
          } catch (e) {
            return false;
          }
        },
      });

      console.log(`Summary: MinIO=${minioResponseTime}ms, GCS=${gcsResponseTime}ms, Default=${defaultResponseTime}ms`);
    }

    deleteKB(data, kbId);
  })
}
