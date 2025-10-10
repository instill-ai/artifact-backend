import http from "k6/http";
import { check, group, sleep } from "k6";
import { randomString } from "https://jslib.k6.io/k6-utils/1.1.0/index.js";

import { artifactPublicHost } from "./const.js";

import * as constant from "./const.js";
import * as helper from "./helper.js";

export function CheckCreateCatalog(data) {
  const groupName = "Artifact API: Create a catalog";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    var reqBody = {
      name: constant.dbIDPrefix + randomString(10),
      description: randomString(50),
      tags: ["test", "integration"],
      type: "CATALOG_TYPE_PERSISTENT"
    };

    // Create a catalog
    var resOrigin = http.request(
      "POST",
      `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`,
      JSON.stringify(reqBody),
      data.header
    );
    let json; try { json = resOrigin.json(); } catch (e) { json = {}; }
    const cat = json.catalog || {};
    const uid = cat.catalogUid;
    const id = cat.catalogId;
    const createTime = cat.createTime || cat.create_time;
    const updateTime = cat.updateTime || cat.update_time;
    check(resOrigin, {
      "POST /v1alpha/namespaces/{namespace_id}/catalogs response status is 200": (r) => r.status === 200,
      "POST /v1alpha/namespaces/{namespace_id}/catalogs response catalog name": () =>
        cat && cat.name === reqBody.name,
      "POST /v1alpha/namespaces/{namespace_id}/catalogs response catalog uid": () =>
        cat && helper.isUUID(uid),
      "POST /v1alpha/namespaces/{namespace_id}/catalogs response catalog id": () =>
        cat && id === reqBody.name,
      "POST /v1alpha/namespaces/{namespace_id}/catalogs response catalog description": () =>
        cat && cat.description === reqBody.description,
      "POST /v1alpha/namespaces/{namespace_id}/catalogs response catalog is valid": () =>
        cat && helper.validateCatalog(cat, false),
      "POST /v1alpha/namespaces/{namespace_id}/catalogs response catalog createTime": () =>
        cat && typeof createTime === 'string' && createTime.length > 0,
      "POST /v1alpha/namespaces/{namespace_id}/catalogs response catalog updateTime": () =>
        cat && typeof updateTime === 'string' && updateTime.length > 0,
    });

    const created = json.catalog;
    if (!created || !(created.catalogId)) {
      const listRes = http.request("GET", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`, null, data.header);
      try {
        const listJson = listRes.json();
        const found = (listJson.catalogs || []).find(c => c.name === reqBody.name || c.catalogId === reqBody.name);
        if (found) created = found;
      } catch (e) { }
    }
    if (created && created.catalogId) {
      http.request("DELETE", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${created.catalogId}`, null, data.header);
    }
  });
}

export function CheckListCatalogs(data) {
  const groupName = "Artifact API: List catalogs";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    var resOrigin = http.request(
      "GET",
      `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`,
      null,
      data.header
    );
    let json; try { json = resOrigin.json(); } catch (e) { json = {}; }
    check(resOrigin, {
      "GET /v1alpha/namespaces/{namespace_id}/catalogs response status is 200": (r) => r.status === 200,
      "GET /v1alpha/namespaces/{namespace_id}/catalogs response catalogs is array": () =>
        Array.isArray(json.catalogs),
    });
    return json.catalogs;
  });
}

export function CheckGetCatalog(data) {
  const groupName = "Artifact API: Get catalog";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    const cRes = http.request(
      "POST",
      `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`,
      JSON.stringify({ name: constant.dbIDPrefix + randomString(10) }),
      data.header
    );
    check(cRes, { "POST /v1alpha/namespaces/{namespace_id}/catalogs 200": (r) => r.status === 200 });
    const created = (cRes.json() || {}).catalog || {};

    const resOrigin = http.request(
      "GET",
      `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`,
      null,
      data.header
    );
    let json; try { json = resOrigin.json(); } catch (e) { json = {}; }
    const catalogs = Array.isArray(json.catalogs) ? json.catalogs : [];
    check(resOrigin, {
      "GET /v1alpha/namespaces/{namespace_id}/catalogs response status is 200": (r) => r.status === 200,
      "GET /v1alpha/namespaces/{namespace_id}/catalogs response catalogs is array": () => Array.isArray(json.catalogs),
      "GET /v1alpha/namespaces/{namespace_id}/catalogs response contains our catalog": () => catalogs.some(c => c.catalogId === created.catalogId),
    });

    http.request(
      "DELETE",
      `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${created.catalogId}`,
      null,
      data.header
    );
  });
}

export function CheckUpdateCatalog(data) {
  const groupName = "Artifact API: Update catalog";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    const cRes = http.request(
      "POST",
      `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`,
      JSON.stringify({ name: constant.dbIDPrefix + randomString(10) }),
      data.header
    );
    const created = (cRes.json() || {}).catalog || {};

    const reqBody = {
      catalogId: created.catalogId,
      description: randomString(50),
      tags: ["test", "integration", "updated"],
      namespaceId: data.expectedOwner.id,
    };

    const resOrigin = http.request(
      "PUT",
      `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${created.catalogId}`,
      JSON.stringify(reqBody),
      data.header
    );
    const json = (function () { try { return resOrigin.json(); } catch (e) { return {}; } })();
    const cat2 = json.catalog || {};
    check(resOrigin, {
      "PUT /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id} response status is 200": (r) => r.status === 200,
      "PUT /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id} response catalog id": () =>
        cat2.catalogId === created.catalogId,
      "PUT /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id} response catalog description updated": () =>
        cat2.description === reqBody.description,
      "PUT /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id} response catalog is valid": () =>
        helper.validateCatalog(cat2, false),
    });

    http.request(
      "DELETE",
      `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${created.catalogId}`,
      null,
      data.header
    );
  });
}

export function CheckDeleteCatalog(data) {
  const groupName = "Artifact API: Delete catalog";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    // Create a catalog to delete
    const createBody = { name: constant.dbIDPrefix + "del-" + randomString(8) };
    const cRes = http.request(
      "POST",
      `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`,
      JSON.stringify(createBody),
      data.header
    );
    let created; try { created = (cRes.json() || {}).catalog; } catch (e) { created = {}; }
    const catalogId = created && created.catalogId;
    check(cRes, { "POST /v1alpha/namespaces/{namespace_id}/catalogs 200": (r) => r.status === 200 });

    // Delete the created catalog
    const dRes = http.request(
      "DELETE",
      `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}`,
      null,
      data.header
    );
    check(dRes, {
      "DELETE /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id} response status is 2xx": (r) => r.status >= 200 && r.status < 300,
    });
  });
}

export function CheckCatalog(data) {
  const groupName = "Artifact API: Catalog end-to-end";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    // Create catalog
    const createBody = {
      name: constant.dbIDPrefix + "cat-" + randomString(8),
      description: randomString(40),
      tags: ["test", "integration", "catalog-e2e"],
      type: "CATALOG_TYPE_PERSISTENT",
    };
    const cRes = http.request(
      "POST",
      `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`,
      JSON.stringify(createBody),
      data.header
    );
    let created; try { created = (cRes.json() || {}).catalog; } catch (e) { created = {}; }
    const catalogId = created && created.catalogId;
    check(cRes, {
      "POST /v1alpha/namespaces/{namespace_id}/catalogs 200": (r) => r.status === 200,
      "POST /v1alpha/namespaces/{namespace_id}/catalogs id matches name": () => catalogId === createBody.name,
      "POST /v1alpha/namespaces/{namespace_id}/catalogs valid": () => created && helper.validateCatalog(created, false),
    });

    // List catalogs - ensure presence
    const listRes = http.request("GET", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs`, null, data.header);
    let listJson; try { listJson = listRes.json(); } catch (e) { listJson = {}; }
    const catalogs = Array.isArray(listJson.catalogs) ? listJson.catalogs : [];
    check(listRes, {
      "GET /v1alpha/namespaces/{namespace_id}/catalogs 200": (r) => r.status === 200,
      "GET /v1alpha/namespaces/{namespace_id}/catalogs contains created": () => catalogs.some((c) => c.catalogId === catalogId),
    });

    // Update catalog
    const updateBody = {
      catalogId: catalogId,
      namespaceId: data.expectedOwner.id,
      description: randomString(48),
      tags: ["test", "integration", "updated"],
    };
    const uRes = http.request(
      "PUT",
      `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}`,
      JSON.stringify(updateBody),
      data.header
    );
    let updated; try { updated = (uRes.json() || {}).catalog; } catch (e) { updated = {}; }
    check(uRes, {
      "PUT /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id} 200": (r) => r.status === 200,
      "PUT /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id} id stable": () => updated.catalogId === catalogId,
      "PUT /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id} description applied": () => updated && updated.description === updateBody.description,
    });

    // Upload all file types (parallel)
    const uploaded = [];
    const uploadReqs = constant.sampleFiles.map((s) => {
      const fileName = `${constant.dbIDPrefix}${s.originalName}`;
      // Add tags to file upload - using same tags for all files initially
      const tags = ["kim", "knives"];
      return {
        s,
        fileName,
        tags,
        req: {
          method: "POST",
          url: `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files`,
          body: JSON.stringify({ name: fileName, type: s.type, content: s.content, tags: tags }),
          params: data.header,
        },
      };
    });
    const uploadResponses = http.batch(uploadReqs.map((x) => x.req));
    for (let i = 0; i < uploadResponses.length; i++) {
      const resp = uploadResponses[i];
      const s = uploadReqs[i].s;
      const fileName = uploadReqs[i].fileName;
      const tags = uploadReqs[i].tags;
      const fJson = (function () { try { return resp.json(); } catch (e) { return {}; } })();
      const file = (fJson && fJson.file) || {};
      check(resp, { [`POST /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/files 200 (${s.originalName})`]: (r) => r.status === 200 });
      if (file && file.fileUid) uploaded.push({ fileUid: file.fileUid, name: fileName, type: s.type, tags: tags });
    }

    // Trigger processing for all files in one call
    const fileUids = uploaded.map((f) => f.fileUid);
    const pRes = http.request(
      "POST",
      `${artifactPublicHost}/v1alpha/catalogs/files/processAsync`,
      JSON.stringify({ fileUids }),
      data.header
    );
    check(pRes, { "POST /v1alpha/catalogs/files/processAsync 200": (r) => r.status === 200 });

    // Wait for completion (batched polling), then verify file-catalog view per file
    {
      const pending = new Set(uploaded.map((f) => f.fileUid));
      let completedCount = 0;
      let lastBatch = [];
      for (let iter = 0; iter < 3600 && pending.size > 0; iter++) {
        lastBatch = http.batch(
          Array.from(pending).map((uid) => ({
            method: "GET",
            url: `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files/${uid}`,
            params: data.header,
          }))
        );
        let idx = 0;
        for (const uid of Array.from(pending)) {
          const r = lastBatch[idx++];
          try {
            const body = r.json();
            const st = (body.file && body.file.processStatus) || "";
            if (r.status === 200 && st === "FILE_PROCESS_STATUS_COMPLETED") {
              pending.delete(uid);
              completedCount++;
            } else if (r.status === 200 && st === "FILE_PROCESS_STATUS_FAILED") {
              // Abort test if any file fails
              check(false, { [`File processing failed for ${body.file.name}`]: () => false });
              return; // Exit the function immediately
            }
          } catch (e) { /* ignore */ }
        }
        if (pending.size === 0) break;
        sleep(0.5);
      }
      check({ status: pending.size === 0 ? 200 : 0 }, { [`All files reached COMPLETED (${completedCount}/${uploaded.length})`]: () => pending.size === 0 });
    }

    for (const f of uploaded) {
      var viewPath = `/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files?filter.fileUids=${f.fileUid}`
      // Get the catalog file view (file-specific) per API doc
      const viewRes = http.request("GET", artifactPublicHost + viewPath, null, data.header);
      if (viewRes.status !== 200) {
        try { console.log(`Catalog view failed (${f.type}) status=${viewRes.status} body=${JSON.stringify(viewRes.json())}`); } catch (e) { console.log(`Catalog view failed (${f.type}) status=${viewRes.status}`); }
      }
      check(viewRes, {
        [`GET ${viewPath} 200 (${f.name}: ${f.type})`]: (r) => r.status === 200,
        [`GET ${viewPath} file has name (${f.name}: ${f.type})`]: (r) => r.json().files[0].name === f.name,
        [`GET ${viewPath} file process status is COMPLETED (${f.name}: ${f.type})`]: (r) => r.json().files[0].processStatus === "FILE_PROCESS_STATUS_COMPLETED",
        [`GET ${viewPath} file has creatorUid (${f.name}: ${f.type})`]: (r) => r.json().files[0].creatorUid === data.expectedOwner.uid,
        [`GET ${viewPath} file has size (${f.name}: ${f.type})`]: (r) => r.json().files[0].size > 0,
        [`GET ${viewPath} file has totalChunks (${f.name}: ${f.type})`]: (r) => r.json().files[0].totalChunks > 0,
        [`GET ${viewPath} file has totalTokens (${f.name}: ${f.type})`]: (r) => r.json().files[0].totalTokens > 0,
        [`GET ${viewPath} file has summary (${f.name}: ${f.type})`]: (r) => r.json().files[0].summary.length > 0,
        [`GET ${viewPath} file has downloadUrl (${f.name}: ${f.type})`]: (r) => r.json().files[0].downloadUrl.includes("v1alpha/blob-urls/"),
        [`GET ${viewPath} file has tags (${f.name}: ${f.type})`]: (r) => {
          const fileData = r.json().files[0];
          return Array.isArray(fileData.tags) && fileData.tags.length === f.tags.length &&
                 f.tags.every(tag => fileData.tags.includes(tag));
        },
      });

      // Check conversion pipeline and page information depending on the file type
      const isDocumentType = ["FILE_TYPE_PDF", "FILE_TYPE_DOC", "FILE_TYPE_DOCX", "FILE_TYPE_PPT", "FILE_TYPE_PPTX"].includes(f.type);
      const isTextType = ["FILE_TYPE_TEXT", "FILE_TYPE_MARKDOWN"].includes(f.type);

      if (isDocumentType) {
        // For document types, check that length unit is pages and coordinates contain page count
        const fileData = viewRes.json().files[0];
        check(viewRes, {
          [`GET ${viewPath} file has length unit UNIT_PAGE (${f.name}: ${f.type})`]: () => fileData.length && fileData.length.unit === "UNIT_PAGE",
          [`GET ${viewPath} file has length coordinates (${f.name}: ${f.type})`]: () => fileData.length && Array.isArray(fileData.length.coordinates) && fileData.length.coordinates.length > 0,
          [`GET ${viewPath} file length coordinates is positive (${f.name}: ${f.type})`]: () => fileData.length && fileData.length.coordinates[0] > 0,
        });
      } else if (isTextType) {
        // For text and markdown types, check that length unit is characters and coordinates contain character count
        const fileData = viewRes.json().files[0];
        check(viewRes, {
          [`GET ${viewPath} file has length unit UNIT_CHARACTER (${f.name}: ${f.type})`]: () => fileData.length && fileData.length.unit === "UNIT_CHARACTER",
          [`GET ${viewPath} file has length coordinates (${f.name}: ${f.type})`]: () => fileData.length && Array.isArray(fileData.length.coordinates) && fileData.length.coordinates.length > 0,
          [`GET ${viewPath} file length coordinates is positive (${f.name}: ${f.type})`]: () => fileData.length && fileData.length.coordinates[0] > 0,
        });
      }
    }

    // List catalog files
    const listFilesRes = http.request("GET", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files?pageSize=100`, null, data.header);
    let listFilesJson; try { listFilesJson = listFilesRes.json(); } catch (e) { listFilesJson = {}; }
    if (!(Array.isArray(listFilesJson.files) && listFilesJson.files.length === uploaded.length)) {
      console.log(`List files size mismatch: got=${(listFilesJson.files || []).length} expected=${uploaded.length}`);
    }
    check(listFilesRes, {
      [`GET /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/files?pageSize=100 200`]: (r) => r.status === 200,
      [`GET /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/files?pageSize=100 catalog files is array`]: () => Array.isArray(listFilesJson.files),
      [`GET /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/files?pageSize=100 catalog files count matches uploads`]: () => Array.isArray(listFilesJson.files) && listFilesJson.files.length === uploaded.length,
      [`GET /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/files?pageSize=100 files have tags`]: () => {
        const files = listFilesJson.files || [];
        return files.every(file => Array.isArray(file.tags) && file.tags.length > 0);
      },
    });

    // List catalog file chunks
    for (const fileUid of fileUids) {
      const listChunksRes = http.request("GET", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/chunks?fileUid=${fileUid}`, null, data.header);
      let listChunksJson; try { listChunksJson = listChunksRes.json(); } catch (e) { listChunksJson = {}; }
      check(listChunksRes, {
        [`GET /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/chunks?fileUid={file_uid} 200`]: (r) => r.status === 200,
        [`GET /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/chunks?fileUid=${fileUid} catalog chunks is array`]: () => Array.isArray(listChunksJson.chunks),
      });
    }

    // Get summary from a catalog file
    for (const fileUid of fileUids) {
      const getSummaryRes = http.request("GET", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files/${fileUid}/summary`, null, data.header);
      let summaryJson; try { summaryJson = getSummaryRes.json(); } catch (e) { summaryJson = {}; }
      check(getSummaryRes, {
        [`GET /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/files/{file_uid}/summary 200`]: (r) => r.status === 200,
      });
    }

    // Update PDF file tags before retrieval tests
    {
      const pdfFile = uploaded.find(f => f.type === "FILE_TYPE_PDF");
      if (pdfFile) {
        const updateTagsBody = {
          tags: ["scott", "kim"]
        };
        const updateTagsRes = http.request(
          "PUT",
          `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/files/${pdfFile.fileUid}/tags`,
          JSON.stringify(updateTagsBody),
          data.header
        );
        let updateTagsJson; try { updateTagsJson = updateTagsRes.json(); } catch (e) { updateTagsJson = {}; }
        const updatedFile = updateTagsJson.file || {};
        const updatedTags = updatedFile.tags || [];

        check(updateTagsRes, {
          [`PUT /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/files/{file_uid}/tags 200`]: (r) => r.status === 200,
          [`PUT /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/files/{file_uid}/tags returns file object`]: () => updatedFile && updatedFile.fileUid,
          [`PUT /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/files/{file_uid}/tags returns correct tags`]: () =>
            Array.isArray(updatedTags) &&
            updatedTags.length === 2 &&
            updatedTags.includes("scott") &&
            updatedTags.includes("kim"),
        });
      }
    }

    // Chunk similarity search tests
    {
      // Test 1: Search with a combination of tags that returns all files
      const searchBody1 = {
        textPrompt: "test file markdown",
        topK: 10,
        tags: ["scott", "kim"],
        contentType: "CONTENT_TYPE_CHUNK"
      };
      const searchRes1 = http.request(
        "POST",
        `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/chunks/retrieve`,
        JSON.stringify(searchBody1),
        data.header
      );
      let searchJson1; try { searchJson1 = searchRes1.json(); } catch (e) { searchJson1 = {}; }
      const similarChunks1 = searchJson1.similarChunks || [];

      check(searchRes1, {
        [`POST /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/chunks/retrieve 200 (all files)`]: (r) => r.status === 200,
        [`POST /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/chunks/retrieve returns similarChunks array (all files)`]: () => Array.isArray(similarChunks1),
        [`POST /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/chunks/retrieve returns results (all files)`]: () => similarChunks1.length > 0,
        [`POST /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/chunks/retrieve chunks have similarity scores (all files)`]: () =>
          similarChunks1.every(chunk => typeof chunk.similarityScore === 'number' && chunk.similarityScore >= 0),
        [`POST /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/chunks/retrieve chunks have metadata (all files)`]: () =>
          similarChunks1.every(chunk => chunk.chunkMetadata && chunk.chunkMetadata.originalFileUid),
      });

      // Test 2: Search with only PDF tag
      const searchBody2 = {
        textPrompt: "test file markdown",
        topK: 10,
        tags: ["scott"],
        contentType: "CONTENT_TYPE_CHUNK"
      };
      const searchRes2 = http.request(
        "POST",
        `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/chunks/retrieve`,
        JSON.stringify(searchBody2),
        data.header
      );
      let searchJson2; try { searchJson2 = searchRes2.json(); } catch (e) { searchJson2 = {}; }
      const similarChunks2 = searchJson2.similarChunks || [];

      // Find the PDF file UID for validation
      const pdfFile = uploaded.find(f => f.type === "FILE_TYPE_PDF");
      const pdfFileUid = pdfFile ? pdfFile.fileUid : null;

      check(searchRes2, {
        [`POST /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/chunks/retrieve 200 (pdf file filtered by tag)`]: (r) => r.status === 200,
        [`POST /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/chunks/retrieve returns similarChunks array (pdf file filtered by tag)`]: () => Array.isArray(similarChunks2),
        [`POST /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/chunks/retrieve returns results (pdf file filtered by tag)`]: () => similarChunks2.length > 0,
        [`POST /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/chunks/retrieve returns only PDF results (pdf file filtered by tag)`]: () =>
          pdfFileUid && similarChunks2.every(chunk => chunk.chunkMetadata.originalFileUid === pdfFileUid),
      });

      // Test 3: Verify that document types (PDF, DOC, DOCX, PPT, PPTX) have page-based references starting and ending at page 1
      const documentTypes = ["FILE_TYPE_PDF", "FILE_TYPE_DOC", "FILE_TYPE_DOCX", "FILE_TYPE_PPT", "FILE_TYPE_PPTX"];
      const documentFiles = uploaded.filter(f => documentTypes.includes(f.type));

      if (documentFiles.length > 0) {
        // Get file UIDs for document types
        const documentFileUids = documentFiles.map(f => f.fileUid);

        // Search specifically for document types using fileUids filter
        const searchBody3 = {
          textPrompt: "test file markdown",
          topK: 50, // Higher limit to catch more document types
          fileUids: documentFileUids,
          contentType: "CONTENT_TYPE_CHUNK"
        };
        const searchRes3 = http.request(
          "POST",
          `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}/chunks/retrieve`,
          JSON.stringify(searchBody3),
          data.header
        );
        let searchJson3; try { searchJson3 = searchRes3.json(); } catch (e) { searchJson3 = {}; }
        const similarChunks3 = searchJson3.similarChunks || [];

        check(searchRes3, {
          [`POST /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/chunks/retrieve 200 (document types check)`]: (r) => r.status === 200,
          [`POST /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/chunks/retrieve returns results for document types`]: () => similarChunks3.length > 0,
          [`POST /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/chunks/retrieve document types have page references`]: () => {
            return similarChunks3.every(chunk =>
              chunk.chunkMetadata.reference &&
              chunk.chunkMetadata.reference.start &&
              chunk.chunkMetadata.reference.start.unit === "UNIT_PAGE" &&
              Array.isArray(chunk.chunkMetadata.reference.start.coordinates) &&
              chunk.chunkMetadata.reference.start.coordinates.length > 0
            );
          },
          [`POST /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/chunks/retrieve document types start at page 1`]: () => {
            return similarChunks3.every(chunk =>
              chunk.chunkMetadata.reference &&
              chunk.chunkMetadata.reference.start &&
              chunk.chunkMetadata.reference.start.unit === "UNIT_PAGE" &&
              Array.isArray(chunk.chunkMetadata.reference.start.coordinates) &&
              chunk.chunkMetadata.reference.start.coordinates.length > 0 &&
              chunk.chunkMetadata.reference.start.coordinates[0] === 1
            );
          },
          [`POST /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id}/chunks/retrieve document types end at page 1`]: () => {
            return similarChunks3.every(chunk =>
              chunk.chunkMetadata.reference &&
              chunk.chunkMetadata.reference.end &&
              chunk.chunkMetadata.reference.end.unit === "UNIT_PAGE" &&
              Array.isArray(chunk.chunkMetadata.reference.end.coordinates) &&
              chunk.chunkMetadata.reference.end.coordinates.length > 0 &&
              chunk.chunkMetadata.reference.end.coordinates[0] === 1
            );
          },
        });
      }
    }

    // Delete the catalog (cleanup)
    const dRes = http.request("DELETE", `${artifactPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/catalogs/${catalogId}`, null, data.header);
    check(dRes, { "DELETE /v1alpha/namespaces/{namespace_id}/catalogs/{catalog_id} 200": (r) => r.status === 200 || r.status === 204 });
  });
}
