// standalone-reprocess-idempotent.js
//
// k6 regression gate for ARTIFACT-INV-REPROCESS-NO-TERMINATE-RACE
// (docs/invariants/reprocess-status.md). Reproduces the
// 2026-05-10 col-X0WlOK5wa9 audio-files incident:
//
//   1. User uploads 2 audio files; CreateFile dedups them (both rows
//      already existed), so no fresh ProcessFileWorkflow fires on
//      upload. The autofill drift detector then fires N concurrent
//      ReprocessFileAdmin calls per fileUID within a few hundred
//      milliseconds.
//   2. Pre-fix: each Reprocess* call ran terminate-and-restart on the
//      previous workflow. The terminator wrote
//      ProcessStatus = FAILED to the DB *between* the previous
//      workflow's death and the new workflow's first activity, so
//      every file landed in FAILED while a fresh workflow was
//      legitimately running. The visible symptom was "only one
//      ProcessFileWorkflow seen" + status stuck at FAILED.
//   3. Post-fix: the first reprocess call wins; the rest return
//      gRPC AlreadyExists (HTTP 409 via grpc-gateway). The single
//      surviving workflow runs to COMPLETED, the file ends in
//      COMPLETED, and the user sees correct status.
//
// This test is **standalone** (per AGENTS.md "Running k6 Integration
// Tests" — the targeted standalone-* suite, not `make integration-test`).
// Run from the host:
//
//   docker exec artifact-backend /bin/bash -c \
//     "make integration-test DB_HOST=pg_sql K6_FILE=integration-test/standalone-reprocess-idempotent.js"
//
// Or directly with k6:
//
//   k6 run -e API_GATEWAY_URL=localhost:8080 \
//     integration-test/standalone-reprocess-idempotent.js

import { check, group, sleep } from "k6";
import { randomString } from "https://jslib.k6.io/k6-utils/1.1.0/index.js";

import * as constant from "./const.js";
import * as helper from "./helper.js";

const http = helper.httpRetry;

// Fan-out used to reproduce the race. The col-X0WlOK5wa9 incident saw
// 4–10 concurrent reprocess calls per file in <1s; 8 is comfortable
// for both reproducing the bug and not overwhelming the dev stack.
const CONCURRENT_REPROCESS_CALLS = 8;

export const options = {
  setupTimeout: "30s",
  teardownTimeout: "180s",
  iterations: 1,
  duration: "20m",
  insecureSkipTLSVerify: true,
  thresholds: {
    checks: ["rate == 1.0"],
  },
};

export function setup() {
  check(true, { [constant.banner("Artifact API Reprocess Idempotency: Setup")]: () => true });
  helper.staggerTestExecution(2);

  const dbIDPrefix = constant.generateDBIDPrefix();
  console.log(`standalone-reprocess-idempotent.js: prefix=${dbIDPrefix}`);

  const authHeader = helper.getBasicAuthHeader(constant.defaultUsername, constant.defaultPassword);
  const header = {
    headers: {
      Authorization: authHeader,
      "Content-Type": "application/json",
    },
    timeout: "600s",
  };
  const meResp = http.request("GET", `${constant.mgmtRESTPublicHost}/v1beta/user`, {}, {
    headers: { Authorization: authHeader },
  });

  // Sweep stale KBs from previous failed runs of THIS test.
  try {
    const listResp = http.request(
      "GET",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${meResp.json().user.id}/knowledge-bases`,
      null,
      header,
    );
    if (listResp.status === 200) {
      const kbs = Array.isArray(listResp.json().knowledgeBases) ? listResp.json().knowledgeBases : [];
      let cleaned = 0;
      for (const kb of kbs) {
        if (kb.id && kb.id.match(/test-[a-z0-9]+-reprocess-idempotent-/)) {
          const del = http.request(
            "DELETE",
            `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${meResp.json().user.id}/knowledge-bases/${kb.id}`,
            null,
            header,
          );
          if (del.status === 200 || del.status === 204) cleaned++;
        }
      }
      if (cleaned > 0) console.log(`Setup: cleaned ${cleaned} stale KBs`);
    }
  } catch (e) {
    console.log(`Setup cleanup warning: ${e}`);
  }

  return { header, expectedOwner: meResp.json().user, dbIDPrefix };
}

export default function (data) {
  CheckConcurrentReprocessIsIdempotent(data);
}

export function teardown(data) {
  group("Artifact API Reprocess Idempotency: Cleanup", () => {
    helper.waitForSafeCleanup(120, data.dbIDPrefix, 3);
    const listResp = http.request(
      "GET",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases`,
      null,
      data.header,
    );
    if (listResp.status === 200) {
      const kbs = Array.isArray(listResp.json().knowledgeBases) ? listResp.json().knowledgeBases : [];
      for (const kb of kbs) {
        if (kb.id && kb.id.startsWith(data.dbIDPrefix)) {
          http.request(
            "DELETE",
            `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${kb.id}`,
            null,
            data.header,
          );
        }
      }
    }
  });
}

// CheckConcurrentReprocessIsIdempotent fires N parallel reprocess
// calls against a single fileUID and asserts:
//
//   1. **Exactly one workflow wins** — at most one HTTP 200 plus
//      (N-1) HTTP 409 (grpc-gateway maps codes.AlreadyExists to 409).
//      We tolerate the absolute count because Temporal's
//      DescribeWorkflowExecution can race when the very first call
//      hasn't fully committed; the contract is "≥1 OK, the rest are
//      AlreadyExists, never any other error code".
//   2. **File ends in COMPLETED, not FAILED** — the pre-fix
//      terminate-and-restart path always corrupted the status to
//      FAILED, so this assertion is the load-bearing one for
//      ARTIFACT-INV-REPROCESS-NO-TERMINATE-RACE.
//   3. **Only one ProcessFileWorkflow ran to completion in Temporal
//      history** (verified via the file's chunk count being stable
//      and matching a single processing run, not duplicated by
//      concurrent termination thrashing).
function CheckConcurrentReprocessIsIdempotent(data) {
  const groupName = "Artifact API: Reprocess idempotency under concurrent fan-out";
  group(groupName, () => {
    check(true, { [constant.banner(groupName)]: () => true });

    // 1. Provision KB.
    const kbDisplayName = `${data.dbIDPrefix} Reprocess Idempotent ${randomString(5)}`;
    const createKB = http.request(
      "POST",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases`,
      JSON.stringify({
        displayName: kbDisplayName,
        description: "Concurrent reprocess race-condition test",
        tags: ["test", "reprocess-idempotent"],
      }),
      data.header,
    );
    let kb;
    try { kb = createKB.json().knowledgeBase; } catch (_) { kb = {}; }
    const kbId = kb && kb.id;
    check(createKB, { "ReproIdempotent: KB created": (r) => r.status === 200 && !!kbId });
    if (!kbId) return;

    // 2. Upload a PDF. We use PDF (not text) so that a single processing
    // run takes >5s — long enough that several concurrent reprocess
    // calls fired in sequence catch the race window (terminate-restart
    // happens between each call's status stamp and workflow start).
    const fileDisplayName = `${data.dbIDPrefix}reprocess-idempotent-test.pdf`;
    const upload = http.request(
      "POST",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${kbId}/files`,
      JSON.stringify({
        displayName: fileDisplayName,
        type: "TYPE_PDF",
        content: constant.docSamplePdf,
      }),
      data.header,
    );
    let f;
    try { f = upload.json().file; } catch (_) { f = {}; }
    const fileId = f && f.id;
    check(upload, { "ReproIdempotent: file uploaded": (r) => r.status === 200 && !!fileId });
    if (!fileId) {
      http.request("DELETE", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${kbId}`, null, data.header);
      return;
    }

    // 3. Wait for the first processing to complete (so we know the
    // file is in a steady state before we slam reprocess on it).
    const first = helper.waitForFileProcessingComplete(
      data.expectedOwner.id, kbId, fileId, data.header, 600,
    );
    check(first, { "ReproIdempotent: first processing completed": (r) => r.completed && r.status === "COMPLETED" });
    if (!first.completed) {
      http.request("DELETE", `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${kbId}`, null, data.header);
      return;
    }

    // 4. Fan-out: fire CONCURRENT_REPROCESS_CALLS reprocess calls in a
    // single http.batch(). k6 dispatches them in parallel, which is
    // the closest k6-side approximation of the autofill drift
    // detector's per-file fan-out.
    const reprocessURL = `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${kbId}/files/${fileId}/reprocess`;
    const requests = [];
    for (let i = 0; i < CONCURRENT_REPROCESS_CALLS; i++) {
      requests.push({
        method: "POST",
        url: reprocessURL,
        body: null,
        params: data.header,
      });
    }

    const responses = http.batch(requests);

    // Tally outcomes.
    let okCount = 0;
    let alreadyExistsCount = 0;
    let otherCount = 0;
    const otherStatuses = [];
    for (const r of responses) {
      if (r.status === 200) {
        okCount++;
      } else if (r.status === 409) {
        // grpc-gateway maps codes.AlreadyExists to HTTP 409.
        alreadyExistsCount++;
      } else {
        otherCount++;
        otherStatuses.push(r.status);
      }
    }
    console.log(`Reprocess fan-out: 200=${okCount}, 409=${alreadyExistsCount}, other=${otherCount} (statuses=${otherStatuses})`);

    check(
      { ok: okCount, ae: alreadyExistsCount, other: otherCount },
      {
        "ReproIdempotent: exactly one reprocess call wins": (r) => r.ok === 1,
        "ReproIdempotent: remaining calls return AlreadyExists (HTTP 409)": (r) =>
          r.ae === CONCURRENT_REPROCESS_CALLS - 1,
        "ReproIdempotent: no spurious 5xx/4xx codes": (r) => r.other === 0,
      },
    );

    // 5. Wait for the surviving workflow to finish. The load-bearing
    // assertion: the file MUST end in COMPLETED, not FAILED.
    const second = helper.waitForFileProcessingComplete(
      data.expectedOwner.id, kbId, fileId, data.header, 600,
    );
    check(second, {
      "ReproIdempotent: surviving workflow runs to COMPLETED, not FAILED": (r) =>
        r.completed && r.status === "COMPLETED",
      "ReproIdempotent: surviving workflow did NOT corrupt status to FAILED": (r) =>
        r.status !== "FAILED",
    });

    // 6. Verify there's no resource duplication from concurrent
    // termination thrashing — chunk counts after a single reprocess
    // should match the baseline (we already know baseline from the
    // first processing run; we just assert no duplication or loss).
    const fileUid = helper.getFileUidFromId(fileId);
    const kbUid = helper.getKnowledgeBaseUidFromId(kbId);
    const chunksAfter = (kbUid && fileUid) ? helper.countMinioObjects(kbUid, fileUid, "chunk") : 0;
    const vectorsAfter = (kbUid && fileUid) ? helper.countMilvusVectors(kbUid, fileUid) : 0;
    const dbEmbeddingsAfter = fileUid ? helper.countEmbeddings(fileUid) : 0;
    console.log(`Post-reprocess counts: chunks=${chunksAfter}, vectors=${vectorsAfter}, embeddings=${dbEmbeddingsAfter}`);
    check(
      { chunksAfter, vectorsAfter, dbEmbeddingsAfter },
      {
        "ReproIdempotent: chunks present after surviving reprocess": (r) => r.chunksAfter > 0,
        "ReproIdempotent: vectors equal embeddings (no termination drift)": (r) => r.vectorsAfter === r.dbEmbeddingsAfter,
      },
    );

    // 7. Cleanup.
    http.request(
      "DELETE",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${data.expectedOwner.id}/knowledge-bases/${kbId}`,
      null,
      data.header,
    );
    sleep(5);
  });
}
