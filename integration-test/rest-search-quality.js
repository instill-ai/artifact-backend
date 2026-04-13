import { check, group, sleep } from "k6";
import { randomString } from "https://jslib.k6.io/k6-utils/1.1.0/index.js";

import * as constant from "./const.js";
import * as helper from "./helper.js";

const http = helper.httpRetry;

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
  check(true, { [constant.banner('Search Quality: Setup')]: () => true });

  helper.staggerTestExecution(3);

  const dbIDPrefix = constant.generateDBIDPrefix();
  console.log(`rest-search-quality.js: Using unique test prefix: ${dbIDPrefix}`);

  const authHeader = helper.getBasicAuthHeader(constant.defaultUsername, constant.defaultPassword);
  var header = {
    "headers": {
      "Authorization": authHeader,
      "Content-Type": "application/json",
    },
    "timeout": "600s",
  };

  var resp = http.request("GET", `${constant.mgmtRESTPublicHost}/v1beta/user`, {}, {
    headers: { "Authorization": authHeader }
  });
  const nsId = resp.json().user.id;

  return {
    header: header,
    nsId: nsId,
    dbIDPrefix: dbIDPrefix,
  };
}

export default function (data) {
  const { header, nsId, dbIDPrefix } = data;

  group("Search Quality - Augmented Chunks and Entity Graph", () => {
    const kbId = `test-${dbIDPrefix}-search-quality-${randomString(4)}`;

    // Create KB
    let createResp = http.request("POST",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${nsId}/knowledge-bases`,
      JSON.stringify({ id: kbId, displayName: "Search Quality Test KB" }),
      header
    );
    check(createResp, { "KB created": (r) => r.status === 200 || r.status === 201 });

    // Upload a markdown test file with entity-rich content
    const markdownContent = btoa([
      "# Monopoly Theory in Business Strategy",
      "",
      "Peter Thiel argues in Zero to One that creative monopoly is the key to",
      "building successful technology companies. Unlike perfect competition,",
      "which drives profits to zero, monopolies can invest in innovation.",
      "",
      "## Key Concepts",
      "",
      "Network effects, economies of scale, and proprietary technology create",
      "sustainable competitive advantages. PayPal demonstrated these principles",
      "during its growth phase.",
      "",
      "## Venture Capital Perspective",
      "",
      "Venture capitalists like those at Founders Fund look for companies with",
      "monopoly potential. The startup ecosystem in Silicon Valley rewards",
      "bold bets on non-consensus ideas.",
    ].join("\n"));

    let uploadResp = http.request("POST",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${nsId}/knowledge-bases/${kbId}/files`,
      JSON.stringify({
        displayName: "monopoly-theory.md",
        type: "FILE_TYPE_MARKDOWN",
        content: markdownContent,
      }),
      header
    );
    check(uploadResp, { "File uploaded": (r) => r.status === 200 || r.status === 201 });

    if (uploadResp.status === 200 || uploadResp.status === 201) {
      // Wait for file processing
      const fileId = uploadResp.json().file.id;
      let processed = false;
      for (let i = 0; i < 60; i++) {
        sleep(2);
        let statusResp = http.request("GET",
          `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${nsId}/knowledge-bases/${kbId}/files/${fileId}`,
          null, header
        );
        if (statusResp.status === 200) {
          const status = statusResp.json().file.processStatus;
          if (status === "FILE_PROCESS_STATUS_COMPLETED") {
            processed = true;
            break;
          }
          if (status === "FILE_PROCESS_STATUS_FAILED") {
            console.log("File processing failed");
            break;
          }
        }
      }

      check(processed, { "File processed successfully": (p) => p === true });

      if (processed) {
        // Search for a concept that should match via augmented chunk
        let searchResp = http.request("POST",
          `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${nsId}/knowledge-bases/${kbId}/chunks/search`,
          JSON.stringify({
            textPrompt: "monopoly",
            topK: 10,
          }),
          header
        );
        check(searchResp, {
          "Search returns results": (r) => r.status === 200,
          "Search has chunks": (r) => {
            if (r.status !== 200) return false;
            const chunks = r.json().similarChunks || [];
            return chunks.length > 0;
          },
          "Search results have meaningful scores": (r) => {
            if (r.status !== 200) return false;
            const chunks = r.json().similarChunks || [];
            if (chunks.length === 0) return false;
            return chunks[0].similarityScore > 0;
          },
        });
      }
    }

    // Cleanup
    http.request("DELETE",
      `${constant.artifactRESTPublicHost}/v1alpha/namespaces/${nsId}/knowledge-bases/${kbId}`,
      null, header
    );
  });
}

export function teardown(data) {
  check(true, { [constant.banner('Search Quality: Teardown')]: () => true });
}
