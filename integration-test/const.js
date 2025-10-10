import sql from 'k6/x/sql';
import driver from 'k6/x/sql/driver/postgres';

import { uuidv4 } from "https://jslib.k6.io/k6-utils/1.4.0/index.js";
import encoding from "k6/encoding";

let proto;

export const apiGatewayMode = (__ENV.API_GATEWAY_URL && true);

if (__ENV.API_GATEWAY_PROTOCOL) {
  if (__ENV.API_GATEWAY_PROTOCOL !== "http" && __ENV.API_GATEWAY_PROTOCOL != "https") {
    fail("only allow `http` or `https` for API_GATEWAY_PROTOCOL")
  }
  proto = __ENV.API_GATEWAY_PROTOCOL
} else {
  proto = "http"
}

export const artifactPrivateHost = `http://artifact-backend:3082`;
export const artifactPublicHost = apiGatewayMode ? `${proto}://${__ENV.API_GATEWAY_URL}` : `http://api-gateway:8080`
export const mgmtPublicHost = apiGatewayMode ? `${proto}://${__ENV.API_GATEWAY_URL}` : `http://api-gateway:8080`
export const artifactGRPCPrivateHost = `artifact-backend:3082`;
export const artifactGRPCPublicHost = apiGatewayMode ? `${__ENV.API_GATEWAY_URL}` : `api-gateway:8080`;
export const mgmtGRPCPublicHost = apiGatewayMode ? `${__ENV.API_GATEWAY_URL}` : `api-gateway:8080`;
export const mgmtGRPCPrivateHost = `mgmt-backend:3084`;
export const mgmtVersion = 'v1beta';

export const namespace = "users/admin"
export const defaultUsername = "admin"
export const defaultPassword = "password"

export const params = {
  headers: {
    "Content-Type": "application/json",
  },
  timeout: "300s",
};

export const paramsGrpc = {
  metadata: {
    "Content-Type": "application/json",
  },
  timeout: "300s",
};

const randomUUID = uuidv4();
export const paramsGRPCWithJwt = {
  metadata: {
    "Content-Type": "application/json",
    "Instill-User-Uid": randomUUID,
  },
};

export const paramsHTTPWithJWT = {
  headers: {
    "Content-Type": "application/json",
    "Instill-User-Uid": randomUUID,
  },
};

// Test data files
export const sampleDoc = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/sample.doc`, "b")
);
export const sampleDocx = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/sample.docx`, "b")
);
export const sampleUppercaseDoc = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/SAMPLE-UPPERCASE-FILENAME.DOC`, "b")
);
export const sampleUppercaseDocx = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/SAMPLE-UPPERCASE-FILENAME.DOCX`, "b")
);
export const sampleTxt = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/sample.txt`, "b")
);
export const sampleHtml = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/sample.html`, "b")
);
export const sampleMd = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/sample.md`, "b")
);
export const sampleCsv = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/sample.csv`, "b")
);
export const samplePdf = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/sample.pdf`, "b")
);
export const samplePpt = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/sample.ppt`, "b")
);
export const samplePptx = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/sample.pptx`, "b")
);
export const sampleXls = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/sample.xls`, "b")
);
export const sampleXlsx = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/sample.xlsx`, "b")
);

// Mapping of sample files to their intended artifact file types
export const sampleFiles = [
  { originalName: "sample.txt", type: "FILE_TYPE_TEXT", content: sampleTxt },
  { originalName: "sample.md", type: "FILE_TYPE_MARKDOWN", content: sampleMd },
  { originalName: "sample.csv", type: "FILE_TYPE_CSV", content: sampleCsv },
  { originalName: "sample.html", type: "FILE_TYPE_HTML", content: sampleHtml },
  { originalName: "sample.pdf", type: "FILE_TYPE_PDF", content: samplePdf },
  { originalName: "sample.ppt", type: "FILE_TYPE_PPT", content: samplePpt },
  { originalName: "sample.pptx", type: "FILE_TYPE_PPTX", content: samplePptx },
  { originalName: "sample.xls", type: "FILE_TYPE_XLS", content: sampleXls },
  { originalName: "sample.xlsx", type: "FILE_TYPE_XLSX", content: sampleXlsx },
  { originalName: "sample.doc", type: "FILE_TYPE_DOC", content: sampleDoc },
  { originalName: "sample.docx", type: "FILE_TYPE_DOCX", content: sampleDocx },
  { originalName: "SAMPLE-UPPERCASE-FILENAME.DOC", type: "FILE_TYPE_DOC", content: sampleUppercaseDoc },
  { originalName: "SAMPLE-UPPERCASE-FILENAME.DOCX", type: "FILE_TYPE_DOCX", content: sampleUppercaseDocx },
];

let dbHost = 'localhost';
if (__ENV.DB_HOST) {
  dbHost = __ENV.DB_HOST;
}

export const db = sql.open(driver, `postgresql://postgres:password@${dbHost}:5432/artifact?sslmode=disable`);

// Since the tests rely on a pre-existing user, this prefix is used to clean
// up only the resources generated by these tests.
export const dbIDPrefix = "integration-test-";

// Pretty banner for k6 checks table; accounts for default left indent
export function banner(title) {
  const content = `║ ${title} ║`;
  const topBorder = "╔" + "═".repeat(content.length - 1) + "╗";
  const bottomBorder = "╚" + "═".repeat(content.length - 1) + "╝";
  const indent = "      ";
  return [topBorder, indent + content, indent + bottomBorder].join("\n");
}

// MinIO configuration for integration tests
export const minioConfig = {
  host: 'minio',
  port: 9000,
  user: 'minioadmin',
  password: 'minioadmin',
  bucket: 'core-artifact',
  blobBucket: 'core-blob',
};
