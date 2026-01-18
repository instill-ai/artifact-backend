import sql from 'k6/x/sql';
import driver from 'k6/x/sql/driver/postgres';

import { uuidv4 } from "https://jslib.k6.io/k6-utils/1.4.0/index.js";
import encoding from "k6/encoding";

let proto;

if (__ENV.API_GATEWAY_PROTOCOL) {
  if (__ENV.API_GATEWAY_PROTOCOL !== "http" && __ENV.API_GATEWAY_PROTOCOL != "https") {
    fail("only allow `http` or `https` for API_GATEWAY_PROTOCOL")
  }
  proto = __ENV.API_GATEWAY_PROTOCOL
} else {
  proto = "http"
}

// API Gateway URL (localhost:8080 from host, api-gateway:8080 from container)
const apiGatewayUrl = __ENV.API_GATEWAY_URL || "localhost:8080";

// Determine if running from host (localhost) or container
// Export for use in test files that need to skip certain tests when running from host
export const isHostMode = apiGatewayUrl === "localhost:8080";
// Backward compatibility alias (old name was confusing but test files use it)
export const apiGatewayMode = isHostMode;

// Public hosts (via API Gateway)
export const apiGatewayPublicHost = `${proto}://${apiGatewayUrl}`;
export const artifactRESTPublicHost = `${proto}://${apiGatewayUrl}`;
export const mgmtRESTPublicHost = `${proto}://${apiGatewayUrl}`;

export const artifactGRPCPublicHost = apiGatewayUrl;
export const mgmtGRPCPublicHost = apiGatewayUrl;

// Private hosts (direct backend, for internal service calls)
// Use localhost ports when running from host, container names when in container
export const artifactGRPCPrivateHost = isHostMode ? `localhost:3082` : `artifact-backend:3082`;
export const mgmtGRPCPrivateHost = isHostMode ? `localhost:3084` : `mgmt-backend:3084`;

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

// Test document sample data files
export const docSampleDoc = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/doc-sample.doc`, "b")
);
export const docSampleDocx = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/doc-sample.docx`, "b")
);
export const docSampleTxt = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/doc-sample.txt`, "b")
);
export const docSampleHtml = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/doc-sample.html`, "b")
);
export const docSampleMd = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/doc-sample.md`, "b")
);
export const docSampleCsv = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/doc-sample.csv`, "b")
);
export const docSamplePdf = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/doc-sample.pdf`, "b")
);
export const docSampleMultiPagePdf = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/doc-sample-multi-page.pdf`, "b")
);
export const docSamplePpt = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/doc-sample.ppt`, "b")
);
export const docSamplePptx = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/doc-sample.pptx`, "b")
);
export const docSampleXls = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/doc-sample.xls`, "b")
);
export const docSampleXlsx = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/doc-sample.xlsx`, "b")
);

// Test image sample data files
export const imageSamplePng = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/img-sample.png`, "b")
);
export const imageSampleJpeg = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/img-sample.jpeg`, "b")
);
export const imageSampleJpg = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/img-sample.jpg`, "b")
);
export const imageSampleGif = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/img-sample.gif`, "b")
);
export const imageSampleWebp = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/img-sample.webp`, "b")
);
export const imageSampleTiff = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/img-sample.tiff`, "b")
);
export const imageSampleTif = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/img-sample.tif`, "b")
);
export const imageSampleHeic = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/img-sample.heic`, "b")
);
export const imageSampleHeif = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/img-sample.heif`, "b")
);
export const imageSampleAvif = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/img-sample.avif`, "b")
);
export const imageSampleBmp = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/img-sample.bmp`, "b")
);

// Test audio sample data files
export const audioSampleMp3 = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/audio-sample.mp3`, "b")
);
export const audioSampleWav = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/audio-sample.wav`, "b")
);
export const audioSampleAac = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/audio-sample.aac`, "b")
);
export const audioSampleOgg = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/audio-sample.ogg`, "b")
);
export const audioSampleFlac = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/audio-sample.flac`, "b")
);
export const audioSampleAiff = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/audio-sample.aiff`, "b")
);
export const audioSampleM4a = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/audio-sample.m4a`, "b")
);
export const audioSampleWma = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/audio-sample.wma`, "b")
);
export const audioSampleWebmAudio = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/audio-sample.webm`, "b")
);

// Test video sample data files
export const videoSampleMp4 = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/video-sample.mp4`, "b")
);
export const videoSampleMkv = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/video-sample.mkv`, "b")
);
export const videoSampleAvi = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/video-sample.mp4`, "b")
);
export const videoSampleMov = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/video-sample.mkv`, "b")
);
export const videoSampleFlv = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/video-sample.flv`, "b")
);
export const videoSampleWmv = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/video-sample.wmv`, "b")
);
export const videoSampleMpeg = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/video-sample.mpeg`, "b")
);
export const videoSampleWebm = encoding.b64encode(
  open(`${__ENV.TEST_FOLDER_ABS_PATH}/integration-test/data/video-sample.webm`, "b")
);

// Mapping of sample files to their intended artifact file types
export const sampleFiles = [
  { originalName: "doc-sample.txt", type: "TYPE_TEXT", content: docSampleTxt },
  { originalName: "doc-sample.md", type: "TYPE_MARKDOWN", content: docSampleMd },
  { originalName: "doc-sample.csv", type: "TYPE_CSV", content: docSampleCsv },
  { originalName: "doc-sample.html", type: "TYPE_HTML", content: docSampleHtml },
  { originalName: "doc-sample.pdf", type: "TYPE_PDF", content: docSamplePdf },
  { originalName: "doc-sample.ppt", type: "TYPE_PPT", content: docSamplePpt },
  { originalName: "doc-sample.pptx", type: "TYPE_PPTX", content: docSamplePptx },
  { originalName: "doc-sample.xls", type: "TYPE_XLS", content: docSampleXls },
  { originalName: "doc-sample.xlsx", type: "TYPE_XLSX", content: docSampleXlsx },
  { originalName: "doc-sample.doc", type: "TYPE_DOC", content: docSampleDoc },
  { originalName: "doc-sample.docx", type: "TYPE_DOCX", content: docSampleDocx },
  { originalName: "img-sample.png", type: "TYPE_PNG", content: imageSamplePng },
  { originalName: "img-sample.jpeg", type: "TYPE_JPEG", content: imageSampleJpeg },
  { originalName: "img-sample.jpg", type: "TYPE_JPEG", content: imageSampleJpg },
  { originalName: "img-sample.gif", type: "TYPE_GIF", content: imageSampleGif },
  { originalName: "img-sample.webp", type: "TYPE_WEBP", content: imageSampleWebp },
  { originalName: "img-sample.tiff", type: "TYPE_TIFF", content: imageSampleTiff },
  { originalName: "img-sample.tif", type: "TYPE_TIFF", content: imageSampleTif },
  { originalName: "img-sample.heic", type: "TYPE_HEIC", content: imageSampleHeic },
  { originalName: "img-sample.heif", type: "TYPE_HEIF", content: imageSampleHeif },
  { originalName: "img-sample.avif", type: "TYPE_AVIF", content: imageSampleAvif },
  { originalName: "img-sample.bmp", type: "TYPE_BMP", content: imageSampleBmp },
  { originalName: "audio-sample.mp3", type: "TYPE_MP3", content: audioSampleMp3 },
  { originalName: "audio-sample.wav", type: "TYPE_WAV", content: audioSampleWav },
  { originalName: "audio-sample.aac", type: "TYPE_AAC", content: audioSampleAac },
  { originalName: "audio-sample.ogg", type: "TYPE_OGG", content: audioSampleOgg },
  { originalName: "audio-sample.flac", type: "TYPE_FLAC", content: audioSampleFlac },
  { originalName: "audio-sample.aiff", type: "TYPE_AIFF", content: audioSampleAiff },
  { originalName: "audio-sample.m4a", type: "TYPE_M4A", content: audioSampleM4a },
  { originalName: "audio-sample.wma", type: "TYPE_WMA", content: audioSampleWma },
  { originalName: "audio-sample.webm", type: "TYPE_WEBM_AUDIO", content: audioSampleWebmAudio },
  { originalName: "video-sample.mp4", type: "TYPE_MP4", content: videoSampleMp4 },
  { originalName: "video-sample.mkv", type: "TYPE_MKV", content: videoSampleMkv },
  { originalName: "video-sample.avi", type: "TYPE_AVI", content: videoSampleAvi },
  { originalName: "video-sample.mov", type: "TYPE_MOV", content: videoSampleMov },
  { originalName: "video-sample.flv", type: "TYPE_FLV", content: videoSampleFlv },
  { originalName: "video-sample.wmv", type: "TYPE_WMV", content: videoSampleWmv },
  { originalName: "video-sample.mpeg", type: "TYPE_MPEG", content: videoSampleMpeg },
  { originalName: "video-sample.webm", type: "TYPE_WEBM_VIDEO", content: videoSampleWebm },
];

let dbHost = 'localhost';
if (__ENV.DB_HOST) {
  dbHost = __ENV.DB_HOST;
}

// Database connections - artifact for KB/file data, mgmt for namespace/owner data
export const artifactDb = sql.open(driver, `postgresql://postgres:password@${dbHost}:5432/artifact?sslmode=disable`);
export const mgmtDb = sql.open(driver, `postgresql://postgres:password@${dbHost}:5432/mgmt?sslmode=disable`);

// Since the tests rely on a pre-existing user, this prefix is used to clean
// up only the resources generated by these tests.
import { randomString } from "https://jslib.k6.io/k6-utils/1.1.0/index.js";

// Add randomization to prevent collisions between parallel/sequential test runs
// Generate unique prefix per test file to avoid parallel test interference
// Format: test-{4 random chars}- = 11 chars, leaving 21 chars for knowledge-base-specific names
// CRITICAL: Each test MUST call this in setup() to get a unique prefix
// DO NOT create a const export - it would be shared across all parallel tests!
export function generateDBIDPrefix() {
  return `test-${randomString(4)}-`;
}

// Pretty banner for k6 checks table; accounts for default left indent
export function banner(title) {
  const content = `║ ${title} ║`;
  const topBorder = "╔" + "═".repeat(content.length - 1) + "╗";
  const bottomBorder = "╚" + "═".repeat(content.length - 1) + "╝";
  const indent = "      ";
  return [topBorder, indent + content, indent + bottomBorder].join("\n");
}

// MinIO configuration for integration tests
// Use localhost when running from host, minio when running in Docker container
// Port mapping: Container port 9000 is exposed as host port 19000
export const minioConfig = {
  host: isHostMode ? 'localhost' : 'minio',
  port: isHostMode ? 19000 : 9000,
  user: 'minioadmin',
  password: 'minioadmin',
  bucket: 'core-artifact',
  blobBucket: 'core-blob',
};

// Milvus configuration for integration tests
// Use localhost when running from host, milvus when running in Docker container
// Port mapping: Container port 19530 is exposed as host port 19530 (same)
export const milvusConfig = {
  host: isHostMode ? 'localhost' : 'milvus',
  port: 19530,
};
