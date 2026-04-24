package object

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path"
	"time"

	"cloud.google.com/go/storage"
	"github.com/minio/minio-go/v7"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/instill-ai/artifact-backend/pkg/types"

	errorsx "github.com/instill-ai/x/errors"
	logx "github.com/instill-ai/x/log"
)

// Streaming upload tuning. These are package-scope so the matching unit
// test in gcs_stream_test.go can reference them without duplicating magic
// numbers. See ARTIFACT-INV-GCS-STREAM-RESUME in artifact-backend/AGENTS.md.
const (
	// maxStreamResumes caps how many times StreamCopy will reopen the
	// source after a mid-stream truncation within a single call. Four or
	// more sequential truncations still fall through to the caller's
	// outer retry (e.g. Temporal) — an intentionally cheap degraded path
	// over the more expensive alternative of persisting GCS session URIs
	// across activity attempts.
	maxStreamResumes = 3

	// streamResumeGap is the cool-down between resumes. Long enough for a
	// transient MinIO TCP / keep-alive hiccup to clear, short enough to
	// stay inside any reasonable activity timeout. Tuned together with
	// the uploadCtx timeout below; if you raise one, revisit the other.
	streamResumeGap = 2 * time.Second

	// gcsResumableChunkSize matches the existing UploadFromReader chunking
	// (32 MiB per resumable upload chunk). Extracted as a named constant
	// so StreamCopy and UploadFromReader stay in lockstep.
	gcsResumableChunkSize = 32 * 1024 * 1024
)

// gcsStorage implements Storage interface for Google Cloud Storage
type gcsStorage struct {
	client        *storage.Client
	projectID     string
	region        string
	bucket        string
	serviceAccKey []byte
	logger        *zap.Logger
}

// GCSConfig holds GCS storage configuration
type GCSConfig struct {
	ProjectID         string
	Region            string
	Bucket            string
	ServiceAccountKey string // JSON string
}

// NewGCSStorage creates a new object.Storage implementation using GCS
func NewGCSStorage(ctx context.Context, config GCSConfig) (Storage, error) {
	if config.Bucket == "" {
		return nil, errorsx.AddMessage(
			errorsx.ErrInvalidArgument,
			"GCS bucket name is required",
		)
	}

	var opts []option.ClientOption
	var unwrappedSAKey []byte
	if config.ServiceAccountKey != "" {
		// The service account key might be wrapped in a Vault response structure
		// We need to extract the actual credentials from data.data if present
		saKeyBytes := []byte(config.ServiceAccountKey)

		// Try to parse the key to see if it's a Vault response
		var keyData map[string]interface{}
		if err := json.Unmarshal(saKeyBytes, &keyData); err == nil {
			// Check if this is a Vault response (has data.data structure)
			if data, ok := keyData["data"].(map[string]interface{}); ok {
				if innerData, ok := data["data"].(map[string]interface{}); ok {
					// Extract the actual service account key from data.data
					actualKey, err := json.Marshal(innerData)
					if err != nil {
						return nil, errorsx.AddMessage(
							fmt.Errorf("failed to marshal service account key: %w", err),
							"Unable to process service account credentials.",
						)
					}
					saKeyBytes = actualKey
				}
			}
		}

		opts = append(opts, option.WithCredentialsJSON(saKeyBytes))
		unwrappedSAKey = saKeyBytes // Store the unwrapped key for signed URL generation
	}

	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to create GCS client: %w", err),
			"Unable to connect to Google Cloud Storage. Please check your configuration.",
		)
	}

	logger, _ := logx.GetZapLogger(ctx)
	logger = logger.With(
		zap.String("storage", "gcs"),
		zap.String("project", config.ProjectID),
		zap.String("region", config.Region),
		zap.String("bucket", config.Bucket))

	return &gcsStorage{
		client:        client,
		projectID:     config.ProjectID,
		region:        config.Region,
		bucket:        config.Bucket,
		serviceAccKey: unwrappedSAKey, // Use unwrapped key for signed URL generation
		logger:        logger,
	}, nil
}

// UploadBase64File implements object.Storage.UploadBase64File
func (g *gcsStorage) UploadBase64File(ctx context.Context, bucket string, filePath string, base64Content string, fileMimeType string) error {
	decodedContent, err := base64.StdEncoding.DecodeString(base64Content)
	if err != nil {
		return fmt.Errorf("failed to decode base64 content: %w", err)
	}
	return g.uploadFile(ctx, bucket, filePath, decodedContent, fileMimeType)
}

// UploadFile implements object.Storage.UploadFile
func (g *gcsStorage) UploadFile(ctx context.Context, bucket string, filePath string, content []byte, fileMimeType string) error {
	return g.uploadFile(ctx, bucket, filePath, content, fileMimeType)
}

// uploadFile is an internal helper to upload raw bytes to GCS
func (g *gcsStorage) uploadFile(ctx context.Context, bucketName string, objectPath string, content []byte, mimeType string) error {
	if g.client == nil {
		return errorsx.AddMessage(
			fmt.Errorf("GCS client not initialized"),
			"GCS storage is not configured.",
		)
	}

	// Use provided bucket or default to configured bucket
	if bucketName == "" {
		bucketName = g.bucket
	}

	// Create bucket handle
	bucket := g.client.Bucket(bucketName)

	// Scale timeout with content size: 15 min base + 2 sec per MB.
	// A 307MB chunk needs ~10 resumable round-trips (32MB each); under GKE
	// egress pressure each can take 30-60s, so budget for ~0.5 MB/s worst case.
	contentSizeMB := len(content) / (1024 * 1024)
	timeout := 15*time.Minute + time.Duration(contentSizeMB)*2*time.Second
	uploadCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	g.logger.Info("Starting GCS upload",
		zap.String("bucket", bucketName),
		zap.String("path", objectPath),
		zap.Int("sizeMB", contentSizeMB),
		zap.Duration("timeout", timeout))

	obj := bucket.Object(objectPath)
	writer := obj.NewWriter(uploadCtx)
	writer.ContentType = mimeType
	writer.ChunkSize = 32 * 1024 * 1024 // 32 MB chunks reduce round-trips for large files
	writer.Metadata = map[string]string{
		"upload_time": time.Now().Format(time.RFC3339),
		"source":      "artifact-backend",
	}

	// Write content
	reader := bytes.NewReader(content)
	if _, err := io.Copy(writer, reader); err != nil {
		writer.Close()
		return errorsx.AddMessage(
			fmt.Errorf("failed to write to GCS: %w", err),
			"Unable to upload file to GCS. Please try again.",
		)
	}

	// Close writer to finalize upload
	if err := writer.Close(); err != nil {
		return errorsx.AddMessage(
			fmt.Errorf("failed to finalize GCS upload: %w", err),
			"Unable to complete file upload to GCS. Please try again.",
		)
	}

	g.logger.Info("File uploaded to GCS successfully",
		zap.String("bucket", bucketName),
		zap.String("path", objectPath))

	return nil
}

// DeleteFile implements object.Storage.DeleteFile
func (g *gcsStorage) DeleteFile(ctx context.Context, bucket string, filePath string) error {
	if g.client == nil {
		return errorsx.AddMessage(
			fmt.Errorf("GCS client not initialized"),
			"GCS storage is not configured.",
		)
	}

	// Use provided bucket or default
	if bucket == "" {
		bucket = g.bucket
	}

	obj := g.client.Bucket(bucket).Object(filePath)
	if err := obj.Delete(ctx); err != nil {
		// Don't return error if object doesn't exist (already deleted)
		if err == storage.ErrObjectNotExist {
			g.logger.Debug("Object already deleted", zap.String("path", filePath))
			return nil
		}
		return errorsx.AddMessage(
			fmt.Errorf("failed to delete GCS object: %w", err),
			"Unable to delete file from GCS.",
		)
	}

	g.logger.Info("File deleted from GCS successfully", zap.String("path", filePath))
	return nil
}

// GetFile implements object.Storage.GetFile
func (g *gcsStorage) GetFile(ctx context.Context, bucket string, filePath string) ([]byte, error) {
	if g.client == nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("GCS client not initialized"),
			"GCS storage is not configured.",
		)
	}

	// Use provided bucket or default
	if bucket == "" {
		bucket = g.bucket
	}

	obj := g.client.Bucket(bucket).Object(filePath)
	reader, err := obj.NewReader(ctx)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to read GCS object: %w", err),
			"Unable to read file from GCS.",
		)
	}
	defer reader.Close()

	content, err := io.ReadAll(reader)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to read GCS object content: %w", err),
			"Failed to read file content from GCS.",
		)
	}

	return content, nil
}

// GetFileMetadata implements object.Storage.GetFileMetadata
func (g *gcsStorage) GetFileMetadata(ctx context.Context, bucket string, filePath string) (*minio.ObjectInfo, error) {
	if g.client == nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("GCS client not initialized"),
			"GCS storage is not configured.",
		)
	}

	// Use provided bucket or default
	if bucket == "" {
		bucket = g.bucket
	}

	obj := g.client.Bucket(bucket).Object(filePath)
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to get GCS object attributes: %w", err),
			"Unable to get file metadata from GCS.",
		)
	}

	// Convert GCS attributes to MinIO ObjectInfo for interface compatibility
	return &minio.ObjectInfo{
		Key:          filePath,
		Size:         attrs.Size,
		LastModified: attrs.Updated,
		ContentType:  attrs.ContentType,
	}, nil
}

// ListFilePathsWithPrefix implements object.Storage.ListFilePathsWithPrefix
func (g *gcsStorage) ListFilePathsWithPrefix(ctx context.Context, bucket string, prefix string) ([]string, error) {
	if g.client == nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("GCS client not initialized"),
			"GCS storage is not configured.",
		)
	}

	// Use provided bucket or default
	if bucket == "" {
		bucket = g.bucket
	}

	var filePaths []string
	it := g.client.Bucket(bucket).Objects(ctx, &storage.Query{
		Prefix: prefix,
	})

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, errorsx.AddMessage(
				fmt.Errorf("failed to list GCS objects: %w", err),
				"Unable to list files from GCS.",
			)
		}
		filePaths = append(filePaths, attrs.Name)
	}

	return filePaths, nil
}

// SaveConvertedFile implements object.Storage.SaveConvertedFile
func (g *gcsStorage) SaveConvertedFile(ctx context.Context, kbUID types.KBUIDType, fileUID types.FileUIDType, convertedFileUID types.ConvertedFileUIDType, fileExt string, content []byte) (string, error) {
	// Build path: converted-file/{kbUID}/{fileUID}/{convertedFileUID}.{ext}
	objectPath := path.Join(
		ConvertedFileDir,
		kbUID.String(),
		fileUID.String(),
		fmt.Sprintf("%s%s", convertedFileUID.String(), fileExt),
	)

	// Upload file
	if err := g.uploadFile(ctx, g.bucket, objectPath, content, "application/octet-stream"); err != nil {
		return "", err
	}

	return objectPath, nil
}

// ListKnowledgeBaseFilePaths implements object.Storage.ListKnowledgeBaseFilePaths
func (g *gcsStorage) ListKnowledgeBaseFilePaths(ctx context.Context, kbUID types.KBUIDType) ([]string, error) {
	// List all files under KB prefix
	prefix := kbUID.String() + "/"
	return g.ListFilePathsWithPrefix(ctx, g.bucket, prefix)
}

// ListConvertedFilesByFileUID implements object.Storage.ListConvertedFilesByFileUID
func (g *gcsStorage) ListConvertedFilesByFileUID(ctx context.Context, kbUID types.KBUIDType, fileUID types.FileUIDType) ([]string, error) {
	// List converted files: converted-file/{kbUID}/{fileUID}/
	prefix := path.Join(ConvertedFileDir, kbUID.String(), fileUID.String()) + "/"
	return g.ListFilePathsWithPrefix(ctx, g.bucket, prefix)
}

// ListTextChunksByFileUID implements object.Storage.ListTextChunksByFileUID
func (g *gcsStorage) ListTextChunksByFileUID(ctx context.Context, kbUID types.KBUIDType, fileUID types.FileUIDType) ([]string, error) {
	// List chunks: chunk/{kbUID}/{fileUID}/
	prefix := path.Join(ChunkDir, kbUID.String(), fileUID.String()) + "/"
	return g.ListFilePathsWithPrefix(ctx, g.bucket, prefix)
}

// GetPresignedURLForUpload implements object.Storage.GetPresignedURLForUpload
func (g *gcsStorage) GetPresignedURLForUpload(ctx context.Context, namespaceUUID types.NamespaceUIDType, objectUUID types.ObjectUIDType, filename string, urlExpiration time.Duration) (*url.URL, error) {
	// Generate upload path
	objectPath := path.Join(BlobBucketName, namespaceUUID.String(), objectUUID.String(), filename)

	// Generate signed URL for upload (PUT)
	signedURL, err := g.generateSignedURL(ctx, g.bucket, objectPath, "PUT", urlExpiration)
	if err != nil {
		return nil, err
	}

	parsedURL, err := url.Parse(signedURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse signed URL: %w", err)
	}

	return parsedURL, nil
}

// GetPresignedURLForDownload implements object.Storage.GetPresignedURLForDownload
func (g *gcsStorage) GetPresignedURLForDownload(ctx context.Context, bucket string, objectPath string, filename string, contentType string, expiration time.Duration) (*url.URL, error) {
	// Use provided bucket or default
	if bucket == "" {
		bucket = g.bucket
	}

	// Generate signed URL for download (GET) with response headers
	// GCS supports response-content-disposition and response-content-type query parameters
	queryParams := url.Values{}
	if filename != "" {
		// Set Content-Disposition to ensure proper filename in browser downloads
		queryParams.Set("response-content-disposition", fmt.Sprintf(`inline; filename="%s"`, filename))
	}
	if contentType != "" {
		// Set Content-Type to ensure proper MIME type handling
		queryParams.Set("response-content-type", contentType)
	}

	signedURL, err := g.generateSignedURLWithParams(ctx, bucket, objectPath, "GET", expiration, queryParams)
	if err != nil {
		return nil, err
	}

	parsedURL, err := url.Parse(signedURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse signed URL: %w", err)
	}

	return parsedURL, nil
}

// generateSignedURL generates a signed URL for GCS without query parameters
func (g *gcsStorage) generateSignedURL(ctx context.Context, bucketName string, objectPath string, method string, expiration time.Duration) (string, error) {
	return g.generateSignedURLWithParams(ctx, bucketName, objectPath, method, expiration, nil)
}

// generateSignedURLWithParams generates a signed URL for GCS with optional query parameters
func (g *gcsStorage) generateSignedURLWithParams(ctx context.Context, bucketName string, objectPath string, method string, expiration time.Duration, queryParams url.Values) (string, error) {
	// Extract service account email from credentials
	var saData struct {
		ClientEmail string `json:"client_email"`
		PrivateKey  string `json:"private_key"`
	}
	if err := json.Unmarshal(g.serviceAccKey, &saData); err != nil {
		return "", errorsx.AddMessage(
			fmt.Errorf("failed to parse service account key: %w", err),
			"Invalid service account configuration.",
		)
	}

	// Generate signed URL using storage package
	opts := &storage.SignedURLOptions{
		Scheme:         storage.SigningSchemeV4,
		Method:         method,
		Expires:        time.Now().Add(expiration),
		GoogleAccessID: saData.ClientEmail,
		PrivateKey:     []byte(saData.PrivateKey),
	}

	// Add query parameters for response header overrides (e.g., Content-Disposition, Content-Type)
	// These parameters are included in the signature for security
	if len(queryParams) > 0 {
		opts.QueryParameters = queryParams
	}

	signedURL, err := storage.SignedURL(bucketName, objectPath, opts)
	if err != nil {
		return "", errorsx.AddMessage(
			fmt.Errorf("failed to generate signed URL: %w", err),
			"Unable to generate temporary access URL for file.",
		)
	}

	return signedURL, nil
}



// GetFileReader returns a streaming reader for a GCS object and its size.
func (g *gcsStorage) GetFileReader(ctx context.Context, bucket string, filePath string) (io.ReadCloser, int64, error) {
	if g.client == nil {
		return nil, 0, fmt.Errorf("GCS client not initialized")
	}
	if bucket == "" {
		bucket = g.bucket
	}

	obj := g.client.Bucket(bucket).Object(filePath)
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("getting GCS object attrs: %w", err)
	}

	reader, err := obj.NewReader(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("opening GCS object reader: %w", err)
	}

	return reader, attrs.Size, nil
}

// UploadFromReader streams data from a reader to GCS without buffering
// the entire content in memory. Uses 32 MB resumable-upload chunks.
func (g *gcsStorage) UploadFromReader(ctx context.Context, bucket string, filePath string, reader io.Reader, size int64, mimeType string) error {
	if g.client == nil {
		return fmt.Errorf("GCS client not initialized")
	}
	if bucket == "" {
		bucket = g.bucket
	}

	sizeMB := size / (1024 * 1024)
	timeout := 15*time.Minute + time.Duration(sizeMB)*2*time.Second
	uploadCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	g.logger.Info("Starting streaming GCS upload",
		zap.String("bucket", bucket),
		zap.String("path", filePath),
		zap.Int64("sizeMB", sizeMB),
		zap.Duration("timeout", timeout))

	obj := g.client.Bucket(bucket).Object(filePath)
	writer := obj.NewWriter(uploadCtx)
	writer.ContentType = mimeType
	writer.ChunkSize = 32 * 1024 * 1024
	writer.Metadata = map[string]string{
		"upload_time": time.Now().Format(time.RFC3339),
		"source":      "artifact-backend",
	}

	if _, err := io.Copy(writer, reader); err != nil {
		writer.Close()
		return fmt.Errorf("streaming to GCS: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("finalizing GCS streaming upload: %w", err)
	}

	g.logger.Info("Streaming GCS upload complete",
		zap.String("bucket", bucket),
		zap.String("path", filePath))

	return nil
}

// GetFileReaderRange is not meaningful for a GCS destination in this
// codebase: GCS here is write-only (content/summary outputs, VertexAI
// caches), never the source side of a stream. The implementation exists
// to satisfy the Storage interface.
func (g *gcsStorage) GetFileReaderRange(_ context.Context, _ string, _ string, _ int64) (io.ReadCloser, error) {
	return nil, fmt.Errorf("gcsStorage.GetFileReaderRange: not implemented; GCS is write-only in this codebase")
}

// CopyObject copies an object from one location to another in GCS.
func (g *gcsStorage) CopyObject(ctx context.Context, srcBucket, srcPath, dstBucket, dstPath string) error {
	src := g.client.Bucket(srcBucket).Object(srcPath)
	dst := g.client.Bucket(dstBucket).Object(dstPath)
	if _, err := dst.CopierFrom(src).Run(ctx); err != nil {
		return fmt.Errorf("copying GCS object from %s/%s to %s/%s: %w", srcBucket, srcPath, dstBucket, dstPath, err)
	}
	return nil
}

// GetBucket returns the default GCS bucket name
func (g *gcsStorage) GetBucket() string {
	return g.bucket
}

// StreamCopy streams a known-size object to GCS via a caller-supplied
// openAt factory, owning one resumable upload writer across the whole call
// and transparently recovering from mid-stream MinIO body truncations.
//
// The outer shape is:
//
//  1. Create a single GCS resumable upload writer with a timeout that
//     accommodates the full payload plus maxStreamResumes rounds of
//     rewind-and-retry.
//  2. Loop: openAt(copied) → io.Copy into the writer via a progressWriter
//     that increments the cumulative counter and fires progressFn on
//     every write (activities use this for Temporal heartbeats).
//  3. If io.Copy ended with io.ErrUnexpectedEOF — or ended cleanly but
//     short of size (the silent-truncation failure mode confirmed in
//     minio-go: HTTP body EOF masked as nil) — treat it as a resumable
//     truncation, close the stale reader, sleep streamResumeGap, and
//     re-open at the new offset. After maxStreamResumes, give up and let
//     the outer retry handle it.
//  4. Any other error → abort (do not finalise the writer) and return.
//  5. On success (copied == size with err == nil), writer.Close() sends
//     the GCS finalize request.
//
// The resume path depends on the MinIO object being immutable for the
// duration of the copy — a property the artifact-backend pipeline
// already provides (standardized / KB file paths are write-once). If
// that invariant is ever relaxed, the follow-up
// ARTIFACT-INV-GCS-STREAM-RESUME-ETAG hardening (capture attrs.Etag at
// first open, re-verify on each resume) becomes required.
func (g *gcsStorage) StreamCopy(ctx context.Context, bucket string, filePath string, openAt func(offset int64) (io.ReadCloser, error), size int64, mimeType string, progressFn func(copied int64)) error {
	if g.client == nil {
		return fmt.Errorf("GCS client not initialized")
	}
	if bucket == "" {
		bucket = g.bucket
	}
	if size < 0 {
		return fmt.Errorf("gcsStorage.StreamCopy: size must be >= 0 (got %d); unknown-size uploads are not resumable", size)
	}
	if openAt == nil {
		return fmt.Errorf("gcsStorage.StreamCopy: openAt factory is required")
	}

	sizeMB := size / (1024 * 1024)

	// Timeout budget has to cover: (a) the baseline 15m + 2s/MB that
	// UploadFromReader uses, plus (b) maxStreamResumes × (streamResumeGap
	// + 5m) to accommodate re-reads of potentially the full remaining
	// payload on each resume. Overshooting is cheap; undershooting
	// truncates a legitimate resume mid-way and masks the fix.
	timeout := 15*time.Minute +
		time.Duration(sizeMB)*2*time.Second +
		maxStreamResumes*(streamResumeGap+5*time.Minute)
	uploadCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	log := g.logger.With(
		zap.String("bucket", bucket),
		zap.String("path", filePath),
		zap.Int64("sizeMB", sizeMB),
		zap.Duration("timeout", timeout),
	)
	log.Info("Starting streaming GCS upload with resume",
		zap.Int("maxStreamResumes", maxStreamResumes),
		zap.Duration("streamResumeGap", streamResumeGap))

	obj := g.client.Bucket(bucket).Object(filePath)
	writer := obj.NewWriter(uploadCtx)
	writer.ContentType = mimeType
	writer.ChunkSize = gcsResumableChunkSize
	writer.Metadata = map[string]string{
		"upload_time": time.Now().Format(time.RFC3339),
		"source":      "artifact-backend",
	}

	// Defer-close protects against early returns that forget to abort
	// the writer. On the happy path we call writer.Close() explicitly
	// below and clear `writer` so this deferred close is a no-op.
	var writerClosed bool
	defer func() {
		if !writerClosed {
			// Best-effort abort. The GCS client elides resumable-upload
			// finalisation when the ctx is cancelled (via `defer cancel()`
			// above), so partial bytes never surface as a committed object.
			_ = writer.Close()
		}
	}()

	copied, resumes, err := streamCopyWithResume(
		uploadCtx, writer, openAt, size, log, progressFn,
		maxStreamResumes, streamResumeGap,
	)
	if err != nil {
		return err
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("finalizing GCS streaming upload: %w", err)
	}
	writerClosed = true

	log.Info("Streaming GCS upload complete",
		zap.Int64("bytes", copied),
		zap.Int("resumes", resumes))

	return nil
}

// streamCopyWithResume is the pure in-activity resume loop that
// StreamCopy wires its GCS writer into. Split out from StreamCopy so
// unit tests can exercise the loop end-to-end without standing up a
// real GCS client — any `io.Writer` that counts bytes (a bytes.Buffer
// suffices) is enough to pin the contract.
//
// Returns the cumulative bytes copied, the number of resumes actually
// taken, and the terminal error (nil on success). The caller owns the
// writer lifecycle: this helper never calls writer.Close() — even on
// error paths — so StreamCopy can decide whether to finalise or abort.
//
// The loop invariants are:
//   - Every openAt(offset) call gets the current `copied` value, and
//     the reader it returns is always closed before the next iteration.
//   - Exit path 1 (success): copyErr == nil AND copied == size.
//   - Exit path 2 (silent truncation promoted): copyErr == nil AND
//     copied < size — treated identically to ErrUnexpectedEOF so the
//     minio-go "body EOF masked as nil" failure mode doesn't wedge
//     uploads at sub-full copies.
//   - Exit path 3 (resume): ErrUnexpectedEOF (real or promoted) and
//     resumes ≤ maxResumes — sleep resumeGap (cancellable) and retry.
//   - Exit path 4 (budget exhausted): more than maxResumes truncations
//     — return the ErrUnexpectedEOF wrapped with the copied/size tally.
//   - Exit path 5 (non-EOF error): return the error wrapped, let the
//     caller decide retry strategy.
func streamCopyWithResume(
	ctx context.Context,
	writer io.Writer,
	openAt func(offset int64) (io.ReadCloser, error),
	size int64,
	log *zap.Logger,
	progressFn func(copied int64),
	maxResumes int,
	resumeGap time.Duration,
) (int64, int, error) {
	copied := int64(0)
	resumes := 0
	progress := &progressWriter{w: writer, counter: &copied, fn: progressFn}

	for {
		reader, err := openAt(copied)
		if err != nil {
			return copied, resumes, fmt.Errorf("opening source at offset %d: %w", copied, err)
		}

		segmentStart := copied
		_, copyErr := io.Copy(progress, reader)
		_ = reader.Close()
		segmentCopied := copied - segmentStart

		if copyErr == nil && copied == size {
			return copied, resumes, nil
		}

		if copyErr == nil && copied < size {
			copyErr = io.ErrUnexpectedEOF
		}

		if errors.Is(copyErr, io.ErrUnexpectedEOF) {
			resumes++
			if resumes > maxResumes {
				return copied, resumes, fmt.Errorf("streaming to GCS: %w after %d resumes (copied %d of %d bytes)",
					io.ErrUnexpectedEOF, maxResumes, copied, size)
			}
			if log != nil {
				log.Warn("Resuming MinIO stream after truncation",
					zap.Int64("copied", copied),
					zap.Int64("size", size),
					zap.Int64("segmentCopied", segmentCopied),
					zap.Int("resume", resumes))
			}
			// Cancellable sleep so a context cancellation here doesn't
			// waste the full gap before we notice.
			select {
			case <-ctx.Done():
				return copied, resumes, fmt.Errorf("streaming to GCS: context cancelled during resume backoff: %w", ctx.Err())
			case <-time.After(resumeGap):
			}
			continue
		}

		return copied, resumes, fmt.Errorf("streaming to GCS: %w", copyErr)
	}
}

// progressWriter wraps the GCS resumable Writer so StreamCopy can
// accumulate the cumulative byte count across resume iterations and fire
// the caller-provided progressFn on every inner Write — frequent enough
// to keep Temporal heartbeats alive on any reasonable heartbeat timeout.
// The counter pointer is shared with the outer StreamCopy loop so the
// truncation-detection logic (err==nil && copied<size) can read the
// up-to-date total without racing.
type progressWriter struct {
	w       io.Writer
	counter *int64
	fn      func(copied int64)
}

func (p *progressWriter) Write(b []byte) (int, error) {
	n, err := p.w.Write(b)
	if n > 0 {
		*p.counter += int64(n)
		if p.fn != nil {
			p.fn(*p.counter)
		}
	}
	return n, err
}
