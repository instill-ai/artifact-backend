package object

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"path"
	"strings"
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
	// Decode the base64 content
	decodedContent, err := base64.StdEncoding.DecodeString(base64Content)
	if err != nil {
		return fmt.Errorf("failed to decode base64 content: %w", err)
	}

	// Upload using raw bytes
	return g.uploadFile(ctx, bucket, filePath, decodedContent, fileMimeType)
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

	// Create object writer with context timeout
	uploadCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	obj := bucket.Object(objectPath)
	writer := obj.NewWriter(uploadCtx)
	writer.ContentType = mimeType
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

// GetGCSURI returns the gs://bucket/path URI for a given object path
// This is useful for VertexAI which requires gs:// URIs
func (g *gcsStorage) GetGCSURI(objectPath string) string {
	return fmt.Sprintf("gs://%s/%s", g.bucket, objectPath)
}

// ParseGCSURI parses a gs://bucket/path URI into bucket and object path components
func ParseGCSURI(gsURI string) (bucket string, objectPath string, err error) {
	if !strings.HasPrefix(gsURI, "gs://") {
		return "", "", fmt.Errorf("URI must start with gs://")
	}

	// Remove gs:// prefix
	remaining := gsURI[5:]

	// Find first slash to separate bucket from path
	slashIndex := strings.Index(remaining, "/")
	if slashIndex == -1 {
		// No path, just bucket
		return remaining, "", nil
	}

	bucket = remaining[:slashIndex]
	objectPath = remaining[slashIndex+1:]

	return bucket, objectPath, nil
}

// GetBucket returns the default GCS bucket name
func (g *gcsStorage) GetBucket() string {
	return g.bucket
}
