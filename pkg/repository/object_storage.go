package repository

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"github.com/instill-ai/artifact-backend/pkg/types"
	"github.com/minio/minio-go/v7"
	"go.uber.org/zap"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/x/resource"

	logx "github.com/instill-ai/x/log"
	miniox "github.com/instill-ai/x/minio"
)

// ObjectStorage defines the interface for object storage operations
type ObjectStorage interface {
	// Basic file operations
	UploadBase64File(ctx context.Context, bucket string, filePath string, base64Content string, fileMimeType string) error
	DeleteFile(ctx context.Context, bucket string, filePath string) error
	GetFile(ctx context.Context, bucket string, filePath string) ([]byte, error)
	GetFileMetadata(ctx context.Context, bucket string, filePath string) (*minio.ObjectInfo, error)
	ListFilePathsWithPrefix(ctx context.Context, bucket string, prefix string) ([]string, error)

	// Knowledge base operations
	SaveConvertedFile(ctx context.Context, kbUID types.KBUIDType, fileUID types.FileUIDType, convertedFileUID types.ConvertedFileUIDType, fileExt string, content []byte) (path string, _ error)
	ListKnowledgeBaseFilePaths(ctx context.Context, kbUID types.KBUIDType) ([]string, error)
	ListConvertedFilesByFileUID(ctx context.Context, kbUID types.KBUIDType, fileUID types.FileUIDType) ([]string, error)
	ListTextChunksByFileUID(ctx context.Context, kbUID types.KBUIDType, fileUID types.FileUIDType) ([]string, error)

	// Object (blob) operations
	GetPresignedURLForUpload(ctx context.Context, namespaceUUID types.NamespaceUIDType, objectUUID types.ObjectUIDType, filename string, urlExpiration time.Duration) (*url.URL, error)
	// GetPresignedURLForDownload generates a presigned URL for downloading any object with proper Content-Disposition and Content-Type headers.
	// This ensures proper browser download behavior with correct filename and MIME type handling.
	GetPresignedURLForDownload(ctx context.Context, bucket string, objectPath string, filename string, contentType string, expiration time.Duration) (*url.URL, error)
}

// MinIO implementation constants
const (
	// BlobBucketName is used to interact with data upload by clients via
	// presigned upload URLs.
	BlobBucketName = "core-blob"

	convertedFileDir = "converted-file"
	chunkDir         = "chunk"

	// presignAgent is a hard-coded value for the presigned URLs. Since the client
	// that requests the presigned URL (console) and the one that interacts with
	// MinIO (api-gateway) are different, the first step to audit the MinIO clients
	// is setting this value as a constant.
	presignAgent = "artifact-backend-presign"
)

type minioClient struct {
	client *minio.Client
	logger *zap.Logger
}

// NewMinioObjectStorage creates a new ObjectStorage implementation using MinIO.
func NewMinioObjectStorage(ctx context.Context, params miniox.ClientParams) (ObjectStorage, error) {
	params.Logger = params.Logger.With(
		zap.String("host:port", params.Config.Host+":"+params.Config.Port),
		zap.String("user", params.Config.User),
	)

	// We'll initialise a client and create the knowledge base bucket. Then,
	// we'll create the blob bucket.
	kbParams := params
	xClient, err := miniox.NewMinIOClientAndInitBucket(ctx, kbParams)
	if err != nil {
		return nil, fmt.Errorf("connecting to MinIO: %w", err)
	}

	client := xClient.Client()

	// Create blob bucket if it doesn't exist
	{
		log := params.Logger.With(zap.String("bucket", BlobBucketName))

		exists, err := client.BucketExists(ctx, BlobBucketName)
		if err != nil {
			return nil, fmt.Errorf("checking bucket existence: %w", err)
		}

		if !exists {
			if err := client.MakeBucket(ctx, BlobBucketName, minio.MakeBucketOptions{
				Region: miniox.Location,
			}); err != nil {
				return nil, fmt.Errorf("creating bucket: %w", err)
			}
			log.Info("Successfully created bucket")
		} else {
			log.Info("Bucket already exists")
		}
	}

	return &minioClient{
		client: client,
		logger: params.Logger,
	}, nil
}

// Basic file operations

func (m *minioClient) UploadBase64File(ctx context.Context, bucket string, filePathName string, base64Content string, fileMimeType string) error {
	// Decode the base64 content
	decodedContent, err := base64.StdEncoding.DecodeString(base64Content)
	if err != nil {
		return err
	}
	// Upload the content to MinIO
	size := int64(len(decodedContent))
	// Retry loop with fresh reader on each attempt
	for i := 0; i < 3; i++ {
		// Create a fresh reader for each attempt (readers can only be read once)
		contentReader := strings.NewReader(string(decodedContent))
		_, err = m.client.PutObject(
			ctx,
			bucket,
			filePathName,
			contentReader,
			size,
			minio.PutObjectOptions{
				ContentType:  fileMimeType,
				UserMetadata: map[string]string{miniox.MinIOHeaderUserUID: m.authenticatedUser(ctx)},
			},
		)
		if err == nil {
			break
		}
		m.logger.Error("Failed to upload file to MinIO, retrying...", zap.String("attempt", fmt.Sprintf("%d", i+1)), zap.Error(err))
		time.Sleep(1 * time.Second)
	}
	if err != nil {
		m.logger.Error("Failed to upload file to MinIO after retries", zap.Error(err))
		return err
	}
	return nil
}

func (m *minioClient) DeleteFile(ctx context.Context, bucket string, filePathName string) error {
	// Delete the file from MinIO
	for attempt := 1; attempt <= 3; attempt++ {
		err := m.client.RemoveObject(ctx, bucket, filePathName, minio.RemoveObjectOptions{})
		if err == nil {
			return nil
		}
		m.logger.Error("Failed to delete file from MinIO, retrying...", zap.String("filePathName", filePathName), zap.Int("attempt", attempt), zap.Error(err))
		time.Sleep(time.Duration(attempt) * time.Second)
	}
	return fmt.Errorf("failed to delete file from MinIO after 3 attempts")
}

func (m *minioClient) ListFilePathsWithPrefix(ctx context.Context, bucket string, prefix string) ([]string, error) {
	opts := minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	}
	opts.Set(miniox.MinIOHeaderUserUID, m.authenticatedUser(ctx))
	objectCh := m.client.ListObjects(ctx, bucket, opts)

	var filePaths []string
	for object := range objectCh {
		if object.Err != nil {
			m.logger.Error("Failed to list object from MinIO", zap.Error(object.Err))
			return nil, fmt.Errorf("failed to list objects: %w", object.Err)
		}
		filePaths = append(filePaths, object.Key)
	}

	return filePaths, nil
}

func (m *minioClient) GetFile(ctx context.Context, bucketName string, objectName string) ([]byte, error) {
	var object *minio.Object
	var err error
	for attempt := 1; attempt <= 3; attempt++ {
		opts := minio.GetObjectOptions{}
		opts.Set(miniox.MinIOHeaderUserUID, m.authenticatedUser(ctx))
		object, err = m.client.GetObject(ctx, bucketName, objectName, opts)
		if err == nil {
			break
		}
		m.logger.Error("Failed to get file from MinIO, retrying...", zap.String("filePathName", objectName), zap.Int("attempt", attempt), zap.Error(err))
		time.Sleep(time.Duration(attempt) * time.Second)
	}
	if err != nil {
		m.logger.Error("Failed to get file from MinIO after 3 attempts", zap.String("filePathName", objectName), zap.Error(err))
		return nil, err
	}
	defer object.Close()

	// Read the object's content
	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(object)
	if err != nil {
		m.logger.Error("Failed to read file from MinIO", zap.Error(err))
		return nil, err
	}

	return buf.Bytes(), nil
}

func (m *minioClient) GetFileMetadata(ctx context.Context, bucket string, filePathName string) (*minio.ObjectInfo, error) {
	object, err := m.client.StatObject(ctx, bucket, filePathName, minio.StatObjectOptions{})
	if err != nil {
		return nil, err
	}
	return &object, nil
}

// Knowledge base operations

// SaveConvertedFile saves the converted file to MinIO
func (m *minioClient) SaveConvertedFile(ctx context.Context, kbUID types.KBUIDType, fileUID types.FileUIDType, convertedFileUID types.ConvertedFileUIDType, fileExt string, content []byte) (string, error) {
	filename := convertedFileUID.String() + "." + fileExt
	path := filepath.Join(convertedFileBasePath(kbUID, fileUID), filename)

	mimeType := "application/octet-stream"
	if fileExt == "md" {
		mimeType = "text/markdown"
	}

	err := m.UploadBase64File(ctx, config.Config.Minio.BucketName, path, base64.StdEncoding.EncodeToString(content), mimeType)
	if err != nil {
		return "", err
	}

	return path, nil
}

// ListKnowledgeBaseFilePaths lists the file paths for a knowledge base
func (m *minioClient) ListKnowledgeBaseFilePaths(ctx context.Context, kbUID types.KBUIDType) ([]string, error) {
	bucket := config.Config.Minio.BucketName
	legacyPrefix := kbUID.String()

	paths1, err := m.ListFilePathsWithPrefix(ctx, bucket, "kb-"+legacyPrefix)
	if err != nil {
		return nil, fmt.Errorf("listing knowledge base files: %w", err)
	}

	paths2, err := m.ListFilePathsWithPrefix(ctx, bucket, legacyPrefix)
	if err != nil {
		return nil, fmt.Errorf("listing legacy knowledge base files: %w", err)
	}

	return append(paths1, paths2...), nil
}

// ListConvertedFilesByFileUID lists the converted file paths for a file
func (m *minioClient) ListConvertedFilesByFileUID(ctx context.Context, kbUID types.KBUIDType, fileUID types.FileUIDType) ([]string, error) {
	return m.ListFilePathsWithPrefix(ctx, config.Config.Minio.BucketName, convertedFileBasePath(kbUID, fileUID))
}

// ListTextChunksByFileUID lists the text chunk paths for a file
func (m *minioClient) ListTextChunksByFileUID(ctx context.Context, kbUID types.KBUIDType, fileUID types.FileUIDType) ([]string, error) {
	return m.ListFilePathsWithPrefix(ctx, config.Config.Minio.BucketName, chunkBasePath(kbUID, fileUID))
}

// Object (blob) operations

// GetPresignedURLForUpload generates a presigned URL for uploading an object to MinIO
func (m *minioClient) GetPresignedURLForUpload(ctx context.Context, namespaceUUID types.NamespaceUIDType, objectUUID types.ObjectUIDType, filename string, expiration time.Duration) (*url.URL, error) {
	logger, err := logx.GetZapLogger(ctx)
	if err != nil {
		return nil, err
	}
	// check if the expiration is within the range of 1sec to 7 days.
	if expiration > time.Hour*24*7 {
		return nil, errors.New("expiration time must be within 1sec to 7 days")
	}

	// When using the presigned URL for uploading, we can set the
	// x-amz-meta-original-filename header to be the original filename of the
	// object.
	reqParams := url.Values{}
	reqParams.Set("x-amz-meta-original-filename", filename)
	// Get presigned URL for uploading object.
	presignedURL, err := m.client.PresignHeader(
		ctx,
		http.MethodPut,
		BlobBucketName,
		GetBlobObjectPath(namespaceUUID, objectUUID),
		expiration,
		reqParams,
		m.presignHeaders(),
	)
	if err != nil {
		logger.Error("Failed to make presigned URL for upload", zap.Error(err))
		return nil, err
	}

	return presignedURL, nil
}

// GetPresignedURLForDownload generates a presigned URL for downloading any object from MinIO
// with proper Content-Disposition and Content-Type headers for browser download behavior.
// This single method handles all download URL generation needs: original files, converted files, summaries, PDFs, etc.
func (m *minioClient) GetPresignedURLForDownload(ctx context.Context, bucket string, objectPath string, filename string, contentType string, expiration time.Duration) (*url.URL, error) {
	logger, err := logx.GetZapLogger(ctx)
	if err != nil {
		return nil, err
	}

	// Validate expiration is within reasonable range (1sec to 7 days)
	if expiration > time.Hour*24*7 {
		return nil, errors.New("expiration time must be within 1sec to 7 days")
	}

	// Set response headers for proper download behavior
	reqParams := url.Values{}
	reqParams.Set("response-content-disposition", fmt.Sprintf(`inline; filename="%s"`, filename))
	reqParams.Set("response-content-type", contentType)

	// Generate presigned URL using PresignedGetObject (public download URL)
	// Unlike PresignHeader, this doesn't sign custom headers, creating a truly public URL
	// that works from any client (browser, curl, etc.) without special headers.
	// This is appropriate for downloads where we want broad accessibility.
	presignedURL, err := m.client.PresignedGetObject(
		ctx,
		bucket,
		objectPath,
		expiration,
		reqParams,
	)
	if err != nil {
		logger.Error("Failed to generate presigned URL",
			zap.String("bucket", bucket),
			zap.String("objectPath", objectPath),
			zap.String("filename", filename),
			zap.Error(err))
		return nil, err
	}

	return presignedURL, nil
}

// Helper functions

func (m *minioClient) authenticatedUser(ctx context.Context) string {
	_, userUID := resource.GetRequesterUIDAndUserUID(ctx)
	return userUID.String()
}

func (m *minioClient) presignHeaders() http.Header {
	h := http.Header{}
	h.Set("User-Agent", presignAgent)
	return h
}

func fileBasePath(kbUID, fileUID types.FileUIDType) string {
	return filepath.Join("kb-"+kbUID.String(), "file-"+fileUID.String())
}

func convertedFileBasePath(kbUID, fileUID types.FileUIDType) string {
	return filepath.Join(fileBasePath(kbUID, fileUID), convertedFileDir)
}

func chunkBasePath(kbUID, fileUID types.FileUIDType) string {
	return filepath.Join(fileBasePath(kbUID, fileUID), chunkDir)
}

// GetBlobObjectPath makes object path from objectUUID.
// Format: ns-{namespaceUUID}/obj-{objectUUID}
func GetBlobObjectPath(namespaceUUID types.NamespaceUIDType, objectUUID types.ObjectUIDType) string {
	return fmt.Sprintf("ns-%s/obj-%s", namespaceUUID.String(), objectUUID.String())
}

// BucketFromDestination infers the bucket from the file destination string.
// Since there's a runtime migration for the object-based flow, this is used to
// retrieve files that might not have been updated their destination yet.
// - Legacy paths with "uploaded-file": artifact bucket (pre-migration)
// - Blob objects (uploaded files): ns-{uuid}/obj-{uuid} → BlobBucketName (core-blob)
// - Artifact files (converted, chunks): kb-{uuid}/... → artifact bucket
func BucketFromDestination(destination string) string {
	// Legacy: paths with "uploaded-file" are in the artifact bucket (pre-migration)
	if strings.Contains(destination, "uploaded-file") {
		return config.Config.Minio.BucketName
	}

	// Blob objects use the ns-{uuid}/obj-{uuid} format
	if strings.HasPrefix(destination, "ns-") {
		return BlobBucketName
	}

	// All other paths (kb-{uuid}/..., legacy paths) use the artifact bucket
	return config.Config.Minio.BucketName
}
