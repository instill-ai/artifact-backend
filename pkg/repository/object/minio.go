package object

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

	"github.com/minio/minio-go/v7"
	"go.uber.org/zap"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/types"
	"github.com/instill-ai/x/resource"

	logx "github.com/instill-ai/x/log"
	miniox "github.com/instill-ai/x/minio"
)

const (
	// presignAgent is a hard-coded value for the presigned URLs
	presignAgent = "artifact-backend-presign"
)

type minioStorage struct {
	client *minio.Client
	logger *zap.Logger
}

// NewMinIOStorage creates a new object.Storage implementation using MinIO
func NewMinIOStorage(ctx context.Context, params miniox.ClientParams) (Storage, error) {
	params.Logger = params.Logger.With(
		zap.String("host:port", params.Config.Host+":"+params.Config.Port),
		zap.String("user", params.Config.User),
	)

	// Initialize client and create the knowledge base bucket
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

	return &minioStorage{
		client: client,
		logger: params.Logger,
	}, nil
}

// UploadBase64File implements object.Storage.UploadBase64File
func (m *minioStorage) UploadBase64File(ctx context.Context, bucket string, filePathName string, base64Content string, fileMimeType string) error {
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

// DeleteFile implements object.Storage.DeleteFile
func (m *minioStorage) DeleteFile(ctx context.Context, bucket string, filePathName string) error {
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

// GetFile implements object.Storage.GetFile
func (m *minioStorage) GetFile(ctx context.Context, bucketName string, objectName string) ([]byte, error) {
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

// GetFileMetadata implements object.Storage.GetFileMetadata
func (m *minioStorage) GetFileMetadata(ctx context.Context, bucket string, filePathName string) (*minio.ObjectInfo, error) {
	object, err := m.client.StatObject(ctx, bucket, filePathName, minio.StatObjectOptions{})
	if err != nil {
		return nil, err
	}
	return &object, nil
}

// ListFilePathsWithPrefix implements object.Storage.ListFilePathsWithPrefix
func (m *minioStorage) ListFilePathsWithPrefix(ctx context.Context, bucket string, prefix string) ([]string, error) {
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

// SaveConvertedFile implements object.Storage.SaveConvertedFile
func (m *minioStorage) SaveConvertedFile(ctx context.Context, kbUID types.KBUIDType, fileUID types.FileUIDType, convertedFileUID types.ConvertedFileUIDType, fileExt string, content []byte) (string, error) {
	filename := convertedFileUID.String() + "." + fileExt
	path := filepath.Join(convertedFileBasePath(kbUID, fileUID), filename)

	// Set appropriate MIME type based on file extension
	// Note: converted-file folder contains:
	// 1. Standard file types: pdf, png, ogg, mp4 (for VIEW_STANDARD_FILE_TYPE)
	// 2. Content/Summary markdown: md (for VIEW_CONTENT/VIEW_SUMMARY)
	mimeType := "application/octet-stream"
	switch fileExt {
	case "md":
		mimeType = "text/markdown"
	case "pdf":
		mimeType = "application/pdf"
	case "png":
		mimeType = "image/png"
	case "ogg":
		mimeType = "audio/ogg"
	case "mp4":
		mimeType = "video/mp4"
	default:
		// This should not happen - only md, pdf, png, ogg, mp4 should be saved to converted-file folder
		// If we reach here, there's a bug in the calling code
		m.logger.Warn("Unexpected file extension in SaveConvertedFile",
			zap.String("extension", fileExt),
			zap.String("path", path))
	}

	err := m.UploadBase64File(ctx, config.Config.Minio.BucketName, path, base64.StdEncoding.EncodeToString(content), mimeType)
	if err != nil {
		return "", err
	}

	return path, nil
}

// ListKnowledgeBaseFilePaths implements object.Storage.ListKnowledgeBaseFilePaths
func (m *minioStorage) ListKnowledgeBaseFilePaths(ctx context.Context, kbUID types.KBUIDType) ([]string, error) {
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

// ListConvertedFilesByFileUID implements object.Storage.ListConvertedFilesByFileUID
func (m *minioStorage) ListConvertedFilesByFileUID(ctx context.Context, kbUID types.KBUIDType, fileUID types.FileUIDType) ([]string, error) {
	return m.ListFilePathsWithPrefix(ctx, config.Config.Minio.BucketName, convertedFileBasePath(kbUID, fileUID))
}

// ListTextChunksByFileUID implements object.Storage.ListTextChunksByFileUID
func (m *minioStorage) ListTextChunksByFileUID(ctx context.Context, kbUID types.KBUIDType, fileUID types.FileUIDType) ([]string, error) {
	return m.ListFilePathsWithPrefix(ctx, config.Config.Minio.BucketName, chunkBasePath(kbUID, fileUID))
}

// GetPresignedURLForUpload implements object.Storage.GetPresignedURLForUpload
func (m *minioStorage) GetPresignedURLForUpload(ctx context.Context, namespaceUUID types.NamespaceUIDType, objectUUID types.ObjectUIDType, filename string, expiration time.Duration) (*url.URL, error) {
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

// GetPresignedURLForDownload implements Storage.GetPresignedURLForDownload
func (m *minioStorage) GetPresignedURLForDownload(ctx context.Context, bucket string, objectPath string, filename string, contentType string, expiration time.Duration) (*url.URL, error) {
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

func (m *minioStorage) authenticatedUser(ctx context.Context) string {
	_, userUID := resource.GetRequesterUIDAndUserUID(ctx)
	return userUID.String()
}

func (m *minioStorage) presignHeaders() http.Header {
	h := http.Header{}
	h.Set("User-Agent", presignAgent)
	return h
}

func fileBasePath(kbUID types.KBUIDType, fileUID types.FileUIDType) string {
	return filepath.Join("kb-"+kbUID.String(), "file-"+fileUID.String())
}

func convertedFileBasePath(kbUID types.KBUIDType, fileUID types.FileUIDType) string {
	return filepath.Join(fileBasePath(kbUID, fileUID), ConvertedFileDir)
}

func chunkBasePath(kbUID types.KBUIDType, fileUID types.FileUIDType) string {
	return filepath.Join(fileBasePath(kbUID, fileUID), ChunkDir)
}

// BucketFromDestination infers the bucket from the file destination string
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

// GetBucket returns the default MinIO bucket name
func (m *minioStorage) GetBucket() string {
	return config.Config.Minio.BucketName
}
