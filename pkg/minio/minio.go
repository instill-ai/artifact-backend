package minio

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"go.uber.org/zap"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/x/resource"

	miniox "github.com/instill-ai/x/minio"
)

type MinioI interface {
	// uploadFile
	UploadBase64File(ctx context.Context, bucket string, filePath string, base64Content string, fileMimeType string) (err error)
	// deleteFile
	DeleteFile(ctx context.Context, bucket string, filePath string) (err error)
	// GetFile
	GetFile(ctx context.Context, bucket string, filePath string) ([]byte, error)
	// GetFileMetadata: get the metadata of the file
	GetFileMetadata(ctx context.Context, bucket string, filePath string) (*minio.ObjectInfo, error)
	// ListFilePathsWithPrefix: list all file paths with a given prefix
	ListFilePathsWithPrefix(ctx context.Context, bucket string, prefix string) ([]string, error)
	// KnowledgeBase
	KnowledgeBaseI
	// Object
	ObjectI
}

type Minio struct {
	client *minio.Client
	logger *zap.Logger
}

const (
	// BlobBucketName is used to interact with data upload by clients via
	// presigned upload URLs.
	BlobBucketName = "core-blob"
)

func NewMinioClientAndInitBucket(ctx context.Context, params miniox.ClientParams) (*Minio, error) {

	params.Logger = params.Logger.With(
		zap.String("host:port", params.Config.Host+":"+params.Config.Port),
		zap.String("user", params.Config.User),
	)

	// We'll initialise a client and create the knowledge base bucket. Then,
	// we'll create the blob bucket.
	// TODO we should have one client per bucket. And use x/minio's public
	// methods instead of the internal client.
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

	return &Minio{
		client: client,
		logger: params.Logger,
	}, nil
}

func (m *Minio) UploadBase64File(ctx context.Context, bucket string, filePathName string, base64Content string, fileMimeType string) (err error) {
	// Decode the base64 content
	decodedContent, err := base64.StdEncoding.DecodeString(base64Content)
	if err != nil {
		return err
	}
	// Convert the decoded content to an io.Reader
	contentReader := strings.NewReader(string(decodedContent))
	// Upload the content to MinIO
	size := int64(len(decodedContent))
	// Create the file path with folder structure
	for i := 0; i < 3; i++ {
		_,
			err = m.client.PutObject(
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

// delete the file from minio
func (m *Minio) DeleteFile(ctx context.Context, bucket string, filePathName string) (err error) {
	// Delete the file from MinIO
	for attempt := 1; attempt <= 3; attempt++ {
		err = m.client.RemoveObject(ctx, bucket, filePathName, minio.RemoveObjectOptions{})
		if err == nil {
			break
		}
		m.logger.Error("Failed to delete file from MinIO, retrying...", zap.String("filePathName", filePathName), zap.Int("attempt", attempt), zap.Error(err))
		time.Sleep(time.Duration(attempt) * time.Second)
	}
	if err != nil {
		m.logger.Error("Failed to delete file from MinIO", zap.Error(err))
		return err
	}
	return nil
}

func (m *Minio) ListFilePathsWithPrefix(ctx context.Context, bucket string, prefix string) ([]string, error) {
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

func (m *Minio) GetFile(ctx context.Context, bucket string, filePathName string) ([]byte, error) {
	var object *minio.Object
	var err error
	for attempt := 1; attempt <= 3; attempt++ {
		opts := minio.GetObjectOptions{}
		opts.Set(miniox.MinIOHeaderUserUID, m.authenticatedUser(ctx))
		object, err = m.client.GetObject(ctx, bucket, filePathName, opts)
		if err == nil {
			break
		}
		m.logger.Error("Failed to get file from MinIO, retrying...", zap.String("filePathName", filePathName), zap.Int("attempt", attempt), zap.Error(err))
		time.Sleep(time.Duration(attempt) * time.Second)
	}
	if err != nil {
		m.logger.Error("Failed to get file from MinIO after 3 attempts", zap.String("filePathName", filePathName), zap.Error(err))
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

func (m *Minio) GetFileMetadata(ctx context.Context, bucket string, filePathName string) (*minio.ObjectInfo, error) {
	object, err := m.client.StatObject(ctx, bucket, filePathName, minio.StatObjectOptions{})
	if err != nil {
		return nil, err
	}
	return &object, nil
}

// Depending on the request, this might yield an empty userUID value.
// TODO we should make the user UID an explicit param in the methods that need
// this information and make sure that the clients provide it.
func (m *Minio) authenticatedUser(ctx context.Context) string {
	_, userUID := resource.GetRequesterUIDAndUserUID(ctx)
	return userUID.String()
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
