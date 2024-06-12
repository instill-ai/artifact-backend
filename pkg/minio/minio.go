package minio

import (
	"context"
	"encoding/base64"
	"strings"

	"github.com/instill-ai/artifact-backend/config"
	log "github.com/instill-ai/artifact-backend/pkg/logger"
	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	"github.com/minio/minio-go"
	"go.uber.org/zap"
)

type MinioI interface {
	GetClient() *minio.Client
	// uploadFile
	UploadBase64File(ctx context.Context, file_path_name string, base64_content string, file_type int) (err error)
	// deleteFile
	DeleteFile(ctx context.Context, file_path_name string) (err error)
}

type Minio struct {
	client *minio.Client
	bucket string
}

func NewMinioClientAndInitBucket() (*Minio, error) {
	cfg := config.Config.Minio
	log, err := log.GetZapLogger(context.Background())
	if err != nil {
		return nil, err
	}
	client, err := minio.New(cfg.Host+":"+cfg.Port, cfg.RootUser, cfg.RootUser, false)
	if err != nil {
		return nil, err
	}
	err = client.MakeBucket(cfg.BucketName, "us-east-1")
	if err != nil {
		// Check if the bucket already exists
		exists, errBucketExists := client.BucketExists(cfg.BucketName)
		if errBucketExists == nil && exists {
			log.Info("Bucket already exists", zap.String("bucket", cfg.BucketName), zap.Error(err))
		} else {
			log.Fatal(err.Error(), zap.Error(err))
		}
	} else {
		log.Info("Successfully created bucket", zap.String("bucket", cfg.BucketName))
	}
	return &Minio{client: client, bucket: cfg.BucketName}, nil
}

func (m *Minio) GetClient() *minio.Client {
	return m.client
}

func (m *Minio) UploadBase64File(ctx context.Context, file_path_name string, base64_content string, file_type int) (err error) {
	log, err := log.GetZapLogger(ctx)
	if err != nil {
		return err
	}
	// Decode the base64 content
	decodedContent, err := base64.StdEncoding.DecodeString(base64_content)
	if err != nil {
		return err
	}
	// Convert the decoded content to an io.Reader
	contentReader := strings.NewReader(string(decodedContent))
	// Upload the content to MinIO
	contentType := checkFileType(file_type) // Adjust based on the file type
	size := int64(len(decodedContent))
	// Create the file path with folder structure
	_, err = m.client.PutObjectWithContext(ctx, m.bucket, file_path_name, contentReader, size, minio.PutObjectOptions{ContentType: contentType})
	if err != nil {
		log.Error("Failed to upload file to MinIO", zap.Error(err))
		return err
	}
	return nil
}

func checkFileType(file_type int) string {
	switch file_type {
	case int(artifactpb.FileType_FILE_TYPE_PDF):
		return "application/pdf"
	case int(artifactpb.FileType_FILE_TYPE_MARKDOWN):
		return "text/markdown"
	case int(artifactpb.FileType_FILE_TYPE_TEXT):
		return "text/plain"
	default:
		return "application/octet-stream"
	}
}

// delete the file from minio
func (m *Minio) DeleteFile(ctx context.Context, file_path_name string) (err error) {
	log, err := log.GetZapLogger(ctx)
	if err != nil {
		return err
	}
	// Delete the file from MinIO
	err = m.client.RemoveObject(m.bucket, file_path_name)
	if err != nil {
		log.Error("Failed to delete file from MinIO", zap.Error(err))
		return err
	}
	return nil
}
