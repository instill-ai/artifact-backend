package minio

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"

	"github.com/instill-ai/artifact-backend/config"
	log "github.com/instill-ai/artifact-backend/pkg/logger"

	"github.com/minio/minio-go"
	"go.uber.org/zap"
)

type MinioI interface {
	GetClient() *minio.Client
	// uploadFile
	UploadBase64File(ctx context.Context, filePath string, base64Content string, fileMimeType string) (err error)
	// deleteFile
	DeleteFile(ctx context.Context, filePath string) (err error)
	// GetFile
	GetFile(ctx context.Context, filePath string) ([]byte, error)
	// GetFilesByPaths
	GetFilesByPaths(ctx context.Context, filePaths []string) ([]FileContent, error)
	// KnowledgeBase
	KnowledgeBaseI
}

type Minio struct {
	client *minio.Client
	bucket string
}

func NewMinioClientAndInitBucket(cfg config.MinioConfig) (*Minio, error) {
	fmt.Printf("Initializing Minio client and bucket\n")
	// cfg := config.Config.Minio
	log, err := log.GetZapLogger(context.Background())
	if err != nil {
		return nil, err
	}
	client, err := minio.New(cfg.Host+":"+cfg.Port, cfg.RootUser, cfg.RootPwd, false)
	if err != nil {
		fmt.Printf("Initializing Minio client and bucket\n")
		// log connection error
		log.Error("cannot connect to minio",
			zap.String("host:port", cfg.Host+":"+cfg.Port),
			zap.String("user", cfg.RootUser),
			zap.String("pwd", cfg.RootPwd), zap.Error(err))
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

func (m *Minio) UploadBase64File(ctx context.Context, filePathName string, base64Content string, fileMimeType string) (err error) {
	log, err := log.GetZapLogger(ctx)
	if err != nil {
		return err
	}
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
	_, err = m.client.PutObjectWithContext(ctx, m.bucket, filePathName, contentReader, size, minio.PutObjectOptions{ContentType: fileMimeType})
	if err != nil {
		log.Error("Failed to upload file to MinIO", zap.Error(err))
		return err
	}
	return nil
}

// delete the file from minio
func (m *Minio) DeleteFile(ctx context.Context, filePathName string) (err error) {
	log, err := log.GetZapLogger(ctx)
	if err != nil {
		return err
	}
	// Delete the file from MinIO
	err = m.client.RemoveObject(m.bucket, filePathName)
	if err != nil {
		log.Error("Failed to delete file from MinIO", zap.Error(err))
		return err
	}
	return nil
}

func (m *Minio) GetFile(ctx context.Context, filePathName string) ([]byte, error) {
	log, err := log.GetZapLogger(ctx)
	if err != nil {
		return nil, err
	}

	// Get the object using the client
	object, err := m.client.GetObject(m.bucket, filePathName, minio.GetObjectOptions{})
	if err != nil {
		log.Error("Failed to get file from MinIO", zap.Error(err))
		return nil, err
	}
	defer object.Close()

	// Read the object's content
	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(object)
	if err != nil {
		log.Error("Failed to read file from MinIO", zap.Error(err))
		return nil, err
	}

	return buf.Bytes(), nil
}

// FileContent represents a file and its content
type FileContent struct {
	Name    string
	Content []byte
}

// GetFiles retrieves the contents of specified files from MinIO
func (m *Minio) GetFilesByPaths(ctx context.Context, filePaths []string) ([]FileContent, error) {
	log, err := log.GetZapLogger(ctx)
	if err != nil {
		return nil, err
	}

	var wg sync.WaitGroup
	files := make([]FileContent, len(filePaths))
	errors := make([]error, len(filePaths))

	for i, path := range filePaths {
		wg.Add(1)
		go func(index int, filePath string) {
			defer wg.Done()

			obj, err := m.client.GetObject(m.bucket, filePath, minio.GetObjectOptions{})
			if err != nil {
				log.Error("Failed to get object from MinIO", zap.String("path", filePath), zap.Error(err))
				errors[index] = err
				return
			}
			defer obj.Close()

			var buffer bytes.Buffer
			_, err = io.Copy(&buffer, obj)
			if err != nil {
				log.Error("Failed to read object content", zap.String("path", filePath), zap.Error(err))
				errors[index] = err
				return
			}

			files[index] = FileContent{
				Name:    filepath.Base(filePath),
				Content: buffer.Bytes(),
			}
		}(i, path)
	}

	wg.Wait()

	// Check if any errors occurred
	for _, err := range errors {
		if err != nil {
			return nil, err
		}
	}

	return files, nil
}
