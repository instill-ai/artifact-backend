package minio

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go"
	"go.uber.org/zap"

	"github.com/instill-ai/artifact-backend/pkg/utils"

	miniox "github.com/instill-ai/x/minio"
)

type MinioI interface {
	// uploadFile
	UploadBase64File(ctx context.Context, bucket string, filePath string, base64Content string, fileMimeType string) (err error)
	// deleteFile
	DeleteFile(ctx context.Context, bucket string, filePath string) (err error)
	// deleteFiles
	DeleteFiles(ctx context.Context, bucket string, filePaths []string) chan error
	// deleteFilesWithPrefix
	DeleteFilesWithPrefix(ctx context.Context, bucket string, prefix string) chan error
	// GetFile
	GetFile(ctx context.Context, bucket string, filePath string) ([]byte, error)
	// GetFilesByPaths
	GetFilesByPaths(ctx context.Context, bucket string, filePaths []string) ([]FileContent, error)
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
	// Note: this bucket is for storing the blob of the file and not changeable
	// in different environment so we dont put it in config
	BlobBucketName          = "instill-ai-blob"
	KnowledgeBaseBucketName = "instill-ai-knowledge-bases"
)

func NewMinioClientAndInitBucket(params miniox.ClientParams) (*Minio, error) {
	cfg := params.Config
	log := params.Logger.With(
		zap.String("host:port", cfg.Host+":"+cfg.Port),
		zap.String("user", cfg.User),
	)

	log.Info("Initializing MinIO client and bucket")

	// TODO: we should use instill-ai/x/minio.NewMinioClientAndInitBucket.
	// TODO: we should add the user UID headers when present.
	endpoint := net.JoinHostPort(cfg.Host, cfg.Port)
	client, err := minio.New(endpoint, cfg.User, cfg.Password, false)
	if err != nil {
		// log connection error
		log.Error("Cannot connect to MinIO", zap.Error(err))
		return nil, fmt.Errorf("connecting to MinIO: %w", err)
	}

	client.SetAppInfo(params.AppInfo.Name, params.AppInfo.Version)

	// create bucket if not exists for knowledge base
	for _, bucket := range []string{KnowledgeBaseBucketName, BlobBucketName} {
		log := log.With(zap.String("bucket", bucket))

		exists, err := client.BucketExists(bucket)
		if err != nil {
			return nil, fmt.Errorf("checking bucket existence: %w", err)
		}

		if !exists {
			err = client.MakeBucket(bucket, miniox.Location)
			if err != nil {
				return nil, fmt.Errorf("creating bucket: %w", err)
			}
			log.Info("Successfully created bucket", zap.String("bucket", bucket))
		} else {
			log.Info("Bucket already exists", zap.String("bucket", bucket))
		}
	}

	return &Minio{
		client: client,
		logger: log,
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
		_, err = m.client.PutObjectWithContext(ctx, bucket, filePathName, contentReader, size, minio.PutObjectOptions{ContentType: fileMimeType})
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
		err = m.client.RemoveObject(bucket, filePathName)
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

// delete bunch of files from minio
func (m *Minio) DeleteFiles(ctx context.Context, bucket string, filePathNames []string) chan error {
	errCh := make(chan error, len(filePathNames))
	defer close(errCh)
	// Delete the files from MinIO parallel
	var wg sync.WaitGroup
	sem := make(chan struct{}, 10) // Rate limit to 10 concurrent deletions
	for _, filePathName := range filePathNames {
		wg.Add(1)
		sem <- struct{}{} // Acquire a token
		go utils.GoRecover(
			func() {
				func(filePathName string, errCh chan error) {
					defer wg.Done()
					defer func() { <-sem }() // Release the token
					var err error
					for attempt := 1; attempt <= 3; attempt++ {
						err = m.client.RemoveObject(bucket, filePathName)
						if err == nil {
							break
						}
						m.logger.Error("Failed to delete file from MinIO, retrying...", zap.String("filePathName", filePathName), zap.Int("attempt", attempt), zap.Error(err))
						time.Sleep(time.Duration(attempt) * time.Second)
					}
					if err != nil {
						m.logger.Error("Failed to delete file from MinIO", zap.Error(err))
						errCh <- err
						return
					}
				}(filePathName, errCh)
			}, fmt.Sprintf("DeleteFiles %s", filePathName))
	}
	wg.Wait()
	return errCh
}

func (m *Minio) GetFile(ctx context.Context, bucket string, filePathName string) ([]byte, error) {
	// Get the object using the client with three attempts and proper time delay
	var object *minio.Object
	var err error
	for attempt := 1; attempt <= 3; attempt++ {
		object, err = m.client.GetObject(bucket, filePathName, minio.GetObjectOptions{})
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

// FileContent represents a file and its content
type FileContent struct {
	Index   int
	Name    string
	Content []byte
}

// GetFiles retrieves the contents of specified files from MinIO
func (m *Minio) GetFilesByPaths(ctx context.Context, bucket string, filePaths []string) ([]FileContent, error) {
	var wg sync.WaitGroup
	fileCh := make(chan FileContent, len(filePaths))
	errorCh := make(chan error, len(filePaths))

	sem := make(chan struct{}, 10) // Rate limit to 10 concurrent requests
	for i, path := range filePaths {
		wg.Add(1)
		sem <- struct{}{} // Acquire a token
		go utils.GoRecover(func() {
			func(i int, filePath string) {
				defer wg.Done()
				defer func() { <-sem }() // Release the token
				var obj *minio.Object
				var err error
				for attempt := 1; attempt <= 3; attempt++ {
					obj, err = m.client.GetObject(bucket, filePath, minio.GetObjectOptions{})
					if err == nil {
						break
					}
					m.logger.Error("Failed to get object from MinIO, retrying...", zap.String("path", filePath), zap.Int("attempt", attempt), zap.Error(err))
					time.Sleep(time.Duration(attempt) * time.Second)
				}
				if err != nil {
					m.logger.Error("Failed to get object from MinIO", zap.String("path", filePath), zap.Error(err))
					errorCh <- err
					return
				}
				defer obj.Close()

				var buffer bytes.Buffer
				_, err = io.Copy(&buffer, obj)
				if err != nil {
					m.logger.Error("Failed to read object content", zap.String("path", filePath), zap.Error(err))
					errorCh <- err
					return
				}
				fileCh <- FileContent{
					Index:   i,
					Name:    filepath.Base(filePath),
					Content: buffer.Bytes(),
				}
			}(i, path)
		}, fmt.Sprintf("GetFilesByPaths %s", path))
	}

	wg.Wait()
	close(fileCh)
	close(errorCh)

	files := make([]FileContent, len(filePaths))
	for file := range fileCh {
		files[file.Index] = file
	}

	// Check if any errors occurred
	for err := range errorCh {
		if err != nil {
			return nil, err
		}
	}

	return files, nil
}

// delete all files with the same prefix from MinIO
func (m *Minio) DeleteFilesWithPrefix(ctx context.Context, bucket string, prefix string) chan error {
	errCh := make(chan error)
	defer close(errCh)

	// List all objects with the given prefix
	objectCh := m.client.ListObjects(bucket, prefix, true, nil)

	// Use a WaitGroup to wait for all deletions to complete
	var wg sync.WaitGroup
	sem := make(chan struct{}, 10) // Rate limit to 10 concurrent deletions

	for object := range objectCh {
		if object.Err != nil {
			m.logger.Error("Failed to list object from MinIO", zap.Error(object.Err))
			errCh <- object.Err
			continue
		}

		wg.Add(1)
		sem <- struct{}{} // Acquire a token
		go utils.GoRecover(func() {
			func(objectName string) {
				defer wg.Done()
				defer func() { <-sem }() // Release the token
				var err error
				for attempt := 1; attempt <= 3; attempt++ {
					err = m.client.RemoveObject(bucket, objectName)
					if err == nil {
						break
					}
					m.logger.Error("Failed to delete object from MinIO, retrying...", zap.String("object", objectName), zap.Int("attempt", attempt), zap.Error(err))
					time.Sleep(time.Duration(attempt) * time.Second)
				}
				if err != nil {
					m.logger.Error("Failed to delete object from MinIO", zap.String("object", objectName), zap.Error(err))
					errCh <- err
				}
			}(object.Key)
		}, fmt.Sprintf("DeleteFilesWithPrefix %s", object.Key))
	}

	// Wait for all deletions to complete
	wg.Wait()

	return errCh
}
