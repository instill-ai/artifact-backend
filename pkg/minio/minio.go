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
	"time"

	"github.com/instill-ai/artifact-backend/config"
	log "github.com/instill-ai/artifact-backend/pkg/logger"
	"github.com/instill-ai/artifact-backend/pkg/utils"

	"github.com/minio/minio-go"
	"go.uber.org/zap"
)

type MinioI interface {
	GetClient() *minio.Client
	// uploadFile
	UploadBase64File(ctx context.Context, filePath string, base64Content string, fileMimeType string) (err error)
	// deleteFile
	DeleteFile(ctx context.Context, filePath string) (err error)
	// deleteFiles
	DeleteFiles(ctx context.Context, filePaths []string) chan error
	// deleteFilesWithPrefix
	DeleteFilesWithPrefix(ctx context.Context, prefix string) chan error
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
	for i := 0; i < 3; i++ {
		_, err = m.client.PutObjectWithContext(ctx, m.bucket, filePathName, contentReader, size, minio.PutObjectOptions{ContentType: fileMimeType})
		if err == nil {
			break
		}
		log.Error("Failed to upload file to MinIO, retrying...", zap.String("attempt", fmt.Sprintf("%d", i+1)), zap.Error(err))
		time.Sleep(1 * time.Second)
	}
	if err != nil {
		log.Error("Failed to upload file to MinIO after retries", zap.Error(err))
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
	for attempt := 1; attempt <= 3; attempt++ {
		err = m.client.RemoveObject(m.bucket, filePathName)
		if err == nil {
			break
		}
		log.Error("Failed to delete file from MinIO, retrying...", zap.String("filePathName", filePathName), zap.Int("attempt", attempt), zap.Error(err))
		time.Sleep(time.Duration(attempt) * time.Second)
	}
	if err != nil {
		log.Error("Failed to delete file from MinIO", zap.Error(err))
		return err
	}
	return nil
}

// delete bunch of files from minio
func (m *Minio) DeleteFiles(ctx context.Context, filePathNames []string) chan error {
	errCh := make(chan error, len(filePathNames))
	defer close(errCh)
	log, err := log.GetZapLogger(ctx)
	if err != nil {
		errCh <- err
		return errCh
	}
	// Delete the files from MinIO parallel
	var wg sync.WaitGroup
	for _, filePathName := range filePathNames {
		wg.Add(1)
		go utils.GoRecover(
			func() {
				func(filePathName string, errCh chan error) {
					defer wg.Done()
					var err error
					for attempt := 1; attempt <= 3; attempt++ {
						err = m.client.RemoveObject(m.bucket, filePathName)
						if err == nil {
							break
						}
						log.Error("Failed to delete file from MinIO, retrying...", zap.String("filePathName", filePathName), zap.Int("attempt", attempt), zap.Error(err))
						time.Sleep(time.Duration(attempt) * time.Second)
					}
					if err != nil {
						log.Error("Failed to delete file from MinIO", zap.Error(err))
						errCh <- err
						return
					}
				}(filePathName, errCh)
			}, fmt.Sprintf("DeleteFiles %s", filePathName))
	}
	wg.Wait()
	return errCh
}

func (m *Minio) GetFile(ctx context.Context, filePathName string) ([]byte, error) {
	log, err := log.GetZapLogger(ctx)
	if err != nil {
		return nil, err
	}

	// Get the object using the client with three attempts and proper time delay
	var object *minio.Object
	for attempt := 1; attempt <= 3; attempt++ {
		object, err = m.client.GetObject(m.bucket, filePathName, minio.GetObjectOptions{})
		if err == nil {
			break
		}
		log.Error("Failed to get file from MinIO, retrying...", zap.String("filePathName", filePathName), zap.Int("attempt", attempt), zap.Error(err))
		time.Sleep(time.Duration(attempt) * time.Second)
	}
	if err != nil {
		log.Error("Failed to get file from MinIO after 3 attempts", zap.String("filePathName", filePathName), zap.Error(err))
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
	Index   int
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
	fileCh := make(chan FileContent, len(filePaths))
	errorCh := make(chan error, len(filePaths))

	for i, path := range filePaths {
		wg.Add(1)
		go utils.GoRecover(func() {
			func(i int, filePath string) {
				defer wg.Done()
				var obj *minio.Object
				var err error
				for attempt := 1; attempt <= 3; attempt++ {
					obj, err = m.client.GetObject(m.bucket, filePath, minio.GetObjectOptions{})
					if err == nil {
						break
					}
					log.Error("Failed to get object from MinIO, retrying...", zap.String("path", filePath), zap.Int("attempt", attempt), zap.Error(err))
					time.Sleep(time.Duration(attempt) * time.Second)
				}
				if err != nil {
					log.Error("Failed to get object from MinIO", zap.String("path", filePath), zap.Error(err))
					errorCh <- err
					return
				}
				defer obj.Close()

				var buffer bytes.Buffer
				_, err = io.Copy(&buffer, obj)
				if err != nil {
					log.Error("Failed to read object content", zap.String("path", filePath), zap.Error(err))
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
func (m *Minio) DeleteFilesWithPrefix(ctx context.Context, prefix string) chan error {
	errCh := make(chan error)
	defer close(errCh)
	log, err := log.GetZapLogger(ctx)
	if err != nil {
		errCh <- err
		return errCh
	}

	// List all objects with the given prefix
	objectCh := m.client.ListObjects(m.bucket, prefix, true, nil)

	// Use a WaitGroup to wait for all deletions to complete
	var wg sync.WaitGroup
	for object := range objectCh {
		if object.Err != nil {
			log.Error("Failed to list object from MinIO", zap.Error(object.Err))
			errCh <- object.Err
			continue
		}

		wg.Add(1)
		go utils.GoRecover(func() {
			func(objectName string) {
				defer wg.Done()
				var err error
				for attempt := 1; attempt <= 3; attempt++ {
					err = m.client.RemoveObject(m.bucket, objectName)
					if err == nil {
						break
					}
					log.Error("Failed to delete object from MinIO, retrying...", zap.String("object", objectName), zap.Int("attempt", attempt), zap.Error(err))
					time.Sleep(time.Duration(attempt) * time.Second)
				}
				if err != nil {
					log.Error("Failed to delete object from MinIO", zap.String("object", objectName), zap.Error(err))
					errCh <- err
				}
			}(object.Key)
		}, fmt.Sprintf("DeleteFilesWithPrefix %s", object.Key))
	}

	// Wait for all deletions to complete
	wg.Wait()

	return errCh
}
