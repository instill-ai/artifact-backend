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

	"github.com/minio/minio-go/v7"
	"go.uber.org/zap"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/utils"
	"github.com/instill-ai/x/resource"

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
	// GetFileMetadata: get the metadata of the file
	GetFileMetadata(ctx context.Context, bucket string, filePath string) (*minio.ObjectInfo, error)
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
						err = m.client.RemoveObject(ctx, bucket, filePathName, minio.RemoveObjectOptions{})
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
					opts := minio.GetObjectOptions{}
					opts.Set(miniox.MinIOHeaderUserUID, m.authenticatedUser(ctx))
					obj, err = m.client.GetObject(ctx, bucket, filePath, opts)
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
	opts := minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	}
	opts.Set(miniox.MinIOHeaderUserUID, m.authenticatedUser(ctx))
	objectCh := m.client.ListObjects(ctx, bucket, opts)

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
					err = m.client.RemoveObject(ctx, bucket, objectName, minio.RemoveObjectOptions{})
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
func BucketFromDestination(destination string) string {
	if strings.Contains(destination, "uploaded-file") {
		return config.Config.Minio.BucketName
	}

	return BlobBucketName
}
