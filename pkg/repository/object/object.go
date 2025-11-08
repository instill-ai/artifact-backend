package object

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/instill-ai/artifact-backend/pkg/types"
	"github.com/minio/minio-go/v7"
)

// Common constants
const (
	// BlobBucketName is used to interact with data upload by clients via presigned upload URLs.
	BlobBucketName = "core-blob"

	ConvertedFileDir = "converted-file"
	ChunkDir         = "chunk"
)

// GetBlobObjectPath makes object path from objectUUID
// Format: ns-{namespaceUUID}/obj-{objectUUID}
func GetBlobObjectPath(namespaceUUID types.NamespaceUIDType, objectUUID types.ObjectUIDType) string {
	return fmt.Sprintf("ns-%s/obj-%s", namespaceUUID.String(), objectUUID.String())
}

// Storage defines the interface for object storage operations
// Implementations: MinIO (default), GCS (for VertexAI)
type Storage interface {
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

	// GetBucket returns the default bucket name for this storage backend
	// This is used by VertexAI to construct gs:// URIs for file caching
	GetBucket() string
}
