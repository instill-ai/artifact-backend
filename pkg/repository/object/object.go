package object

import (
	"context"
	"fmt"
	"io"
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

	// CopyObject copies an object from one location to another within or across buckets.
	// Uses server-side copy when possible for near-zero cost.
	CopyObject(ctx context.Context, srcBucket, srcPath, dstBucket, dstPath string) error

	// GetBucket returns the default bucket name for this storage backend
	// This is used by VertexAI to construct gs:// URIs for file caching
	GetBucket() string

	// UploadFile uploads raw bytes directly without base64 encoding overhead.
	UploadFile(ctx context.Context, bucket string, filePath string, content []byte, fileMimeType string) error

	// GetFileReader returns a streaming reader for a file and its size.
	// Callers must close the returned ReadCloser when done.
	// This avoids loading entire files into memory for large-file operations.
	GetFileReader(ctx context.Context, bucket string, filePath string) (io.ReadCloser, int64, error)

	// GetFileReaderRange returns a streaming reader for a file starting at
	// byte offset. Callers must close the returned ReadCloser when done and
	// must fetch total size via GetFileMetadata separately (a ranged Stat
	// reports the range length, not the object length).
	//
	// Semantics:
	//   - offset == 0 → read the full object from byte 0 (implementations
	//     MUST NOT set an HTTP Range header in this case; MinIO's
	//     SetRange(0, 0) is a footgun that asks for a single byte).
	//   - offset >  0 → read from byte offset to end of object (HTTP
	//     Range: bytes=N-).
	//
	// Primary caller is the in-activity MinIO → GCS stream resume loop
	// (see ARTIFACT-INV-GCS-STREAM-RESUME): when MinIO's HTTP body is
	// truncated mid-stream, the caller reopens at the already-copied
	// offset and keeps writing to the same GCS resumable writer without
	// re-streaming the bytes already committed.
	GetFileReaderRange(ctx context.Context, bucket string, filePath string, offset int64) (io.ReadCloser, error)

	// UploadFromReader streams data from a reader to storage without buffering
	// the entire content in memory. The size parameter must reflect the total
	// number of bytes that will be read; pass -1 if unknown (not all backends
	// support unknown size).
	UploadFromReader(ctx context.Context, bucket string, filePath string, reader io.Reader, size int64, fileMimeType string) error

	// StreamCopy streams a known-size object to storage using a caller-
	// supplied openAt factory that can (re)open the source at an arbitrary
	// offset. StreamCopy owns one resumable upload writer for the life of
	// the call and transparently resumes on mid-stream truncations up to a
	// backend-defined budget; callers never see io.ErrUnexpectedEOF unless
	// the budget is exhausted.
	//
	// Contract:
	//   - size MUST be the full object length in bytes. The writer is
	//     closed (finalised) iff exactly size bytes have been copied.
	//   - openAt(offset) MUST return a fresh reader positioned at offset
	//     in the source object; closing that reader MUST NOT affect
	//     subsequent opens. The caller is responsible for closing each
	//     returned reader; implementations MUST close readers between
	//     resume attempts.
	//   - progressFn, if non-nil, is invoked after every successful chunk
	//     write with the cumulative copied byte count. Activities use this
	//     to drive Temporal heartbeats.
	//
	// Not every backend supports resumable streaming; backends that don't
	// (e.g. MinIO as a destination) MUST return a clear "not implemented"
	// error so misuse fails loudly at first call.
	StreamCopy(ctx context.Context, bucket string, filePath string, openAt func(offset int64) (io.ReadCloser, error), size int64, fileMimeType string, progressFn func(copied int64)) error
}
