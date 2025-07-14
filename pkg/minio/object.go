package minio

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"

	logx "github.com/instill-ai/x/log"
)

// ObjectI is the interface for object-related operations.
type ObjectI interface {
	// GetPresignedURLForUpload creates a presigned URL for uploading an object.
	GetPresignedURLForUpload(ctx context.Context, namespaceUUID uuid.UUID, objectUUID uuid.UUID, filename string, urlExpiration time.Duration) (*url.URL, error)
	// GetPresignedURLForDownload creates a presigned URL for downloading an object.
	// TODO: the filename and contentType can be removed from the args, we can
	// get them directly from the object metadata in MinIO.
	GetPresignedURLForDownload(ctx context.Context, namespaceUUID uuid.UUID, objectUUID uuid.UUID, filename string, contentType string, urlExpiration time.Duration) (*url.URL, error)
}

// GetPresignedURLForUpload creates a presigned URL for uploading an object.
func (m *Minio) GetPresignedURLForUpload(ctx context.Context, namespaceUUID uuid.UUID, objectUUID uuid.UUID, filename string, expiration time.Duration) (*url.URL, error) {
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

// GetPresignedURLForDownload creates a presigned URL for downloading an object.
func (m *Minio) GetPresignedURLForDownload(ctx context.Context, namespaceUUID uuid.UUID, objectUUID uuid.UUID, filename string, contentType string, expiration time.Duration) (*url.URL, error) {
	logger, err := logx.GetZapLogger(ctx)
	if err != nil {
		return nil, err
	}
	// check if the expiration is within the range of 1sec to 7 days.
	if expiration > time.Hour*24*7 {
		return nil, errors.New("expiration time must be within 1sec to 7 days")
	}

	// These headers will be used to set the content-disposition and content-type headers when downloading the object.
	reqParams := url.Values{}
	reqParams.Set("response-content-disposition", fmt.Sprintf(`inline; filename="%s"`, filename))
	reqParams.Set("response-content-type", contentType)

	// Get presigned URL for downloading object
	presignedURL, err := m.client.PresignHeader(
		ctx,
		http.MethodGet,
		BlobBucketName,
		GetBlobObjectPath(namespaceUUID, objectUUID),
		expiration,
		reqParams,
		m.presignHeaders(),
	)
	if err != nil {
		logger.Error("Failed to make presigned URL for download", zap.Error(err))
		return nil, err
	}

	return presignedURL, nil
}

// make object path from objectUUID.
// namespaceUUID / objectUUID
func GetBlobObjectPath(namespaceUUID uuid.UUID, objectUUID uuid.UUID) string {
	return fmt.Sprintf("ns-%s/obj-%s", namespaceUUID.String(), objectUUID.String())
}

// presignAgent is a hard-coded value for the presigned URLs. Since the client
// that requests the presigned URL (console) and the one that interacts with
// MinIO (api-gateway) are different, the first step to audit the MinIO clients
// is setting this value as a constant.
// TODO:
//  1. Pass the agent from the console when requesting the presigned URL and
//     when using that URL.
//  2. Read the agent value in the public GetObject*URL methods.
//  3. Pass the agent value from `api-gateway` when relaying the presigned URL
//     calls.
const presignAgent = "artifact-backend-presign"

// presignHeaders are added to the presign request. The client that uses the
// presigned URL must use the same values for these headers.
// For the moment, only the agent is added and its value is hardcoded. This is
// useful to audit the MinIO calls that come from Instill AI's services.
// TODO: The GetObject[Up|Down]loadURL methods should accept an `extraHeaders`
// param that will let clients set these params.
func (m *Minio) presignHeaders() http.Header {
	h := http.Header{}
	h.Set("User-Agent", presignAgent)
	return h
}
