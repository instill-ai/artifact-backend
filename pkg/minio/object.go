package minio

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/gofrs/uuid"
	"github.com/instill-ai/artifact-backend/pkg/logger"
	"go.uber.org/zap"
)

// ObjectI is the interface for object-related operations.
type ObjectI interface {
	// GetPresignedURLForUpload creates a presigned URL for uploading an object.
	GetPresignedURLForUpload(ctx context.Context, namespaceUUID uuid.UUID, objectUUID uuid.UUID, urlExpiration time.Duration) (*url.URL, error)
	// GetPresignedURLForDownload creates a presigned URL for downloading an object.
	GetPresignedURLForDownload(ctx context.Context, namespaceUUID uuid.UUID, objectUUID uuid.UUID, urlExpiration time.Duration) (*url.URL, error)
}

// GetPresignedURLForUpload creates a presigned URL for uploading an object.
func (m *Minio) GetPresignedURLForUpload(ctx context.Context, namespaceUUID uuid.UUID, objectUUID uuid.UUID, expiration time.Duration) (*url.URL, error) {
	log, err := logger.GetZapLogger(ctx)
	if err != nil {
		return nil, err
	}
	// check if the expiration is within the range of 1sec to 7 days.
	if expiration > time.Hour*24*7 {
		return nil, errors.New("expiration time must be within 1sec to 7 days")
	}
	// Get presigned URL for uploading object
	presignedURL, err := m.client.PresignedPutObject(BlobBucketName, GetBlobObjectPath(namespaceUUID, objectUUID), expiration)
	if err != nil {
		log.Error("Failed to make presigned URL for upload", zap.Error(err))
		return nil, err
	}

	return presignedURL, nil
}

// GetPresignedURLForDownload creates a presigned URL for downloading an object.
func (m *Minio) GetPresignedURLForDownload(ctx context.Context, namespaceUUID uuid.UUID, objectUUID uuid.UUID, expiration time.Duration) (*url.URL, error) {
	log, err := logger.GetZapLogger(ctx)
	if err != nil {
		return nil, err
	}
	// check if the expiration is within the range of 1sec to 7 days.
	if expiration > time.Hour*24*7 {
		return nil, errors.New("expiration time must be within 1sec to 7 days")
	}

	// Get presigned URL for downloading object
	presignedURL, err := m.client.PresignedGetObject(BlobBucketName, GetBlobObjectPath(namespaceUUID, objectUUID), expiration, url.Values{})
	if err != nil {
		log.Error("Failed to make presigned URL for download", zap.Error(err))
		return nil, err
	}

	return presignedURL, nil
}

// make object path from objectUUID.
// namespaceUUID / objectUUID
func GetBlobObjectPath(namespaceUUID uuid.UUID, objectUUID uuid.UUID) string {
	return fmt.Sprintf("ns-%s/obj-%s", namespaceUUID.String(), objectUUID.String())
}
