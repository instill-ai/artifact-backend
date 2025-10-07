package service

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gofrs/uuid"
	"github.com/gogo/status"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/utils"

	miniolocal "github.com/instill-ai/artifact-backend/pkg/minio"
	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	logx "github.com/instill-ai/x/log"
)

// MaxUploadFileSizeMB returns the maximum file size for artifact uploads, in
// megabbytes. For now, this is a constant (512 Mb).
const MaxUploadFileSizeMB int64 = 512

const blobURLPath = "/v1alpha/blob-urls"

// error type for object
var (
	ErrObjectNotUploaded = errors.New("object is not uploaded yet")
)

// GetUploadURL get the upload url of the object
// this function will create a new object and object_url record in the database
func (s *service) GetUploadURL(
	ctx context.Context,
	req *artifactpb.GetObjectUploadURLRequest,
	namespaceUID uuid.UUID,
	namespaceID string,
	creatorUID uuid.UUID,
) (*artifactpb.GetObjectUploadURLResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)
	// name cannot be empty
	if req.GetObjectName() == "" {
		logger.Error("name cannot be empty")
		return nil, status.Errorf(codes.InvalidArgument, "name cannot be empty")
	}

	// check if name is longer than 400 characters
	if len(req.GetObjectName()) > 400 {
		logger.Error("name should not be longer than 400 characters")
		return nil, status.Errorf(codes.InvalidArgument, "name should not be longer than 400 characters")
	}

	// check expiration_time is valid. if it is lower than 60, we will use 60 as the expiration_time
	if req.GetUrlExpireDays() < 1 {
		req.UrlExpireDays = 1
	}

	// check expiration_time is valid. if it is greater than 7 days, we will use 7 days as the expiration_time
	if req.GetUrlExpireDays() > 7 {
		req.UrlExpireDays = 7
	}

	objectExpireDays := int(req.GetObjectExpireDays())
	lastModifiedTime := req.GetLastModifiedTime().AsTime()
	contentType := utils.DetermineMimeType(req.GetObjectName())
	// create object
	object := &repository.Object{
		Name:             req.GetObjectName(),
		NamespaceUID:     namespaceUID,
		CreatorUID:       creatorUID,
		ContentType:      contentType,
		Size:             0,     // we will update the size when the object is uploaded. when trying to get download url, we will check the size of the object in minio.
		IsUploaded:       false, // we will check and update the is_uploaded when trying to get download url.
		Destination:      "",
		ObjectExpireDays: &objectExpireDays,
		LastModifiedTime: &lastModifiedTime,
	}

	// create object
	createdObject, err := s.repository.CreateObject(ctx, *object)
	if err != nil {
		logger.Error("failed to create object", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to create object: %v", err)
	}

	// get path of the object
	minioPath := miniolocal.GetBlobObjectPath(createdObject.NamespaceUID, createdObject.UID)
	// update the object destination
	createdObject.Destination = minioPath
	_, err = s.repository.UpdateObject(ctx, *createdObject)
	if err != nil {
		logger.Error("failed to update object", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to update object: %v", err)
	}

	// get presigned url for uploading object
	expirationTime := time.Duration(req.GetUrlExpireDays()) * time.Hour * 24
	presignedURL, err := s.minIO.GetPresignedURLForUpload(ctx, namespaceUID, createdObject.UID, req.GetObjectName(), expirationTime)
	if err != nil {
		logger.Error("failed to make presigned url for upload", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to make presigned url for upload: %v", err)
	}

	uploadURL, err := EncodeBlobURL(presignedURL)
	if err != nil {
		logger.Error("failed to encode blob url", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to encode blob url: %v", err)
	}

	expireAtTS, err := getExpireAtTS(presignedURL)
	if err != nil {
		logger.Error("failed to get expire at ts", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to get expire at ts: %v", err)
	}

	return &artifactpb.GetObjectUploadURLResponse{
		UploadUrl:   uploadURL,
		UrlExpireAt: timestamppb.New(expireAtTS),
		Object:      repository.TurnObjectInDBToObjectInProto(createdObject),
	}, nil
}

// GetDownloadURL gets the download url of the object
// this function will create a new object_url record in the database for downloading
func (s *service) GetDownloadURL(
	ctx context.Context,
	req *artifactpb.GetObjectDownloadURLRequest,
	namespaceUID uuid.UUID,
	namespaceID string,
) (*artifactpb.GetObjectDownloadURLResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)
	objectUID, err := uuid.FromString(req.GetObjectUid())
	if err != nil {
		logger.Error("failed to parse object uid", zap.Error(err))
		return nil, status.Errorf(codes.InvalidArgument, "failed to parse object uid: %v", err)
	}
	// Get the object from database
	object, err := s.repository.GetObjectByUID(ctx, objectUID)
	if err != nil {
		logger.Error("failed to get object", zap.Error(err))
		return nil, status.Errorf(codes.NotFound, "object not found: %v", err)
	}

	// Verify namespace matches
	if object.NamespaceUID != namespaceUID {
		logger.Error("namespace mismatch")
		return nil, status.Error(codes.PermissionDenied, "namespace mismatch")
	}

	if !object.IsUploaded {
		if !strings.HasPrefix(object.Destination, "ns-") {
			return nil, ErrObjectNotUploaded
		}

		objectInfo, err := s.minIO.GetFileMetadata(ctx, miniolocal.BlobBucketName, object.Destination)
		if err != nil {
			logger.Error("failed to get file", zap.Error(err))
			return nil, status.Errorf(codes.Internal, "failed to get file: %v", err)
		}
		object.IsUploaded = true
		object.Size = objectInfo.Size
		object.LastModifiedTime = &objectInfo.LastModified
		object.ContentType = objectInfo.ContentType
	}

	// Check URL expiration days
	urlExpireDays := req.GetUrlExpireDays()
	if urlExpireDays < 1 {
		urlExpireDays = 1
	}
	if urlExpireDays > 7 {
		urlExpireDays = 7
	}

	expirationTime := time.Duration(urlExpireDays) * time.Hour * 24

	// Get presigned URL for downloading object
	presignedURL, err := s.minIO.GetPresignedURLForDownload(
		ctx,
		object.NamespaceUID,
		object.UID,
		object.Name,
		object.ContentType,
		expirationTime,
	)
	if err != nil {
		logger.Error("failed to make presigned url for download", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to make presigned url for download: %v", err)
	}

	downloadURL, err := EncodeBlobURL(presignedURL)
	if err != nil {
		logger.Error("failed to encode blob url", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to encode blob url: %v", err)
	}

	expireAtTS, err := getExpireAtTS(presignedURL)
	if err != nil {
		logger.Error("failed to get expire at ts", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to get expire at ts: %v", err)
	}

	objectInProto := repository.TurnObjectInDBToObjectInProto(object)

	return &artifactpb.GetObjectDownloadURLResponse{
		DownloadUrl: downloadURL,
		UrlExpireAt: timestamppb.New(expireAtTS),
		Object:      objectInProto,
	}, nil
}

// EncodeBlobURL encodes the presigned URL to a blob URL. The presigned URL
// provided by MinIO is a self-contained URL that can be used to upload or
// download the object. The structure follows the AWS S3 presigned URL format,
// which consists of query parameters including signature.
//
// To make the URL easier to use in different use cases, we encode the presigned
// URL to a base64 string in the format:
// schema://host:port/v1alpha/blob-urls/base64_encoded_presigned_url
//
// This approach is inspired by MinIO WebUI, which uses the same base64 encoding
// for presigned URLs when generating shareable links. Benefits of this
// approach:
//  1. The URL remains self-contained and signed.
//  2. No query parameters in the URL, making it easier to use in different
//     contexts (e.g., as a query parameter of another endpoint).
//  3. Provides basic encapsulation for the presigned URL.
//  4. Simplifies proxy implementation in the API gateway - the gateway can
//     directly decode the base64 string to the presigned URL and forward the
//     request to MinIO.
func EncodeBlobURL(presignedURL *url.URL) (string, error) {
	presignedURLBase64 := base64.URLEncoding.EncodeToString([]byte(presignedURL.String()))

	path, err := url.JoinPath(blobURLPath, presignedURLBase64)
	if err != nil {
		return "", status.Errorf(codes.Internal, "failed to join path: %v", err)
	}
	instillCoreHost, err := url.Parse(config.Config.Server.InstillCoreHost)
	if err != nil {
		return "", status.Errorf(codes.Internal, "failed to parse instill core host: %v", err)
	}
	u := url.URL{
		Scheme: instillCoreHost.Scheme,
		Host:   instillCoreHost.Host,
		Path:   path,
	}
	return u.String(), nil
}

// DecodeBlobURL decodes a blob URL back to the original presigned URL.
// This is the inverse of encodeBlobURL and extracts the base64-encoded
// presigned URL from the blob URL format:
// schema://host:port/v1alpha/blob-urls/base64_encoded_presigned_url
func DecodeBlobURL(blobURL string) (string, error) {
	// Check if it's a blob URL
	if !strings.Contains(blobURL, "/v1alpha/blob-urls/") {
		return "", fmt.Errorf("not a valid blob URL format")
	}

	// Parse the blob URL and extract the base64-encoded presigned URL
	urlParts := strings.Split(blobURL, "/")
	if len(urlParts) < 4 {
		return "", fmt.Errorf("invalid blob URL format")
	}

	// Find the "blob-urls" segment and get the next segment
	for i, part := range urlParts {
		if part == "blob-urls" && i+1 < len(urlParts) {
			base64EncodedURL := urlParts[i+1]

			// Decode the base64-encoded presigned URL
			decodedURL, err := base64.URLEncoding.DecodeString(base64EncodedURL)
			if err != nil {
				// Try standard encoding if URL encoding fails
				decodedURL, err = base64.StdEncoding.DecodeString(base64EncodedURL)
				if err != nil {
					return "", fmt.Errorf("failed to decode presigned URL: %w", err)
				}
			}

			return string(decodedURL), nil
		}
	}

	return "", fmt.Errorf("could not find blob-urls segment in URL")
}

func getExpireAtTS(presignedURL *url.URL) (time.Time, error) {
	issuedAt := presignedURL.Query().Get("X-Amz-Date")
	expireTimeSeconds, err := strconv.Atoi(presignedURL.Query().Get("X-Amz-Expires"))
	if err != nil {
		return time.Time{}, err
	}
	issuedAtTS, err := time.Parse("20060102T150405Z", issuedAt)
	if err != nil {
		return time.Time{}, err
	}
	expireAtTS := issuedAtTS.Add(time.Duration(expireTimeSeconds) * time.Second)
	return expireAtTS, nil
}
