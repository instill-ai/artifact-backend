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

	"github.com/gogo/status"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/repository/object"
	"github.com/instill-ai/artifact-backend/pkg/types"
	"github.com/instill-ai/artifact-backend/pkg/utils"

	artifactpb "github.com/instill-ai/protogen-go/artifact/v1alpha"
	logx "github.com/instill-ai/x/log"
)

// MaxUploadFileSizeMB returns the maximum file size for artifact uploads, in
// megabytes. For now, this is a constant (512 Mb).
const MaxUploadFileSizeMB int64 = 512

const blobURLPath = "/v1alpha/blob-urls"

// error type for object
var (
	ErrObjectNotUploaded = errors.New("object is not uploaded yet")
)

// GetUploadURL get the upload url of the object
func (s *service) GetUploadURL(
	ctx context.Context,
	req *artifactpb.GetObjectUploadURLRequest,
	namespaceUID types.NamespaceUIDType,
	namespaceID string,
	creatorUID types.CreatorUIDType,
) (*artifactpb.GetObjectUploadURLResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)
	// display name cannot be empty
	if req.GetDisplayName() == "" {
		logger.Error("display name cannot be empty")
		return nil, status.Errorf(codes.InvalidArgument, "display name cannot be empty")
	}

	// check if display name is longer than 400 characters
	if len(req.GetDisplayName()) > 400 {
		logger.Error("display name should not be longer than 400 characters")
		return nil, status.Errorf(codes.InvalidArgument, "display name should not be longer than 400 characters")
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
	contentType := utils.DetermineMimeType(req.GetDisplayName())
	// create object
	obj := &repository.ObjectModel{
		DisplayName:      req.GetDisplayName(),
		NamespaceUID:     namespaceUID,
		CreatorUID:       creatorUID,
		ContentType:      contentType,
		Size:             0,     // we will update the size when the object is uploaded. when trying to get download url, we will check the size of the object in minio.
		IsUploaded:       false, // we will check and update the is_uploaded when trying to get download url.
		StoragePath:      "",
		ObjectExpireDays: &objectExpireDays,
		LastModifiedTime: &lastModifiedTime,
	}

	// create object
	createdObject, err := s.repository.CreateObject(ctx, *obj)
	if err != nil {
		logger.Error("failed to create object", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to create object: %v", err)
	}

	// get path of the object in blob storage
	minioPath := object.GetBlobObjectPath(createdObject.NamespaceUID, createdObject.UID)
	// update the object storage path
	createdObject.StoragePath = minioPath
	_, err = s.repository.UpdateObject(ctx, *createdObject)
	if err != nil {
		logger.Error("failed to update object", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to update object: %v", err)
	}

	// get presigned url for uploading object
	expirationTime := time.Duration(req.GetUrlExpireDays()) * time.Hour * 24
	presignedURL, err := s.repository.GetMinIOStorage().GetPresignedURLForUpload(ctx, namespaceUID, createdObject.UID, req.GetDisplayName(), expirationTime)
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

// GetObject retrieves an object by its ID within a namespace
func (s *service) GetObject(
	ctx context.Context,
	namespaceUID types.NamespaceUIDType,
	objectID string,
) (*artifactpb.Object, error) {
	logger, _ := logx.GetZapLogger(ctx)

	// Get the object from database using the hash-based ID
	obj, err := s.repository.GetObjectByID(ctx, namespaceUID, types.ObjectIDType(objectID))
	if err != nil {
		logger.Error("failed to get object", zap.Error(err))
		return nil, status.Errorf(codes.NotFound, "object not found: %v", err)
	}

	return repository.TurnObjectInDBToObjectInProto(obj), nil
}

// UpdateObject updates an object's metadata based on the update mask
func (s *service) UpdateObject(
	ctx context.Context,
	namespaceUID types.NamespaceUIDType,
	objectID string,
	obj *artifactpb.Object,
	updateMask *fieldmaskpb.FieldMask,
) (*artifactpb.Object, error) {
	logger, _ := logx.GetZapLogger(ctx)

	// Get the existing object from database using the hash-based ID
	existingObj, err := s.repository.GetObjectByID(ctx, namespaceUID, types.ObjectIDType(objectID))
	if err != nil {
		logger.Error("failed to get object", zap.Error(err))
		return nil, status.Errorf(codes.NotFound, "object not found: %v", err)
	}

	// Verify namespace matches
	if existingObj.NamespaceUID != namespaceUID {
		logger.Error("namespace mismatch")
		return nil, status.Error(codes.PermissionDenied, "namespace mismatch")
	}

	// Build update map based on update mask (or update all mutable fields if no mask)
	updateMap := make(map[string]any)
	paths := updateMask.GetPaths()

	// If no update mask, or mask contains is_uploaded, update the field
	if len(paths) == 0 || containsPath(paths, "is_uploaded") {
		if obj.GetIsUploaded() {
			updateMap[repository.ObjectColumn.IsUploaded] = true

			// When marking as uploaded, verify the file exists in MinIO and get metadata
			if !existingObj.IsUploaded && strings.HasPrefix(existingObj.StoragePath, "ns-") {
				fileMetadata, err := s.repository.GetMinIOStorage().GetFileMetadata(ctx, object.BlobBucketName, existingObj.StoragePath)
				if err != nil {
					logger.Warn("failed to verify file in MinIO during upload confirmation", zap.Error(err))
					// Continue anyway - the file might not be ready yet, but we'll mark as uploaded
				} else {
					// Update size and metadata from MinIO
					if fileMetadata.Size > 0 {
						updateMap[repository.ObjectColumn.Size] = fileMetadata.Size
					}
					if fileMetadata.ContentType != "" {
						updateMap[repository.ObjectColumn.ContentType] = fileMetadata.ContentType
					}
					updateMap[repository.ObjectColumn.LastModifiedTime] = fileMetadata.LastModified
				}
			}
		}
	}

	// If no update mask, or mask contains size, update the field (if not already set from MinIO)
	if (len(paths) == 0 || containsPath(paths, "size")) && obj.GetSize() > 0 {
		if _, exists := updateMap[repository.ObjectColumn.Size]; !exists {
			updateMap[repository.ObjectColumn.Size] = obj.GetSize()
		}
	}

	// If no update mask, or mask contains content_type, update the field
	if (len(paths) == 0 || containsPath(paths, "content_type")) && obj.GetContentType() != "" {
		if _, exists := updateMap[repository.ObjectColumn.ContentType]; !exists {
			updateMap[repository.ObjectColumn.ContentType] = obj.GetContentType()
		}
	}

	if len(updateMap) == 0 {
		// Nothing to update, return existing object
		return repository.TurnObjectInDBToObjectInProto(existingObj), nil
	}

	// Perform the update
	updatedObj, err := s.repository.UpdateObjectByUpdateMap(ctx, existingObj.UID, updateMap)
	if err != nil {
		logger.Error("failed to update object", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to update object: %v", err)
	}

	logger.Info("object updated",
		zap.String("object_id", objectID),
		zap.Any("updated_fields", updateMap))

	return repository.TurnObjectInDBToObjectInProto(updatedObj), nil
}

// containsPath checks if a path is in the list of paths
func containsPath(paths []string, target string) bool {
	for _, p := range paths {
		if p == target {
			return true
		}
	}
	return false
}

// GetDownloadURL gets the download url of the object
// this function will create a new object_url record in the database for downloading
func (s *service) GetDownloadURL(
	ctx context.Context,
	objectID string,
	namespaceUID types.NamespaceUIDType,
	namespaceID string,
	urlExpireDays int32,
	downloadFilenameParam string,
) (*artifactpb.GetObjectDownloadURLResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)

	// Get the object from database using the hash-based ID
	obj, err := s.repository.GetObjectByID(ctx, namespaceUID, types.ObjectIDType(objectID))
	if err != nil {
		logger.Error("failed to get object", zap.Error(err))
		return nil, status.Errorf(codes.NotFound, "object not found: %v", err)
	}

	// Verify namespace matches
	if obj.NamespaceUID != namespaceUID {
		logger.Error("namespace mismatch")
		return nil, status.Error(codes.PermissionDenied, "namespace mismatch")
	}

	if !obj.IsUploaded {
		if !strings.HasPrefix(obj.StoragePath, "ns-") {
			return nil, ErrObjectNotUploaded
		}

		objectInfo, err := s.repository.GetMinIOStorage().GetFileMetadata(ctx, object.BlobBucketName, obj.StoragePath)
		if err != nil {
			logger.Error("failed to get file", zap.Error(err))
			return nil, status.Errorf(codes.Internal, "failed to get file: %v", err)
		}
		obj.IsUploaded = true
		obj.Size = objectInfo.Size
		obj.LastModifiedTime = &objectInfo.LastModified
		obj.ContentType = objectInfo.ContentType
	}

	// Check URL expiration days
	if urlExpireDays < 1 {
		urlExpireDays = 1
	}
	if urlExpireDays > 7 {
		urlExpireDays = 7
	}

	expirationTime := time.Duration(urlExpireDays) * time.Hour * 24

	// Determine download filename:
	// 1. Use custom download_filename from request if provided (user-friendly name)
	// 2. Otherwise extract filename from the object display name path
	downloadFilename := downloadFilenameParam
	if downloadFilename == "" {
		// Extract filename from obj.DisplayName which might be a path like
		// "code_executor_agent/namespace/chat/filename.png/0"
		downloadFilename = obj.DisplayName
		nameParts := strings.Split(obj.DisplayName, "/")
		if len(nameParts) > 1 {
			// Check if the last part is a version number (all digits)
			lastPart := nameParts[len(nameParts)-1]
			if _, err := strconv.Atoi(lastPart); err == nil && len(nameParts) > 2 {
				// Last part is version number, use second-to-last as filename
				downloadFilename = nameParts[len(nameParts)-2]
			} else {
				// Last part is the filename
				downloadFilename = lastPart
			}
		}
	}

	// Get presigned URL for downloading object
	presignedURL, err := s.repository.GetMinIOStorage().GetPresignedURLForDownload(
		ctx,
		object.BlobBucketName,
		obj.StoragePath,
		downloadFilename,
		obj.ContentType,
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

	objectInProto := repository.TurnObjectInDBToObjectInProto(obj)

	return &artifactpb.GetObjectDownloadURLResponse{
		DownloadUrl: downloadURL,
		UrlExpireAt: timestamppb.New(expireAtTS),
		Object:      objectInProto,
	}, nil
}

// GetDownloadURLByObjectUID gets the download url of the object by its UID.
// This is similar to GetDownloadURL but accepts object UID instead of hash-based ID.
// Use this when you only have the object UID (e.g., extracted from storage path).
func (s *service) GetDownloadURLByObjectUID(
	ctx context.Context,
	objectUID types.ObjectUIDType,
	namespaceUID types.NamespaceUIDType,
	namespaceID string,
	urlExpireDays int32,
	downloadFilenameParam string,
) (*artifactpb.GetObjectDownloadURLResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)

	// Get the object from database using UID
	obj, err := s.repository.GetObjectByUID(ctx, objectUID)
	if err != nil {
		logger.Error("failed to get object by UID", zap.Error(err), zap.String("objectUID", objectUID.String()))
		return nil, status.Errorf(codes.NotFound, "object not found: %v", err)
	}

	// Verify namespace matches
	if obj.NamespaceUID != namespaceUID {
		logger.Error("namespace mismatch")
		return nil, status.Error(codes.PermissionDenied, "namespace mismatch")
	}

	if !obj.IsUploaded {
		if !strings.HasPrefix(obj.StoragePath, "ns-") {
			return nil, ErrObjectNotUploaded
		}

		objectInfo, err := s.repository.GetMinIOStorage().GetFileMetadata(ctx, object.BlobBucketName, obj.StoragePath)
		if err != nil {
			logger.Error("failed to get file", zap.Error(err))
			return nil, status.Errorf(codes.Internal, "failed to get file: %v", err)
		}
		obj.IsUploaded = true
		obj.Size = objectInfo.Size
		obj.LastModifiedTime = &objectInfo.LastModified
		obj.ContentType = objectInfo.ContentType
	}

	// Check URL expiration days
	if urlExpireDays < 1 {
		urlExpireDays = 1
	}
	if urlExpireDays > 7 {
		urlExpireDays = 7
	}

	expirationTime := time.Duration(urlExpireDays) * time.Hour * 24

	// Determine download filename
	downloadFilename := downloadFilenameParam
	if downloadFilename == "" {
		downloadFilename = obj.DisplayName
		nameParts := strings.Split(obj.DisplayName, "/")
		if len(nameParts) > 1 {
			lastPart := nameParts[len(nameParts)-1]
			if _, err := strconv.Atoi(lastPart); err == nil && len(nameParts) > 2 {
				downloadFilename = nameParts[len(nameParts)-2]
			} else {
				downloadFilename = lastPart
			}
		}
	}

	// Get presigned URL for downloading object
	presignedURL, err := s.repository.GetMinIOStorage().GetPresignedURLForDownload(
		ctx,
		object.BlobBucketName,
		obj.StoragePath,
		downloadFilename,
		obj.ContentType,
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

	objectInProto := repository.TurnObjectInDBToObjectInProto(obj)

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

func getExpireAtTS(presignedURL *url.URL) (time.Time, error) {
	issuedAt := presignedURL.Query().Get("X-Amz-Date")
	expiresStr := presignedURL.Query().Get("X-Amz-Expires")

	// Handle empty expiration parameter - default to 24 hours from now
	if expiresStr == "" {
		return time.Now().Add(24 * time.Hour), nil
	}

	expireTimeSeconds, err := strconv.Atoi(expiresStr)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse X-Amz-Expires '%s': %w", expiresStr, err)
	}

	// Handle empty issued date - use current time
	if issuedAt == "" {
		return time.Now().Add(time.Duration(expireTimeSeconds) * time.Second), nil
	}

	issuedAtTS, err := time.Parse("20060102T150405Z", issuedAt)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse X-Amz-Date '%s': %w", issuedAt, err)
	}
	expireAtTS := issuedAtTS.Add(time.Duration(expireTimeSeconds) * time.Second)
	return expireAtTS, nil
}
