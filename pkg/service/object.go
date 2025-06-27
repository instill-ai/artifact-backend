package service

import (
	"context"
	"errors"
	"path"
	"time"

	"github.com/gofrs/uuid"
	"github.com/gogo/status"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/logger"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/utils"

	miniolocal "github.com/instill-ai/artifact-backend/pkg/minio"
	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

// error type for object
var (
	ErrObjectNotUploaded = errors.New("object is not uploaded yet")
)

// GetUploadURL get the upload url of the object
// this function will create a new object and object_url record in the database
func (s *Service) GetUploadURL(
	ctx context.Context,
	req *artifactpb.GetObjectUploadURLRequest,
	namespaceUID uuid.UUID,
	namespaceID string,
	creatorUID uuid.UUID,
) (*artifactpb.GetObjectUploadURLResponse, error) {
	log, _ := logger.GetZapLogger(ctx)
	// name cannot be empty
	if req.GetObjectName() == "" {
		log.Error("name cannot be empty")
		return nil, status.Errorf(codes.InvalidArgument, "name cannot be empty")
	}

	// check if name is longer than 400 characters
	if len(req.GetObjectName()) > 400 {
		log.Error("name should not be longer than 400 characters")
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
	createdObject, err := s.Repository.CreateObject(ctx, *object)
	if err != nil {
		log.Error("failed to create object", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to create object: %v", err)
	}

	// get path of the object
	minioPath := miniolocal.GetBlobObjectPath(createdObject.NamespaceUID, createdObject.UID)
	// update the object destination
	createdObject.Destination = minioPath
	_, err = s.Repository.UpdateObject(ctx, *createdObject)
	if err != nil {
		log.Error("failed to update object", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to update object: %v", err)
	}

	// get presigned url for uploading object
	presignedURL, err := s.MinIO.GetPresignedURLForUpload(ctx, namespaceUID, createdObject.UID, time.Duration(req.GetUrlExpireDays())*time.Hour*24)
	if err != nil {
		log.Error("failed to make presigned url for upload", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to make presigned url for upload: %v", err)
	}

	// remove the protocol and host from the presignedURL
	presignedURLPathQuery := presignedURL.Path + "?" + presignedURL.RawQuery

	// create object_url and update the encoded_url_path
	objectURL := &repository.ObjectURL{
		NamespaceUID:   createdObject.NamespaceUID,
		ObjectUID:      createdObject.UID,
		URLExpireAt:    time.Now().UTC().Add(time.Duration(req.GetUrlExpireDays()) * time.Hour * 24),
		MinioURLPath:   presignedURLPathQuery,
		EncodedURLPath: "",
		Type:           repository.ObjectURLTypeUpload,
	}

	createdObjectURL, err := s.Repository.CreateObjectURLWithUIDInEncodedURLPath(ctx, *objectURL, namespaceID, EncodedMinioURLPath)
	if err != nil {
		log.Error("failed to create object url", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to create object url: %v", err)
	}

	objectInProto := repository.TurnObjectInDBToObjectInProto(createdObject)

	return &artifactpb.GetObjectUploadURLResponse{
		UploadUrl:   createdObjectURL.EncodedURLPath,
		UrlExpireAt: timestamppb.New(createdObjectURL.URLExpireAt),
		Object:      objectInProto,
	}, nil
}

// GetDownloadURL gets the download url of the object
// this function will create a new object_url record in the database for downloading
func (s *Service) GetDownloadURL(
	ctx context.Context,
	req *artifactpb.GetObjectDownloadURLRequest,
	namespaceUID uuid.UUID,
	namespaceID string,
) (*artifactpb.GetObjectDownloadURLResponse, error) {
	log, _ := logger.GetZapLogger(ctx)
	objectUID, err := uuid.FromString(req.GetObjectUid())
	if err != nil {
		log.Error("failed to parse object uid", zap.Error(err))
		return nil, status.Errorf(codes.InvalidArgument, "failed to parse object uid: %v", err)
	}
	// Get the object from database
	object, err := s.Repository.GetObjectByUID(ctx, objectUID)
	if err != nil {
		log.Error("failed to get object", zap.Error(err))
		return nil, status.Errorf(codes.NotFound, "object not found: %v", err)
	}

	// Verify namespace matches
	if object.NamespaceUID != namespaceUID {
		log.Error("namespace mismatch")
		return nil, status.Error(codes.PermissionDenied, "namespace mismatch")
	}

	if !object.IsUploaded {
		return nil, ErrObjectNotUploaded
	}

	// Check URL expiration days
	urlExpireDays := req.GetUrlExpireDays()
	if urlExpireDays < 1 {
		urlExpireDays = 1
	}
	if urlExpireDays > 7 {
		urlExpireDays = 7
	}

	// Get presigned URL for downloading object
	presignedURL, err := s.MinIO.GetPresignedURLForDownload(
		ctx,
		object.NamespaceUID,
		object.UID,
		time.Duration(urlExpireDays)*time.Hour*24,
	)
	if err != nil {
		log.Error("failed to make presigned url for download", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to make presigned url for download: %v", err)
	}

	// Remove the protocol and host from the presignedURL
	presignedURLPathQuery := presignedURL.Path + "?" + presignedURL.RawQuery

	// Create object_url for download
	objectURL := &repository.ObjectURL{
		NamespaceUID:   object.NamespaceUID,
		ObjectUID:      object.UID,
		URLExpireAt:    time.Now().UTC().Add(time.Duration(urlExpireDays) * time.Hour * 24),
		MinioURLPath:   presignedURLPathQuery,
		EncodedURLPath: "",
		Type:           repository.ObjectURLTypeDownload,
	}

	createdObjectURL, err := s.Repository.CreateObjectURLWithUIDInEncodedURLPath(ctx, *objectURL, namespaceID, EncodedMinioURLPath)
	if err != nil {
		log.Error("failed to create object url", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to create object url: %v", err)
	}

	objectInProto := repository.TurnObjectInDBToObjectInProto(object)

	return &artifactpb.GetObjectDownloadURLResponse{
		DownloadUrl: createdObjectURL.EncodedURLPath,
		UrlExpireAt: timestamppb.New(createdObjectURL.URLExpireAt),
		Object:      objectInProto,
	}, nil
}

// EncodedMinioURLPath get the encoded minio url path
func EncodedMinioURLPath(namespaceID string, objectURLUUID uuid.UUID) string {

	// Construct the path
	urlPath := path.Join("v1alpha", "namespaces", namespaceID, "blob-urls", objectURLUUID.String())

	// Ensure the path starts with a forward slash
	return config.Config.Blob.HostPort + "/" + urlPath
}
