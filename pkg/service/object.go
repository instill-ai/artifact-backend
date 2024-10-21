package service

import (
	"context"
	"fmt"
	"time"

	"github.com/gofrs/uuid"
	"github.com/gogo/status"
	"github.com/instill-ai/artifact-backend/pkg/logger"
	"github.com/instill-ai/artifact-backend/pkg/minio"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/utils"
	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// GetUploadURL get the upload url of the object
// this function will create a new object and object_url record in the database
func (s *Service) GetUploadURL(
	ctx context.Context,
	req *artifactpb.GetObjectUploadURLRequest,
	namespaceUID uuid.UUID,
	creatorUID uuid.UUID,
) (*artifactpb.GetObjectUploadURLResponse, error) {
	log, _ := logger.GetZapLogger(ctx)
	// name cannot be empty
	if req.GetObjectName() == "" {
		log.Error("name cannot be empty")
		return nil, status.Errorf(codes.InvalidArgument, "name cannot be empty")
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
	minioPath := minio.GetBlobObjectPath(createdObject.NamespaceUID, createdObject.UID)
	// update the object destination
	createdObject.Destination = minioPath
	_, err = s.Repository.UpdateObject(ctx, *createdObject)
	if err != nil {
		log.Error("failed to update object", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to update object: %v", err)
	}

	// get presigned url for uploading object
	presignedURL, err := s.MinIO.MakePresignedURLForUpload(ctx, namespaceUID, createdObject.UID, time.Duration(req.GetUrlExpireDays())*time.Hour*24)
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

	createdObjectURL, err := s.Repository.CreateObjectURL(ctx, *objectURL)
	if err != nil {
		log.Error("failed to create object url", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to create object url: %v", err)
	}
	createdObjectURL.EncodedURLPath = getEncodedMinioURLPath(createdObjectURL.UID)
	_, err = s.Repository.UpdateObjectURL(ctx, *createdObjectURL)
	if err != nil {
		log.Error("failed to update object url", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to update object url: %v", err)
	}

	objectInProto := turnObjectInDBToObjectInProto(createdObject)

	return &artifactpb.GetObjectUploadURLResponse{
		UploadUrl:   createdObjectURL.EncodedURLPath,
		UrlExpireAt: timestamppb.New(createdObjectURL.URLExpireAt),
		Object:      objectInProto,
	}, nil
}

// getEncodedMinioURLPath get the encoded minio url path
func getEncodedMinioURLPath(objectURLUUID uuid.UUID) string {
	return "/" + "v1alpha" + "/" + "object-urls" + "/" + objectURLUUID.String()
}

// turn object in db to object in proto
func turnObjectInDBToObjectInProto(object *repository.Object) *artifactpb.Object {
	objectInProto := &artifactpb.Object{
		Uid:              object.UID.String(),
		NamespaceUid:     object.NamespaceUID.String(),
		Name:             object.Name,
		Creator:          object.CreatorUID.String(),
		ContentType:      object.ContentType,
		Size:             object.Size,
		IsUploaded:       object.IsUploaded,
		Path:             &object.Destination,
		ObjectExpireDays: int32(*object.ObjectExpireDays),
		CreatedTime:      timestamppb.New(object.CreateTime),
		UpdatedTime:      timestamppb.New(object.UpdateTime),
	}
	if object.LastModifiedTime != nil &&
		!object.LastModifiedTime.IsZero() &&
		object.LastModifiedTime.Format(time.RFC3339) != "1970-01-01T00:00:00Z" {
		lastModifiedTime := timestamppb.New(*object.LastModifiedTime)
		fmt.Println("lastModifiedTime.Format(time.RFC3339)", object.LastModifiedTime.Format(time.RFC3339))
		objectInProto.LastModifiedTime = lastModifiedTime
	}
	return objectInProto
}
