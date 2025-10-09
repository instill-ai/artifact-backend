package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/gofrs/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

const (
	// ObjectURLTableName is the table name for object URLs
	ObjectURLTableName = "object_url"
)

// TODO: the ObjectURLModel will be fully removed in the future, we will use the
// presigned URL instead.
type ObjectURL interface {
	ListAllObjectURLs(ctx context.Context, namespaceUID, objectUID uuid.UUID) ([]ObjectURLModel, error)
	DeleteObjectURL(ctx context.Context, uid uuid.UUID) error
	GetObjectURLByUID(ctx context.Context, uid uuid.UUID) (*ObjectURLModel, error)
	GetObjectURLCountByObject(ctx context.Context, objectUID uuid.UUID) (int64, error)
	GetObjectUploadURL(ctx context.Context, objectUID uuid.UUID) (*ObjectURLModel, error)
	GetObjectDownloadURL(ctx context.Context, objectUID uuid.UUID) (*ObjectURLModel, error)
	GetObjectURLByEncodedURLPath(ctx context.Context, encodedURLPath string) (*ObjectURLModel, error)
}

// ObjectURLModel represents the object_url table in the database.
// Note: due to ObjectURLModel will be translated to object_uRL in the database, we use ObjectURLModel instead of objectURL
type ObjectURLModel struct {
	UID            uuid.UUID `gorm:"column:uid;type:uuid;default:gen_random_uuid();primaryKey" json:"uid"`
	NamespaceUID   uuid.UUID `gorm:"column:namespace_uid;type:uuid;not null" json:"namespace_uid"`
	ObjectUID      uuid.UUID `gorm:"column:object_uid;type:uuid;not null" json:"object_uid"`
	URLExpireAt    time.Time `gorm:"column:url_expire_at" json:"url_expire_at"`
	MinioURLPath   string    `gorm:"column:minio_url_path;type:text;not null" json:"minio_url_path"`
	EncodedURLPath string    `gorm:"column:encoded_url_path;type:text;not null" json:"encoded_url_path"`
	// download or upload
	Type       string     `gorm:"column:type;size:10;not null" json:"type"`
	CreateTime time.Time  `gorm:"column:create_time;not null;default:CURRENT_TIMESTAMP" json:"create_time"`
	UpdateTime time.Time  `gorm:"column:update_time;not null;autoUpdateTime" json:"update_time"`
	DeleteTime *time.Time `gorm:"column:delete_time" json:"delete_time"`
}

// Override the table name
func (ObjectURLModel) TableName() string {
	return ObjectURLTableName
}

type ObjectURLColumns struct {
	UID            string
	NamespaceUID   string
	ObjectUID      string
	URLExpireAt    string
	MinioURLPath   string
	EncodedURLPath string
	Type           string
	CreateTime     string
	UpdateTime     string
	DeleteTime     string
}

var ObjectURLColumn = ObjectURLColumns{
	UID:            "uid",
	NamespaceUID:   "namespace_uid",
	ObjectUID:      "object_uid",
	URLExpireAt:    "url_expire_at",
	MinioURLPath:   "minio_url_path",
	EncodedURLPath: "encoded_url_path",
	Type:           "type",
	CreateTime:     "create_time",
	UpdateTime:     "update_time",
	DeleteTime:     "delete_time",
}

const (
	ObjectURLTypeDownload = "download"
	ObjectURLTypeUpload   = "upload"
)

// ListAllObjectURLs fetches all ObjectURLModel records from the database for a given namespace and object, excluding soft-deleted ones.
func (r *repository) ListAllObjectURLs(ctx context.Context, namespaceUID, objectUID uuid.UUID) ([]ObjectURLModel, error) {
	var objectURLs []ObjectURLModel
	whereString := fmt.Sprintf("%v IS NULL AND %v = ? AND %v = ?", ObjectURLColumn.DeleteTime, ObjectURLColumn.NamespaceUID, ObjectURLColumn.ObjectUID)
	if err := r.db.WithContext(ctx).Where(whereString, namespaceUID, objectUID).Find(&objectURLs).Error; err != nil {
		return nil, err
	}
	return objectURLs, nil
}

// DeleteObjectURL performs a soft delete on an ObjectURLModel record.
func (r *repository) DeleteObjectURL(ctx context.Context, uid uuid.UUID) error {
	deleteTime := time.Now().UTC()
	whereString := fmt.Sprintf("%v = ? AND %v IS NULL", ObjectURLColumn.UID, ObjectURLColumn.DeleteTime)
	if err := r.db.WithContext(ctx).Where(whereString, uid).Update(ObjectURLColumn.DeleteTime, deleteTime).Error; err != nil {
		return err
	}
	return nil
}

// GetObjectURLByUID fetches an ObjectURLModel record by its UID.
func (r *repository) GetObjectURLByUID(ctx context.Context, uid uuid.UUID) (*ObjectURLModel, error) {
	var objectURL ObjectURLModel
	whereString := fmt.Sprintf("%v = ? AND %v IS NULL", ObjectURLColumn.UID, ObjectURLColumn.DeleteTime)
	if err := r.db.WithContext(ctx).Where(whereString, uid).First(&objectURL).Error; err != nil {
		return nil, err
	}
	return &objectURL, nil
}

// GetObjectURLCountByObject gets the count of ObjectURLs for a specific Object
func (r *repository) GetObjectURLCountByObject(ctx context.Context, objectUID uuid.UUID) (int64, error) {
	var count int64
	whereString := fmt.Sprintf("%v = ? AND %v IS NULL", ObjectURLColumn.ObjectUID, ObjectURLColumn.DeleteTime)
	if err := r.db.WithContext(ctx).Where(whereString, objectUID).Count(&count).Error; err != nil {
		return 0, err
	}
	return count, nil
}

// GetObjectUploadURL gets the upload url for a specific Object
func (r *repository) GetObjectUploadURL(ctx context.Context, objectUID uuid.UUID) (*ObjectURLModel, error) {
	var objectURL ObjectURLModel
	whereString := fmt.Sprintf("%v = ? AND %v = ? AND %v IS NULL", ObjectURLColumn.ObjectUID, ObjectURLColumn.Type, ObjectURLColumn.DeleteTime)
	if err := r.db.WithContext(ctx).Where(whereString, objectUID, ObjectURLTypeUpload).First(&objectURL).Error; err != nil {
		return nil, err
	}
	return &objectURL, nil
}

// GetObjectDownloadURL gets the download url for a specific Object
func (r *repository) GetObjectDownloadURL(ctx context.Context, objectUID uuid.UUID) (*ObjectURLModel, error) {
	var objectURL ObjectURLModel
	whereString := fmt.Sprintf("%v = ? AND %v = ? AND %v IS NULL", ObjectURLColumn.ObjectUID, ObjectURLColumn.Type, ObjectURLColumn.DeleteTime)
	if err := r.db.WithContext(ctx).Where(whereString, objectUID, ObjectURLTypeDownload).First(&objectURL).Error; err != nil {
		return nil, err
	}
	return &objectURL, nil
}

// GetObjectURLByEncodedURLPath gets the objectURL by the encodedURLPath
func (r *repository) GetObjectURLByEncodedURLPath(ctx context.Context, encodedURLPath string) (*ObjectURLModel, error) {
	var objectURL ObjectURLModel
	if err := r.db.WithContext(ctx).Where(ObjectURLColumn.EncodedURLPath, encodedURLPath).First(&objectURL).Error; err != nil {
		return nil, err
	}
	return &objectURL, nil
}

// turn objectURL to pb.GetObjectURLResponse
func TurnObjectURLToResponse(objectURL *ObjectURLModel) *artifactpb.GetObjectURLResponse {
	response := &artifactpb.GetObjectURLResponse{
		ObjectUrl: &artifactpb.ObjectURL{
			Uid:            objectURL.UID.String(),
			NamespaceUid:   objectURL.NamespaceUID.String(),
			ObjectUid:      objectURL.ObjectUID.String(),
			UrlExpireAt:    timestamppb.New(objectURL.URLExpireAt),
			MinioUrlPath:   objectURL.MinioURLPath,
			EncodedUrlPath: objectURL.EncodedURLPath,
			Type:           objectURL.Type,
			CreateTime:     timestamppb.New(objectURL.CreateTime),
			UpdateTime:     timestamppb.New(objectURL.UpdateTime),
		},
	}
	if objectURL.DeleteTime != nil {
		response.ObjectUrl.DeleteTime = timestamppb.New(*objectURL.DeleteTime)
	}
	return response
}
