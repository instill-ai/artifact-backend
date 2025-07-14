package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/gofrs/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

// TODO: the ObjectURL will be fully removed in the future, we will use the
// presigned URL instead.
type ObjectURLI interface {
	ListAllObjectURLs(ctx context.Context, namespaceUID, objectUID uuid.UUID) ([]ObjectURL, error)
	DeleteObjectURL(ctx context.Context, uid uuid.UUID) error
	GetObjectURLByUID(ctx context.Context, uid uuid.UUID) (*ObjectURL, error)
	GetObjectURLCountByObject(ctx context.Context, objectUID uuid.UUID) (int64, error)
	GetObjectUploadURL(ctx context.Context, objectUID uuid.UUID) (*ObjectURL, error)
	GetObjectDownloadURL(ctx context.Context, objectUID uuid.UUID) (*ObjectURL, error)
	GetObjectURLByEncodedURLPath(ctx context.Context, encodedURLPath string) (*ObjectURL, error)
}

// ObjectURL represents the object_url table in the database.
// Note: due to ObjectURL will be translated to object_uRL in the database, we use ObjectURL instead of objectURL
type ObjectURL struct {
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
func (ObjectURL) TableName() string {
	return "object_url"
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

// ListAllObjectURLs fetches all ObjectURL records from the database for a given namespace and object, excluding soft-deleted ones.
func (r *Repository) ListAllObjectURLs(ctx context.Context, namespaceUID, objectUID uuid.UUID) ([]ObjectURL, error) {
	var objectURLs []ObjectURL
	whereString := fmt.Sprintf("%v IS NULL AND %v = ? AND %v = ?", ObjectURLColumn.DeleteTime, ObjectURLColumn.NamespaceUID, ObjectURLColumn.ObjectUID)
	if err := r.db.WithContext(ctx).Where(whereString, namespaceUID, objectUID).Find(&objectURLs).Error; err != nil {
		return nil, err
	}
	return objectURLs, nil
}

// CreateObjectURLWithUIDInEncodedURLPath creates an ObjectURL record in the database with the UID in the encoded_url_path.
func (r *Repository) CreateObjectURLWithUIDInEncodedURLPath(ctx context.Context, objectURL ObjectURL, namespaceID string, EncodedMinioURLPath func(namespaceID string, objectURLUUID uuid.UUID) string) (*ObjectURL, error) {
	var result ObjectURL
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Create the initial object URL
		if err := tx.Create(&objectURL).Error; err != nil {
			return err
		}

		// Update the encoded_url_path to include the UID
		updateMap := map[string]any{
			ObjectURLColumn.EncodedURLPath: EncodedMinioURLPath(namespaceID, objectURL.UID),
		}

		if err := tx.Model(&objectURL).Updates(updateMap).Error; err != nil {
			return err
		}

		// Fetch the final result
		if err := tx.Where(ObjectURLColumn.UID, objectURL.UID).First(&result).Error; err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &result, nil
}

// DeleteObjectURL performs a soft delete on an ObjectURL record.
func (r *Repository) DeleteObjectURL(ctx context.Context, uid uuid.UUID) error {
	deleteTime := time.Now().UTC()
	whereString := fmt.Sprintf("%v = ? AND %v IS NULL", ObjectURLColumn.UID, ObjectURLColumn.DeleteTime)
	if err := r.db.WithContext(ctx).Where(whereString, uid).Update(ObjectURLColumn.DeleteTime, deleteTime).Error; err != nil {
		return err
	}
	return nil
}

// GetObjectURLByUID fetches an ObjectURL record by its UID.
func (r *Repository) GetObjectURLByUID(ctx context.Context, uid uuid.UUID) (*ObjectURL, error) {
	var objectURL ObjectURL
	whereString := fmt.Sprintf("%v = ? AND %v IS NULL", ObjectURLColumn.UID, ObjectURLColumn.DeleteTime)
	if err := r.db.WithContext(ctx).Where(whereString, uid).First(&objectURL).Error; err != nil {
		return nil, err
	}
	return &objectURL, nil
}

// GetObjectURLCountByObject gets the count of ObjectURLs for a specific Object
func (r *Repository) GetObjectURLCountByObject(ctx context.Context, objectUID uuid.UUID) (int64, error) {
	var count int64
	whereString := fmt.Sprintf("%v = ? AND %v IS NULL", ObjectURLColumn.ObjectUID, ObjectURLColumn.DeleteTime)
	if err := r.db.WithContext(ctx).Where(whereString, objectUID).Count(&count).Error; err != nil {
		return 0, err
	}
	return count, nil
}

// GetObjectUploadURL gets the upload url for a specific Object
func (r *Repository) GetObjectUploadURL(ctx context.Context, objectUID uuid.UUID) (*ObjectURL, error) {
	var objectURL ObjectURL
	whereString := fmt.Sprintf("%v = ? AND %v = ? AND %v IS NULL", ObjectURLColumn.ObjectUID, ObjectURLColumn.Type, ObjectURLColumn.DeleteTime)
	if err := r.db.WithContext(ctx).Where(whereString, objectUID, ObjectURLTypeUpload).First(&objectURL).Error; err != nil {
		return nil, err
	}
	return &objectURL, nil
}

// GetObjectDownloadURL gets the download url for a specific Object
func (r *Repository) GetObjectDownloadURL(ctx context.Context, objectUID uuid.UUID) (*ObjectURL, error) {
	var objectURL ObjectURL
	whereString := fmt.Sprintf("%v = ? AND %v = ? AND %v IS NULL", ObjectURLColumn.ObjectUID, ObjectURLColumn.Type, ObjectURLColumn.DeleteTime)
	if err := r.db.WithContext(ctx).Where(whereString, objectUID, ObjectURLTypeDownload).First(&objectURL).Error; err != nil {
		return nil, err
	}
	return &objectURL, nil
}

// GetObjectURLByEncodedURLPath gets the objectURL by the encodedURLPath
func (r *Repository) GetObjectURLByEncodedURLPath(ctx context.Context, encodedURLPath string) (*ObjectURL, error) {
	var objectURL ObjectURL
	if err := r.db.WithContext(ctx).Where(ObjectURLColumn.EncodedURLPath, encodedURLPath).First(&objectURL).Error; err != nil {
		return nil, err
	}
	return &objectURL, nil
}

// turn objectURL to pb.GetObjectURLResponse
func TurnObjectURLToResponse(objectURL *ObjectURL) *artifactpb.GetObjectURLResponse {
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
