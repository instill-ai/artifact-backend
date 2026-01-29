package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/gofrs/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"

	"github.com/instill-ai/artifact-backend/pkg/types"
	"github.com/instill-ai/artifact-backend/pkg/utils"

	artifactpb "github.com/instill-ai/protogen-go/artifact/v1alpha"
)

// Object interface defines database operations for objects
type Object interface {
	CreateObject(ctx context.Context, obj ObjectModel) (*ObjectModel, error)
	ListAllObjects(ctx context.Context, namespaceUID types.NamespaceUIDType, creatorUID types.CreatorUIDType) ([]ObjectModel, error)
	UpdateObject(ctx context.Context, obj ObjectModel) (*ObjectModel, error)
	DeleteObjectByStoragePath(ctx context.Context, storagePath string) error
	DeleteObject(ctx context.Context, uid types.ObjectUIDType) error
	GetObjectByUID(ctx context.Context, uid types.ObjectUIDType) (*ObjectModel, error)
	GetObjectByID(ctx context.Context, namespaceUID types.NamespaceUIDType, id types.ObjectIDType) (*ObjectModel, error)
	UpdateObjectByUpdateMap(ctx context.Context, objUID types.ObjectUIDType, updateMap map[string]any) (*ObjectModel, error)
}

// ObjectModel represents an object in the database
// Field ordering follows AIP standard: name (derived), id, display_name, slug, aliases, etc.
type ObjectModel struct {
	UID          types.ObjectUIDType    `gorm:"column:uid;type:uuid;default:gen_random_uuid();primaryKey" json:"uid"`
	ID           types.ObjectIDType     `gorm:"column:id;size:255;not null" json:"id"`
	DisplayName  string                 `gorm:"column:display_name;size:1040" json:"display_name"`
	// Slug is a URL-friendly identifier derived from display_name
	Slug string `gorm:"column:slug;size:255" json:"slug"`
	// Aliases stores previous slugs for backward compatibility when display_name changes
	Aliases      AliasesArray           `gorm:"column:aliases;type:text[]" json:"aliases"`
	NamespaceUID types.NamespaceUIDType `gorm:"column:namespace_uid;type:uuid;not null" json:"namespace_uid"`
	CreatorUID   types.CreatorUIDType   `gorm:"column:creator_uid;type:uuid;not null" json:"creator_uid"`
	CreateTime   time.Time              `gorm:"column:create_time;not null;default:CURRENT_TIMESTAMP" json:"create_time"`
	UpdateTime   time.Time              `gorm:"column:update_time;not null;autoUpdateTime" json:"update_time"`
	DeleteTime   gorm.DeletedAt         `gorm:"column:delete_time;index" json:"delete_time"`
	Size         int64                  `gorm:"column:size" json:"size"`
	ContentType  string                 `gorm:"column:content_type;size:255" json:"content_type"`
	IsUploaded   bool                   `gorm:"column:is_uploaded;not null;default:false" json:"is_uploaded"`
	// StoragePath is the path in blob storage (e.g., "ns-{nsUID}/obj-{objUID}")
	StoragePath      string     `gorm:"column:storage_path;size:255" json:"storage_path"`
	ObjectExpireDays *int       `gorm:"column:object_expire_days" json:"object_expire_days"`
	LastModifiedTime *time.Time `gorm:"column:last_modified_time" json:"last_modified_time"`
}

// TableName overrides the default table name for GORM
func (ObjectModel) TableName() string {
	return "object"
}

// BeforeCreate is a GORM hook that generates UID and ID if not provided (AIP standard)
func (obj *ObjectModel) BeforeCreate(tx *gorm.DB) error {
	// Generate UID if not provided (required before generating ID)
	if uuid.UUID(obj.UID) == uuid.Nil {
		obj.UID = types.ObjectUIDType(uuid.Must(uuid.NewV4()))
		tx.Statement.SetColumn("UID", obj.UID)
	}
	// Generate prefixed canonical ID if not provided
	if obj.ID == "" {
		obj.ID = types.ObjectIDType(utils.GeneratePrefixedResourceID(utils.PrefixObject, uuid.UUID(obj.UID)))
		tx.Statement.SetColumn("ID", obj.ID)
	}
	return nil
}

// ObjectColumns defines column names for the object table
type ObjectColumns struct {
	UID              string
	ID               string
	DisplayName      string
	Slug             string
	Aliases          string
	NamespaceUID     string
	CreatorUID       string
	CreateTime       string
	UpdateTime       string
	DeleteTime       string
	Size             string
	ContentType      string
	IsUploaded       string
	StoragePath      string
	ObjectExpireDays string
	LastModifiedTime string
}

// ObjectColumn contains all column names for the object table
var ObjectColumn = ObjectColumns{
	UID:              "uid",
	ID:               "id",
	DisplayName:      "display_name",
	Slug:             "slug",
	Aliases:          "aliases",
	NamespaceUID:     "namespace_uid",
	CreatorUID:       "creator_uid",
	CreateTime:       "create_time",
	UpdateTime:       "update_time",
	DeleteTime:       "delete_time",
	Size:             "size",
	ContentType:      "content_type",
	IsUploaded:       "is_uploaded",
	StoragePath:      "storage_path",
	ObjectExpireDays: "object_expire_days",
	LastModifiedTime: "last_modified_time",
}

// CreateObject inserts a new ObjectModel record into the database.
func (r *repository) CreateObject(ctx context.Context, obj ObjectModel) (*ObjectModel, error) {
	if err := r.db.WithContext(ctx).Create(&obj).Error; err != nil {
		return nil, err
	}
	return &obj, nil
}

// ListAllObjects fetches all ObjectModel records from the database for a given namespace and creator, excluding soft-deleted ones.
func (r *repository) ListAllObjects(ctx context.Context, namespaceUID types.NamespaceUIDType, creatorUID types.CreatorUIDType) ([]ObjectModel, error) {
	var objects []ObjectModel
	// GORM's DeletedAt automatically filters out soft-deleted records
	whereString := fmt.Sprintf("%v = ? AND %v = ?", ObjectColumn.NamespaceUID, ObjectColumn.CreatorUID)
	if err := r.db.WithContext(ctx).Where(whereString, namespaceUID, creatorUID).Find(&objects).Error; err != nil {
		return nil, err
	}
	return objects, nil
}

// UpdateObject updates an ObjectModel record in the database.
func (r *repository) UpdateObject(ctx context.Context, obj ObjectModel) (*ObjectModel, error) {
	if err := r.db.WithContext(ctx).Save(&obj).Error; err != nil {
		return nil, err
	}
	return &obj, nil
}

// UpdateObjectByUpdateMap updates an ObjectModel record in the database.
func (r *repository) UpdateObjectByUpdateMap(ctx context.Context, objUID types.ObjectUIDType, updateMap map[string]any) (*ObjectModel, error) {
	var obj ObjectModel
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&ObjectModel{}).Where(ObjectColumn.UID, objUID).Updates(updateMap).Error; err != nil {
			return err
		}
		if err := tx.Where(ObjectColumn.UID, objUID).First(&obj).Error; err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &obj, nil
}

// DeleteObject performs a soft delete on an ObjectModel record.
func (r *repository) DeleteObject(ctx context.Context, uid types.ObjectUIDType) error {
	deleteTime := time.Now().UTC()
	whereString := fmt.Sprintf("%v = ? AND %v IS NULL", ObjectColumn.UID, ObjectColumn.DeleteTime)
	if err := r.db.WithContext(ctx).Model(&ObjectModel{}).Where(whereString, uid).Update(ObjectColumn.DeleteTime, deleteTime).Error; err != nil {
		return err
	}
	return nil
}

// DeleteObjectByStoragePath soft deletes an object by its storage path
// This is used when cleaning up blob objects after file deletion
func (r *repository) DeleteObjectByStoragePath(ctx context.Context, storagePath string) error {
	deleteTime := time.Now().UTC()
	whereString := fmt.Sprintf("%v = ? AND %v IS NULL", ObjectColumn.StoragePath, ObjectColumn.DeleteTime)
	if err := r.db.WithContext(ctx).Model(&ObjectModel{}).Where(whereString, storagePath).Update(ObjectColumn.DeleteTime, deleteTime).Error; err != nil {
		return err
	}
	return nil
}

// GetObjectByUID fetches an ObjectModel record by its UID.
func (r *repository) GetObjectByUID(ctx context.Context, uid types.ObjectUIDType) (*ObjectModel, error) {
	var obj ObjectModel
	whereString := fmt.Sprintf("%v = ? AND %v IS NULL", ObjectColumn.UID, ObjectColumn.DeleteTime)
	if err := r.db.WithContext(ctx).Where(whereString, uid).First(&obj).Error; err != nil {
		return nil, err
	}
	return &obj, nil
}

// GetObjectByID fetches an ObjectModel record by its ID within a namespace.
func (r *repository) GetObjectByID(ctx context.Context, namespaceUID types.NamespaceUIDType, id types.ObjectIDType) (*ObjectModel, error) {
	var obj ObjectModel
	whereString := fmt.Sprintf("%v = ? AND %v = ? AND %v IS NULL", ObjectColumn.NamespaceUID, ObjectColumn.ID, ObjectColumn.DeleteTime)
	if err := r.db.WithContext(ctx).Where(whereString, namespaceUID, id).First(&obj).Error; err != nil {
		return nil, err
	}
	return &obj, nil
}

// TurnObjectInDBToObjectInProto turns the object in db to the object in proto
func TurnObjectInDBToObjectInProto(obj *ObjectModel) *artifactpb.Object {
	// Build canonical resource name following AIP format
	name := fmt.Sprintf("namespaces/%s/objects/%s", obj.NamespaceUID.String(), string(obj.ID))

	protoObj := &artifactpb.Object{
		Name:             name,
		Id:               string(obj.ID),
		DisplayName:      obj.DisplayName,
		OwnerName:        fmt.Sprintf("namespaces/%s", obj.NamespaceUID.String()),
		CreatorName:      fmt.Sprintf("users/%s", obj.CreatorUID.String()),
		CreateTime:       timestamppb.New(obj.CreateTime),
		UpdateTime:       timestamppb.New(obj.UpdateTime),
		Size:             obj.Size,
		ContentType:      obj.ContentType,
		IsUploaded:       obj.IsUploaded,
		ObjectExpireDays: 0,
	}
	if obj.LastModifiedTime != nil {
		protoObj.LastModifiedTime = timestamppb.New(*obj.LastModifiedTime)
	}
	if obj.ObjectExpireDays != nil {
		protoObj.ObjectExpireDays = int32(*obj.ObjectExpireDays)
	}
	if obj.DeleteTime.Valid {
		protoObj.DeleteTime = timestamppb.New(obj.DeleteTime.Time)
	}
	return protoObj
}
