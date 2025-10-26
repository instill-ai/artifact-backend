package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/instill-ai/artifact-backend/pkg/types"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

type Object interface {
	CreateObject(ctx context.Context, obj ObjectModel) (*ObjectModel, error)
	ListAllObjects(ctx context.Context, namespaceUID types.NamespaceUIDType, creatorUID types.CreatorUIDType) ([]ObjectModel, error)
	UpdateObject(ctx context.Context, obj ObjectModel) (*ObjectModel, error)
	DeleteObject(ctx context.Context, uid types.ObjectUIDType) error
	GetObjectByUID(ctx context.Context, uid types.ObjectUIDType) (*ObjectModel, error)
	UpdateObjectByUpdateMap(ctx context.Context, objUID types.ObjectUIDType, updateMap map[string]any) (*ObjectModel, error)
}

type ObjectModel struct {
	UID          types.ObjectUIDType    `gorm:"column:uid;type:uuid;default:gen_random_uuid();primaryKey" json:"uid"`
	Name         string                 `gorm:"column:name;size:1040" json:"name"`
	Size         int64                  `gorm:"column:size;" json:"size"`
	ContentType  string                 `gorm:"column:content_type;size:255" json:"content_type"`
	NamespaceUID types.NamespaceUIDType `gorm:"column:namespace_uid;type:uuid;not null" json:"namespace_uid"`
	CreatorUID   types.CreatorUIDType   `gorm:"column:creator_uid;type:uuid;not null" json:"creator_uid"`
	IsUploaded   bool                   `gorm:"column:is_uploaded;not null;default:false" json:"is_uploaded"`
	// BucketName/ns:<nid>/obj:<uid>
	Destination      string         `gorm:"column:destination;size:255" json:"destination"`
	ObjectExpireDays *int           `gorm:"column:object_expire_days" json:"object_expire_days"`
	LastModifiedTime *time.Time     `gorm:"column:last_modified_time" json:"last_modified_time"`
	CreateTime       time.Time      `gorm:"column:create_time;not null;default:CURRENT_TIMESTAMP" json:"create_time"`
	UpdateTime       time.Time      `gorm:"column:update_time;not null;autoUpdateTime" json:"update_time"`
	DeleteTime       gorm.DeletedAt `gorm:"column:delete_time;index" json:"delete_time"`
}

// TableName overrides the default table name for GORM
func (ObjectModel) TableName() string {
	return "object"
}

type ObjectColumns struct {
	UID              string
	Name             string
	Size             string
	ContentType      string
	NamespaceUID     string
	CreatorUID       string
	IsUploaded       string
	Destination      string
	ObjectExpireDays string
	LastModifiedTime string
	CreateTime       string
	UpdateTime       string
	DeleteTime       string
}

var ObjectColumn = ObjectColumns{
	UID:              "uid",
	Name:             "name",
	Size:             "size",
	ContentType:      "content_type",
	NamespaceUID:     "namespace_uid",
	CreatorUID:       "creator_uid",
	IsUploaded:       "is_uploaded",
	Destination:      "destination",
	ObjectExpireDays: "object_expire_days",
	LastModifiedTime: "last_modified_time",
	CreateTime:       "create_time",
	UpdateTime:       "update_time",
	DeleteTime:       "delete_time",
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

// GetObjectByUID fetches an ObjectModel record by its UID.
func (r *repository) GetObjectByUID(ctx context.Context, uid types.ObjectUIDType) (*ObjectModel, error) {
	var obj ObjectModel
	whereString := fmt.Sprintf("%v = ? AND %v IS NULL", ObjectColumn.UID, ObjectColumn.DeleteTime)
	if err := r.db.WithContext(ctx).Where(whereString, uid).First(&obj).Error; err != nil {
		return nil, err
	}
	return &obj, nil
}

// TurnObjectInDBToObjectInProto turns the object in db to the object in proto
func TurnObjectInDBToObjectInProto(obj *ObjectModel) *artifactpb.Object {
	protoObj := &artifactpb.Object{
		Uid:          obj.UID.String(),
		NamespaceUid: obj.NamespaceUID.String(),
		Name:         obj.Name,
		Size:         obj.Size,
		ContentType:  obj.ContentType,
		Creator:      obj.CreatorUID.String(),
		Path:         &obj.Destination,
		IsUploaded:   obj.IsUploaded,
		CreatedTime:  timestamppb.New(obj.CreateTime),
		UpdatedTime:  timestamppb.New(obj.UpdateTime),
	}
	if obj.LastModifiedTime != nil {
		protoObj.LastModifiedTime = timestamppb.New(*obj.LastModifiedTime)
	}
	if obj.ObjectExpireDays != nil {
		protoObj.ObjectExpireDays = int32(*obj.ObjectExpireDays)
	}
	return protoObj
}
