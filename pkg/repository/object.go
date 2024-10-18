package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/gofrs/uuid"
	"gorm.io/gorm"
)

type ObjectI interface {
	CreateObject(ctx context.Context, obj Object) (*Object, error)
	ListAllObjects(ctx context.Context, namespaceUID, creatorUID uuid.UUID) ([]Object, error)
	UpdateObject(ctx context.Context, obj Object) (*Object, error)
	DeleteObject(ctx context.Context, uid uuid.UUID) error
	GetObjectByUID(ctx context.Context, uid uuid.UUID) (*Object, error)
}

type Object struct {
	UID          uuid.UUID `gorm:"column:uid;type:uuid;default:gen_random_uuid();primaryKey" json:"uid"`
	Name         string    `gorm:"column:name;size:1040" json:"name"`
	Size             int64      `gorm:"column:size;" json:"size"`
	ContentType      string    `gorm:"column:content_type;size:255" json:"content_type"`
	NamespaceUID     uuid.UUID `gorm:"column:namespace_uid;type:uuid;not null" json:"namespace_uid"`
	CreatorUID       uuid.UUID `gorm:"column:creator_uid;type:uuid;not null" json:"creator_uid"`
	IsUploaded       bool      `gorm:"column:is_uploaded;not null;default:false" json:"is_uploaded"`
	// BucketName/ns:<nid>/obj:<uid>
	Destination      string     `gorm:"column:destination;size:255" json:"destination"`
	ObjectExpireDays *int       `gorm:"column:object_expire_days" json:"object_expire_days"`
	LastModifiedTime *time.Time `gorm:"column:last_modified_time" json:"last_modified_time"`
	CreateTime       time.Time  `gorm:"column:create_time;not null;default:CURRENT_TIMESTAMP" json:"create_time"`
	UpdateTime       time.Time  `gorm:"column:update_time;not null;autoUpdateTime" json:"update_time"`
	DeleteTime       *time.Time `gorm:"column:delete_time" json:"delete_time"`
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

// CreateObject inserts a new Object record into the database.
func (r *Repository) CreateObject(ctx context.Context, obj Object) (*Object, error) {
	if err := r.db.WithContext(ctx).Create(&obj).Error; err != nil {
		return nil, err
	}
	return &obj, nil
}

// ListAllObjects fetches all Object records from the database for a given namespace and creator, excluding soft-deleted ones.
func (r *Repository) ListAllObjects(ctx context.Context, namespaceUID, creatorUID uuid.UUID) ([]Object, error) {
	var objects []Object
	whereString := fmt.Sprintf("%v IS NULL AND %v = ? AND %v = ?", ObjectColumn.DeleteTime, ObjectColumn.NamespaceUID, ObjectColumn.CreatorUID)
	if err := r.db.WithContext(ctx).Where(whereString, namespaceUID, creatorUID).Find(&objects).Error; err != nil {
		return nil, err
	}
	return objects, nil
}

// UpdateObject updates an Object record in the database.
func (r *Repository) UpdateObject(ctx context.Context, obj Object) (*Object, error) {
	if err := r.db.WithContext(ctx).Save(&obj).Error; err != nil {
		return nil, err
	}
	return &obj, nil
}

// UpdateObjectByUpdateMap updates an Object record in the database.
func (r *Repository) UpdateObjectByUpdateMap(ctx context.Context, objUID uuid.UUID, updateMap map[string]any) (*Object, error) {
	var obj Object
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&Object{}).Where(ObjectColumn.UID, objUID).Updates(updateMap).Error; err != nil {
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

// DeleteObject performs a soft delete on an Object record.
func (r *Repository) DeleteObject(ctx context.Context, uid uuid.UUID) error {
	deleteTime := time.Now().UTC()
	whereString := fmt.Sprintf("%v = ? AND %v IS NULL", ObjectColumn.UID, ObjectColumn.DeleteTime)
	if err := r.db.WithContext(ctx).Model(&Object{}).Where(whereString, uid).Update(ObjectColumn.DeleteTime, deleteTime).Error; err != nil {
		return err
	}
	return nil
}

// GetObjectByUID fetches an Object record by its UID.
func (r *Repository) GetObjectByUID(ctx context.Context, uid uuid.UUID) (*Object, error) {
	var obj Object
	whereString := fmt.Sprintf("%v = ? AND %v IS NULL", ObjectColumn.UID, ObjectColumn.DeleteTime)
	if err := r.db.WithContext(ctx).Where(whereString, uid).First(&obj).Error; err != nil {
		return nil, err
	}
	return &obj, nil
}
