package repository

import (
	"context"

	"github.com/gofrs/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/instill-ai/artifact-backend/pkg/types"
)

// Entity defines repository operations for KB entities and their file links.
type Entity interface {
	UpsertEntities(ctx context.Context, kbUID types.KBUIDType, entities []EntityRecord) ([]uuid.UUID, error)
	LinkEntityFile(ctx context.Context, entityUID uuid.UUID, fileUID types.FileUIDType) error
	EntityHop(ctx context.Context, fileUIDs []types.FileUIDType) ([]types.FileUIDType, error)
	DeleteEntitiesByFileUID(ctx context.Context, fileUID types.FileUIDType) error
}

// EntityRecord is the GORM model for the kb_entity table.
type EntityRecord struct {
	UID        uuid.UUID      `gorm:"column:uid;type:uuid;primaryKey;default:gen_random_uuid()"`
	KBUID      types.KBUIDType `gorm:"column:kb_uid;type:uuid;not null"`
	Name       string         `gorm:"column:name;type:text;not null"`
	EntityType string         `gorm:"column:entity_type;type:text"`
}

func (EntityRecord) TableName() string { return "kb_entity" }

// EntityFileRecord is the GORM model for the kb_entity_file junction table.
type EntityFileRecord struct {
	EntityUID uuid.UUID         `gorm:"column:entity_uid;type:uuid;primaryKey"`
	FileUID   types.FileUIDType `gorm:"column:file_uid;type:uuid;primaryKey"`
}

func (EntityFileRecord) TableName() string { return "kb_entity_file" }

// UpsertEntities inserts or returns existing entities within a KB.
// Returns the UIDs in the same order as the input.
func (r *repository) UpsertEntities(_ context.Context, kbUID types.KBUIDType, entities []EntityRecord) ([]uuid.UUID, error) {
	if len(entities) == 0 {
		return nil, nil
	}

	uids := make([]uuid.UUID, len(entities))

	for i := range entities {
		entities[i].KBUID = kbUID
		if entities[i].UID.IsNil() {
			entities[i].UID = uuid.Must(uuid.NewV4())
		}
	}

	result := r.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "kb_uid"}, {Name: "name"}},
		DoUpdates: clause.AssignmentColumns([]string{"entity_type"}),
	}).Create(&entities)
	if result.Error != nil {
		return nil, result.Error
	}

	// Re-read UIDs since ON CONFLICT may have used existing rows.
	for i, e := range entities {
		var existing EntityRecord
		if err := r.db.Where("kb_uid = ? AND name = ?", kbUID, e.Name).First(&existing).Error; err != nil {
			return nil, err
		}
		uids[i] = existing.UID
	}

	return uids, nil
}

// LinkEntityFile creates an entity-file junction record. Idempotent.
func (r *repository) LinkEntityFile(_ context.Context, entityUID uuid.UUID, fileUID types.FileUIDType) error {
	rec := EntityFileRecord{EntityUID: entityUID, FileUID: fileUID}
	return r.db.Clauses(clause.OnConflict{DoNothing: true}).Create(&rec).Error
}

// EntityHop finds file UIDs that share entities with the given file UIDs
// but are not in the input set.
func (r *repository) EntityHop(_ context.Context, fileUIDs []types.FileUIDType) ([]types.FileUIDType, error) {
	if len(fileUIDs) == 0 {
		return nil, nil
	}

	var result []types.FileUIDType
	err := r.db.Raw(`
		SELECT DISTINCT ef2.file_uid
		FROM kb_entity_file ef1
		JOIN kb_entity_file ef2 ON ef1.entity_uid = ef2.entity_uid
		WHERE ef1.file_uid IN ? AND ef2.file_uid NOT IN ?
	`, fileUIDs, fileUIDs).Scan(&result).Error
	if err != nil {
		return nil, err
	}
	return result, nil
}

// DeleteEntitiesByFileUID removes all entity-file links for a file and
// cleans up any orphaned entities (entities with no remaining file links).
func (r *repository) DeleteEntitiesByFileUID(_ context.Context, fileUID types.FileUIDType) error {
	return r.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("file_uid = ?", fileUID).Delete(&EntityFileRecord{}).Error; err != nil {
			return err
		}
		// Remove orphaned entities.
		return tx.Exec(`
			DELETE FROM kb_entity WHERE uid NOT IN (
				SELECT DISTINCT entity_uid FROM kb_entity_file
			)
		`).Error
	})
}
