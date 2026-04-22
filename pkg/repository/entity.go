package repository

import (
	"context"

	"github.com/gofrs/uuid"
	"github.com/lib/pq"
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
	// ListEntitiesByFileUID returns every kb_entity row linked to the given
	// file through kb_entity_file. Ordered by name for deterministic
	// augmented-chunk text across backfill re-runs.
	ListEntitiesByFileUID(ctx context.Context, fileUID types.FileUIDType) ([]EntityRecord, error)
	// ListFileUIDsWithEntitiesByKB returns every file UID in the KB that
	// has at least one linked kb_entity row. Used by the alias backfill
	// workflow to page only over files the summary LLM has already been
	// applied to — freshly-ingested files produce their aliases inline so
	// they do not need the backfill pass.
	ListFileUIDsWithEntitiesByKB(ctx context.Context, kbUID types.KBUIDType) ([]types.FileUIDType, error)
}

// EntityRecord is the GORM model for the kb_entity table.
type EntityRecord struct {
	UID        uuid.UUID       `gorm:"column:uid;type:uuid;primaryKey;default:gen_random_uuid()"`
	KBUID      types.KBUIDType `gorm:"column:kb_uid;type:uuid;not null"`
	Name       string          `gorm:"column:name;type:text;not null"`
	EntityType string          `gorm:"column:entity_type;type:text"`
	// Aliases carries bilingual / synonym surface forms so that BM25 over the
	// augmented chunk (and future entity SQL rescue) can bridge scripts and
	// synonym variants. Always deduped + capped by the parser before it
	// reaches the repository.
	Aliases pq.StringArray `gorm:"column:aliases;type:text[];not null;default:'{}'"`
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

	// On conflict, keep the existing row but (a) refresh entity_type from the
	// incoming record, and (b) union-merge aliases so that re-ingestion
	// never shrinks an already-populated alias set. The DISTINCT unnest over
	// the concatenation (kb_entity.aliases || EXCLUDED.aliases) deduplicates
	// across both sources in SQL so the Go side never has to round-trip the
	// merge.
	result := r.db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "kb_uid"}, {Name: "name"}},
		DoUpdates: clause.Assignments(map[string]any{
			"entity_type": gorm.Expr("EXCLUDED.entity_type"),
			"aliases": gorm.Expr(
				"(SELECT ARRAY(SELECT DISTINCT UNNEST(kb_entity.aliases || EXCLUDED.aliases)))",
			),
		}),
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

// ListEntitiesByFileUID returns the entities linked to the given file,
// ordered by name. Selection is a simple inner join on kb_entity_file so that
// orphaned entities (none remaining after a DeleteEntitiesByFileUID) never
// surface here.
func (r *repository) ListEntitiesByFileUID(ctx context.Context, fileUID types.FileUIDType) ([]EntityRecord, error) {
	var rows []EntityRecord
	err := r.db.WithContext(ctx).
		Table("kb_entity e").
		Joins("JOIN kb_entity_file ef ON ef.entity_uid = e.uid").
		Where("ef.file_uid = ?", fileUID).
		Order("e.name ASC").
		Find(&rows).Error
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// ListFileUIDsWithEntitiesByKB returns every file UID inside the KB that has
// at least one kb_entity_file row. Used by BackfillEntityAliasesWorkflow to
// restrict the scan to files that have already gone through the summary LLM.
// Files with zero entities never had a TYPE_AUGMENTED chunk to begin with.
func (r *repository) ListFileUIDsWithEntitiesByKB(ctx context.Context, kbUID types.KBUIDType) ([]types.FileUIDType, error) {
	var rows []types.FileUIDType
	err := r.db.WithContext(ctx).Raw(`
		SELECT DISTINCT ef.file_uid
		FROM kb_entity_file ef
		JOIN kb_entity e ON e.uid = ef.entity_uid
		WHERE e.kb_uid = ?
	`, kbUID).Scan(&rows).Error
	if err != nil {
		return nil, err
	}
	return rows, nil
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
