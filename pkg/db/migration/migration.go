package migration

import (
	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/instill-ai/artifact-backend/pkg/db/migration/convert"
	"github.com/instill-ai/artifact-backend/pkg/db/migration/convert/convert000025"
)

// TargetSchemaVersion determines the database schema version.
const TargetSchemaVersion uint = 27

type migration interface {
	Migrate() error
}

// CodeMigrator orchestrates the execution of the code associated with the
// different database migrations and holds their dependencies.
type CodeMigrator struct {
	Logger *zap.Logger

	DB *gorm.DB
}

// Migrate executes custom code as part of a database migration. This code is
// intended to be run only once and typically goes along a change
// in the database schemas. Some use cases might be backfilling a table or
// updating some existing records according to the schema changes.
//
// Note that the changes in the database schemas shouldn't be run here, only
// code accompanying them.
func (cm *CodeMigrator) Migrate(version uint) error {
	var m migration

	bc := convert.Basic{
		DB:     cm.DB,
		Logger: cm.Logger,
	}

	switch version {
	case 25:
		m = &convert000025.BumpConversionPipeline{Basic: bc}
	default:
		return nil
	}

	return m.Migrate()
}
