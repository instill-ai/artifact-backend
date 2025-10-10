package convert000025

import (
	"fmt"

	"gorm.io/gorm"

	"github.com/instill-ai/artifact-backend/pkg/db/migration/convert"
	"github.com/instill-ai/artifact-backend/pkg/repository"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

const batchSize = 100

type BumpConversionPipeline struct {
	convert.Basic
}

const (
	oldConvertingPipe = "preset/indexing-advanced-convert-doc@v1.3.1"
	newConvertingPipe = "preset/indexing-advanced-convert-doc@v1.3.2"
)

func (c *BumpConversionPipeline) Migrate() error {
	files := make([]*repository.KnowledgeBaseFileModel, 0, batchSize)
	q := c.DB.Select("uid").
		Where("process_status = ?", artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_FAILED.String()).
		Where("extra_meta_data->>'converting_pipe' = ?", oldConvertingPipe)

	return q.FindInBatches(&files, batchSize, func(tx *gorm.DB, _ int) error {
		for _, f := range files {
			// Update the process status of the files
			updates := map[string]any{
				// Clear previous failure reason
				"extra_meta_data": gorm.Expr(
					"COALESCE(extra_meta_data, '{}'::jsonb) || ?::jsonb",
					fmt.Sprintf(`{"converting_pipe": "%s"}`, newConvertingPipe),
				),
			}

			if err := tx.Model(f).Where("uid = ?", f.UID).Updates(updates).Error; err != nil {
				return fmt.Errorf("updating record %s: %w", f.UID.String(), err)
			}
		}

		return nil
	}).Error
}
