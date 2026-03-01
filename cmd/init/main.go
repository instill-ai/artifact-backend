package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
	"gorm.io/gorm"

	grpczap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/ai"
	"github.com/instill-ai/artifact-backend/pkg/ai/gemini"
	"github.com/instill-ai/artifact-backend/pkg/ai/openai"
	"github.com/instill-ai/artifact-backend/pkg/pipeline"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/repository/object"

	database "github.com/instill-ai/artifact-backend/pkg/db"
	pipelinepb "github.com/instill-ai/protogen-go/pipeline/v1beta"
	clientx "github.com/instill-ai/x/client"
	clientgrpcx "github.com/instill-ai/x/client/grpc"
	logx "github.com/instill-ai/x/log"
	miniox "github.com/instill-ai/x/minio"
)

// DefaultSystemPresets defines the default system configurations to seed
var DefaultSystemPresets = []repository.PresetSystem{
	{
		DisplayName:    "OpenAI",
		Slug:           "openai",
		ModelFamily:    ai.ModelFamilyOpenAI,
		Dimensionality: openai.DefaultEmbeddingDimension,
		Description:    "OpenAI embedding configuration (text-embedding-3-small, 1536 dimensions)",
		IsDefault:      false,
	},
	{
		DisplayName:    "Gemini",
		Slug:           "gemini",
		ModelFamily:    ai.ModelFamilyGemini,
		Dimensionality: gemini.DefaultEmbeddingDimension,
		Description:    "Gemini embedding configuration (text-embedding-004, 3072 dimensions)",
		IsDefault:      true, // Gemini is the default
	},
}

func main() {
	ctx := context.Background()

	if err := config.Init(config.ParseConfigFlag()); err != nil {
		log.Fatal(err.Error())
	}

	logx.Debug = config.Config.Server.Debug
	logger, _ := logx.GetZapLogger(context.Background())
	defer func() {
		// can't handle the error due to https://github.com/uber-go/zap/issues/880
		_ = logger.Sync()
	}()

	// Set gRPC logging based on debug mode
	if config.Config.Server.Debug {
		grpczap.ReplaceGrpcLoggerV2WithVerbosity(logger, 0) // All logs
	} else {
		grpczap.ReplaceGrpcLoggerV2WithVerbosity(logger, 3) // verbosity 3 will avoid [transport] from emitting
	}

	// Initialize database connection for system seeding
	db := database.GetSharedConnection()
	defer database.Close(db)

	// Seed default systems
	logger.Info("Seeding default system configurations...")
	repo := repository.NewDBOnlyRepository(db)
	if err := repo.SeedDefaultSystems(ctx, DefaultSystemPresets); err != nil {
		logger.Fatal(fmt.Sprintf("failed to seed default systems: %v", err))
	}
	logger.Info("Default system configurations seeded successfully")

	// Initialize pipeline client for preset pipelines
	pipelinePublicServiceClient, pipelinePublicClose, err := clientgrpcx.NewClient[pipelinepb.PipelinePublicServiceClient](
		clientgrpcx.WithServiceConfig(clientx.ServiceConfig{
			Host:       config.Config.PipelineBackend.Host,
			PublicPort: config.Config.PipelineBackend.PublicPort,
		}),
		clientgrpcx.WithSetOTELClientHandler(config.Config.OTELCollector.Enable),
	)
	if err != nil {
		logger.Fatal("failed to create pipeline public service client", zap.Error(err))
	}
	defer func() {
		if err := pipelinePublicClose(); err != nil {
			logger.Error("failed to close pipeline public service client", zap.Error(err))
		}
	}()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "Instill-Service", "instill")

	upserter := &pipeline.ReleaseUpserter{
		FS:                          pipeline.PresetPipelinesFS,
		PipelinePublicServiceClient: pipelinePublicServiceClient,
	}

	for _, pr := range pipeline.PresetPipelinesList {
		logger := logger.With(zap.String("slug", pr.Slug()), zap.String("displayName", pr.DisplayName), zap.String("version", pr.Version))
		if err := upserter.Upsert(ctx, pr); err != nil {
			logger.Error("Failed to add pipeline", zap.Error(err))
			continue
		}

		logger.Info("Processed pipeline")
	}

	// Backfill content_sha256 for existing files that predate the dedup feature.
	// Uses a separate context with generous timeout since this reads from MinIO.
	backfillCtx, backfillCancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer backfillCancel()
	if err := backfillContentSHA256(backfillCtx, db, logger); err != nil {
		logger.Error("SHA256 backfill failed (non-fatal)", zap.Error(err))
	}
}

const backfillBatchSize = 50

func backfillContentSHA256(ctx context.Context, db *gorm.DB, logger *zap.Logger) error {
	var count int64
	if err := db.WithContext(ctx).Table("file").
		Where("content_sha256 IS NULL OR content_sha256 = ''").
		Where("delete_time IS NULL").
		Where("storage_path IS NOT NULL AND storage_path != ''").
		Count(&count).Error; err != nil {
		return fmt.Errorf("counting files needing backfill: %w", err)
	}

	if count == 0 {
		logger.Info("SHA256 backfill: all files already have content_sha256")
		return nil
	}

	logger.Info("SHA256 backfill: starting", zap.Int64("files_to_process", count))

	minioClient, err := object.NewMinIOStorage(ctx, miniox.ClientParams{
		Config: config.Config.Minio,
		Logger: logger,
		AppInfo: miniox.AppInfo{
			Name:    "artifact-backend",
			Version: "init",
		},
	})
	if err != nil {
		return fmt.Errorf("initializing MinIO client for backfill: %w", err)
	}

	var processed, skipped int
	failedUIDs := make(map[string]bool)

	for {
		var files []struct {
			UID         string `gorm:"column:uid"`
			StoragePath string `gorm:"column:storage_path"`
		}

		q := db.WithContext(ctx).Table("file").
			Select("uid, storage_path").
			Where("content_sha256 IS NULL OR content_sha256 = ''").
			Where("delete_time IS NULL").
			Where("storage_path IS NOT NULL AND storage_path != ''")

		if len(failedUIDs) > 0 {
			excludeUIDs := make([]string, 0, len(failedUIDs))
			for uid := range failedUIDs {
				excludeUIDs = append(excludeUIDs, uid)
			}
			q = q.Where("uid NOT IN ?", excludeUIDs)
		}

		if err := q.Limit(backfillBatchSize).Find(&files).Error; err != nil {
			return fmt.Errorf("querying files for backfill: %w", err)
		}

		if len(files) == 0 {
			break
		}

		for _, f := range files {
			fileBytes, err := minioClient.GetFile(ctx, object.BlobBucketName, f.StoragePath)
			if err != nil || len(fileBytes) == 0 {
				logger.Warn("SHA256 backfill: skipping file (cannot read from MinIO)",
					zap.String("uid", f.UID), zap.Error(err))
				failedUIDs[f.UID] = true
				skipped++
				continue
			}

			hash := sha256.Sum256(fileBytes)
			hashHex := hex.EncodeToString(hash[:])

			if err := db.WithContext(ctx).Table("file").Where("uid = ?", f.UID).
				Update("content_sha256", hashHex).Error; err != nil {
				logger.Warn("SHA256 backfill: failed to update file",
					zap.String("uid", f.UID), zap.Error(err))
				failedUIDs[f.UID] = true
				skipped++
				continue
			}
			processed++
		}
	}

	logger.Info("SHA256 backfill: completed",
		zap.Int("processed", processed),
		zap.Int("skipped", skipped))
	return nil
}
