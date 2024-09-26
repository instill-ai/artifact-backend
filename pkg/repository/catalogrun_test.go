//go:build dbtest
// +build dbtest

package repository_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/require"
	"gopkg.in/guregu/null.v4"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"

	qt "github.com/frankban/quicktest"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/repository"

	database "github.com/instill-ai/artifact-backend/pkg/db"

	pb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	runpb "github.com/instill-ai/protogen-go/common/run/v1alpha"
)

var db *gorm.DB

func TestMain(m *testing.M) {
	databaseConfig := config.DatabaseConfig{
		Username: "postgres",
		Host:     "localhost",
		Port:     5432,
		Name:     "artifact",
		TimeZone: "Etc/UTC",
	}

	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%d sslmode=disable TimeZone=%s",
		databaseConfig.Host,
		databaseConfig.Username,
		databaseConfig.Password,
		databaseConfig.Name,
		databaseConfig.Port,
		databaseConfig.TimeZone,
	)

	var err error
	db, err = gorm.Open(postgres.New(postgres.Config{
		DSN:                  dsn,
		PreferSimpleProtocol: true, // disables implicit prepared statement usage
	}), &gorm.Config{
		QueryFields: true, // QueryFields mode will select by all fields’ name for current model
		NamingStrategy: schema.NamingStrategy{
			SingularTable: true,
		},
	})
	db.Logger = logger.Default.LogMode(logger.Info)

	if err != nil {
		panic(err.Error())
	}

	defer database.Close(db)

	os.Exit(m.Run())
}

func TestRepository_CreateCatalogRun(t *testing.T) {
	c := qt.New(t)
	tx := db.Begin()
	c.Cleanup(func() { tx.Rollback() })

	repo := repository.NewRepository(tx)
	run := &repository.CatalogRun{
		CatalogUID:    uuid.Must(uuid.NewV4()),
		Status:        repository.RunStatus(runpb.RunStatus_RUN_STATUS_PROCESSING),
		Source:        repository.RunSource(runpb.RunSource_RUN_SOURCE_API),
		Action:        repository.RunAction(pb.CatalogRunAction_CATALOG_RUN_ACTION_CREATE),
		RunnerUID:     uuid.Must(uuid.NewV4()),
		RequesterUID:  uuid.Must(uuid.NewV4()),
		StartedTime:   time.Now().Add(-10 * time.Minute),
		CompletedTime: null.TimeFrom(time.Now()),
		FileUIDs:      []string{uuid.Must(uuid.NewV4()).String()},
	}
	run.TotalDuration = null.IntFrom(run.CompletedTime.Time.Sub(run.CreateTime).Milliseconds())

	ctx := context.Background()
	created, err := repo.CreateCatalogRun(ctx, run)
	require.NoError(t, err)

	require.False(t, created.UID.IsNil())
	require.Equal(t, created.CatalogUID, run.CatalogUID)
	require.Equal(t, created.Status, run.Status)
	require.Equal(t, created.Source, run.Source)
	require.Equal(t, created.FileUIDs, run.FileUIDs)
	require.LessOrEqual(t, time.Since(created.CreateTime).Milliseconds(), int64(1000))
	require.LessOrEqual(t, time.Since(created.UpdateTime).Milliseconds(), int64(1000))
}

func TestRepository_UpdateCatalogRun(t *testing.T) {
	c := qt.New(t)
	tx := db.Begin()
	c.Cleanup(func() { tx.Rollback() })

	repo := repository.NewRepository(tx)
	run := &repository.CatalogRun{
		CatalogUID:   uuid.Must(uuid.NewV4()),
		Status:       repository.RunStatus(runpb.RunStatus_RUN_STATUS_PROCESSING),
		Source:       repository.RunSource(runpb.RunSource_RUN_SOURCE_API),
		Action:       repository.RunAction(pb.CatalogRunAction_CATALOG_RUN_ACTION_CREATE),
		RunnerUID:    uuid.Must(uuid.NewV4()),
		RequesterUID: uuid.Must(uuid.NewV4()),
		StartedTime:  time.Now().Add(-10 * time.Minute),
		FileUIDs:     []string{uuid.Must(uuid.NewV4()).String()},
	}

	ctx := context.Background()
	created, err := repo.CreateCatalogRun(ctx, run)
	require.NoError(t, err)
	require.False(t, created.UID.IsNil())
	require.False(t, created.CompletedTime.Valid)

	created.CompletedTime = null.TimeFrom(time.Now())
	created.TotalDuration = null.IntFrom(run.CompletedTime.Time.Sub(run.CreateTime).Milliseconds())
	err = repo.UpdateCatalogRun(ctx, created.UID, created)
	require.NoError(t, err)

	check := &repository.CatalogRun{UID: created.UID}
	err = tx.First(check).Error
	require.NoError(t, err)
	require.Equal(t, check.UID, created.UID)
	require.True(t, created.CompletedTime.Valid)
	require.True(t, created.TotalDuration.Valid)
	require.NotEqual(t, check.CreateTime, check.UpdateTime)
}
