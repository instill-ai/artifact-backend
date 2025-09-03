package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	"github.com/golang-migrate/migrate/v4"
	"go.uber.org/zap"

	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/db/migration"

	database "github.com/instill-ai/artifact-backend/pkg/db"
	logx "github.com/instill-ai/x/log"
)

func dbExistsOrCreate(databaseConfig config.DatabaseConfig) error {
	datasource := fmt.Sprintf("host=%s user=%s password=%s dbname=postgres port=%d sslmode=disable TimeZone=%s",
		databaseConfig.Host,
		databaseConfig.Username,
		databaseConfig.Password,
		databaseConfig.Port,
		databaseConfig.TimeZone,
	)

	db, err := sql.Open("postgres", datasource)
	if err != nil {
		return err
	}

	defer db.Close()

	// Open() may just validate its arguments without creating a connection to the database.
	// To verify that the data source name is valid, call Ping().
	if err = db.Ping(); err != nil {
		return err
	}

	var count int

	q := fmt.Sprintf("SELECT count(*) FROM pg_catalog.pg_database WHERE datname = '%s';", databaseConfig.Name)
	if err := db.QueryRow(q).Scan(&count); err != nil {
		return err
	}

	if count > 0 {
		return nil
	}

	fmt.Printf("Create database %s\n", databaseConfig.Name)
	if _, err := db.Exec(fmt.Sprintf("CREATE DATABASE %s;", databaseConfig.Name)); err != nil {
		return err
	}

	return nil
}

func main() {
	if err := config.Init(config.ParseConfigFlag()); err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logx.Debug = config.Config.Server.Debug
	logger, _ := logx.GetZapLogger(ctx)
	defer func() {
		// can't handle the error due to https://github.com/uber-go/zap/issues/880
		_ = logger.Sync()
	}()

	databaseConfig := config.Config.Database
	if err := dbExistsOrCreate(databaseConfig); err != nil {
		logger.Fatal("Checking database existence", zap.Error(err))
	}

	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?%s",
		databaseConfig.Username,
		databaseConfig.Password,
		databaseConfig.Host,
		databaseConfig.Port,
		databaseConfig.Name,
		"sslmode=disable",
	)

	db := database.GetSharedConnection().WithContext(ctx)
	codeMigrator := &migration.CodeMigrator{
		Logger: logger,
		DB:     db,
	}

	defer func() { database.Close(db) }()

	if err := runMigration(dsn, migration.TargetSchemaVersion, codeMigrator.Migrate, logger); err != nil {
		logger.Fatal("Running migration", zap.Error(err))
	}
}

func runMigration(
	dsn string,
	expectedVersion uint,
	execCode func(version uint) error,
	logger *zap.Logger,
) error {
	migrateFolder, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("accessing base path: %w", err)
	}

	m, err := migrate.New(fmt.Sprintf("file:///%s/pkg/db/migration", migrateFolder), dsn)
	if err != nil {
		return fmt.Errorf("creating migration: %w", err)
	}

	curVersion, dirty, err := m.Version()
	if err != nil && curVersion != 0 {
		return fmt.Errorf("getting current version: %w", err)
	}

	logger.Info("Running migration",
		zap.Uint("expectedVersion", expectedVersion),
		zap.Uint("currentVersion", curVersion),
		zap.Bool("dirty", dirty),
	)

	if dirty {
		return fmt.Errorf("database is dirty, please fix it")
	}

	step := curVersion
	for {
		if expectedVersion <= step {
			logger.Info("Migration completed", zap.Uint("expectedVersion", expectedVersion))
			break
		}

		logger.Info("Step up", zap.Uint("step", step+1))
		if err := m.Steps(1); err != nil {
			return fmt.Errorf("stepping up: %w", err)
		}

		if step, _, err = m.Version(); err != nil {
			return fmt.Errorf("getting new version: %w", err)
		}

		if err := execCode(step); err != nil {
			return fmt.Errorf("running associated code: %w", err)
		}
	}

	return nil
}
