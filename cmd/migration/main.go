package main

import (
	"database/sql"
	"fmt"
	"os"

	"github.com/golang-migrate/migrate/v4"

	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"

	"github.com/instill-ai/artifact-backend/config"
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
	if err := config.Init(); err != nil {
		panic(err)
	}

	databaseConfig := config.Config.Database
	if err := dbExistsOrCreate(databaseConfig); err != nil {
		panic(err)
	}

	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?%s",
		databaseConfig.Username,
		databaseConfig.Password,
		databaseConfig.Host,
		databaseConfig.Port,
		databaseConfig.Name,
		"sslmode=disable",
	)

	migrateFolder, _ := os.Getwd()
	m, err := migrate.New(fmt.Sprintf("file:///%s/pkg/db/migration", migrateFolder), dsn)
	if err != nil {
		panic(err)
	}

	expectedVersion := databaseConfig.Version
	curVersion, dirty, err := m.Version()
	if err != nil && curVersion != 0 {
		panic(err)
	}

	fmt.Printf("Expected migration version is %d\n", expectedVersion)
	fmt.Printf("The current schema version is %d, and dirty flag is %t\n", curVersion, dirty)
	if dirty {
		panic("the database's dirty flag is set, please fix it")
	}

	step := curVersion
	for {
		if expectedVersion <= step {
			fmt.Printf("Migration to version %d complete\n", expectedVersion)
			break
		}

		fmt.Printf("Step up to version %d\n", step+1)
		if err := m.Steps(1); err != nil {
			panic(err)
		}

		step, _, err = m.Version()
		if err != nil {
			panic(err)
		}
	}
}
