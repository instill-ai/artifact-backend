package main

import (
	"database/sql"
	"fmt"

	_ "github.com/golang-migrate/migrate/v4/database/postgres"

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
}
