package repository

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/gofrs/uuid"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	"github.com/instill-ai/artifact-backend/pkg/types"
)

func setupTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("failed to get raw db: %v", err)
	}

	// SQLite-compatible schema (avoids Postgres-specific types)
	for _, ddl := range []string{
		`CREATE TABLE file (
			uid TEXT PRIMARY KEY,
			id TEXT NOT NULL,
			display_name TEXT NOT NULL,
			slug TEXT,
			aliases TEXT,
			description TEXT,
			namespace_uid TEXT NOT NULL,
			creator_uid TEXT,
			file_type TEXT NOT NULL,
			storage_path TEXT NOT NULL,
			object_uid TEXT,
			process_status TEXT NOT NULL,
			extra_meta_data TEXT,
			create_time DATETIME,
			update_time DATETIME,
			delete_time DATETIME,
			size INTEGER,
			requester_uid TEXT,
			external_metadata TEXT,
			tags TEXT,
			usage_metadata TEXT,
			content_sha256 TEXT,
			is_text_based BOOLEAN DEFAULT FALSE,
			visibility TEXT NOT NULL DEFAULT 'VISIBILITY_WORKSPACE'
		)`,
		`CREATE TABLE file_knowledge_base (
			file_uid TEXT NOT NULL,
			kb_uid TEXT NOT NULL,
			content_sha256 TEXT,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (file_uid, kb_uid)
		)`,
	} {
		if _, err := sqlDB.Exec(ddl); err != nil {
			t.Fatalf("failed to create table: %v", err)
		}
	}

	return db
}

func TestCreateFileAdmin(t *testing.T) {
	c := qt.New(t)

	c.Run("creates file and association in a single transaction", func(c *qt.C) {
		db := setupTestDB(t)
		repo := NewDBOnlyRepository(db)

		fileUID := uuid.Must(uuid.NewV4())
		kbUID := uuid.Must(uuid.NewV4())
		nsUID := uuid.Must(uuid.NewV4())

		file := FileModel{
			UID:           types.FileUIDType(fileUID),
			ID:            "fil-test123",
			DisplayName:   "test-file.pdf",
			NamespaceUID:  types.NamespaceUIDType(nsUID),
			FileType:      "TYPE_PDF",
			StoragePath:   "/some/path",
			ProcessStatus: "FILE_PROCESS_STATUS_NOTSTARTED",
		}

		got, err := repo.CreateFileAdmin(context.Background(), file, types.KnowledgeBaseUIDType(kbUID))
		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.IsNotNil)
		c.Check(got.UID, qt.Equals, file.UID)
		c.Check(got.ID, qt.Equals, "fil-test123")
		c.Check(got.DisplayName, qt.Equals, "test-file.pdf")

		// Verify file record exists in DB
		var dbFile FileModel
		err = db.First(&dbFile, "uid = ?", fileUID).Error
		c.Assert(err, qt.IsNil)
		c.Check(dbFile.ID, qt.Equals, "fil-test123")

		// Verify association record exists
		var assoc FileKnowledgeBase
		err = db.First(&assoc, "file_uid = ? AND kb_uid = ?", fileUID, kbUID).Error
		c.Assert(err, qt.IsNil)
		c.Check(assoc.FileUID, qt.Equals, types.FileUIDType(fileUID))
		c.Check(assoc.KBUID, qt.Equals, types.KnowledgeBaseUIDType(kbUID))
	})

	c.Run("rolls back on duplicate file UID", func(c *qt.C) {
		db := setupTestDB(t)
		repo := NewDBOnlyRepository(db)

		fileUID := uuid.Must(uuid.NewV4())
		kbUID := uuid.Must(uuid.NewV4())
		nsUID := uuid.Must(uuid.NewV4())

		file := FileModel{
			UID:           types.FileUIDType(fileUID),
			ID:            "fil-dup123",
			DisplayName:   "dup-file.pdf",
			NamespaceUID:  types.NamespaceUIDType(nsUID),
			FileType:      "TYPE_PDF",
			StoragePath:   "/some/path",
			ProcessStatus: "FILE_PROCESS_STATUS_NOTSTARTED",
		}

		_, err := repo.CreateFileAdmin(context.Background(), file, types.KnowledgeBaseUIDType(kbUID))
		c.Assert(err, qt.IsNil)

		// Second create with same UID fails (primary key conflict)
		_, err = repo.CreateFileAdmin(context.Background(), file, types.KnowledgeBaseUIDType(kbUID))
		c.Assert(err, qt.IsNotNil)
	})

	c.Run("creates files in different KBs", func(c *qt.C) {
		db := setupTestDB(t)
		repo := NewDBOnlyRepository(db)

		kbUID1 := uuid.Must(uuid.NewV4())
		kbUID2 := uuid.Must(uuid.NewV4())
		nsUID := uuid.Must(uuid.NewV4())

		file1 := FileModel{
			UID:           types.FileUIDType(uuid.Must(uuid.NewV4())),
			ID:            "fil-one123",
			DisplayName:   "file1.pdf",
			NamespaceUID:  types.NamespaceUIDType(nsUID),
			FileType:      "TYPE_PDF",
			StoragePath:   "/path/1",
			ProcessStatus: "FILE_PROCESS_STATUS_NOTSTARTED",
		}
		file2 := FileModel{
			UID:           types.FileUIDType(uuid.Must(uuid.NewV4())),
			ID:            "fil-two456",
			DisplayName:   "file2.pdf",
			NamespaceUID:  types.NamespaceUIDType(nsUID),
			FileType:      "TYPE_PDF",
			StoragePath:   "/path/2",
			ProcessStatus: "FILE_PROCESS_STATUS_NOTSTARTED",
		}

		_, err := repo.CreateFileAdmin(context.Background(), file1, types.KnowledgeBaseUIDType(kbUID1))
		c.Assert(err, qt.IsNil)

		_, err = repo.CreateFileAdmin(context.Background(), file2, types.KnowledgeBaseUIDType(kbUID2))
		c.Assert(err, qt.IsNil)

		var count int64
		db.Model(&FileKnowledgeBase{}).Count(&count)
		c.Check(count, qt.Equals, int64(2))
	})
}
