package repository

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gofrs/uuid"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"github.com/instill-ai/artifact-backend/pkg/types"
)

func setupMockDB(t *testing.T) (*gorm.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock: %v", err)
	}
	gormDB, err := gorm.Open(postgres.New(postgres.Config{
		Conn: db,
	}), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open gorm: %v", err)
	}
	return gormDB, mock
}

func TestGetObjectsByUIDs_Empty(t *testing.T) {
	gormDB, _ := setupMockDB(t)
	repo := &repository{db: gormDB}

	result, err := repo.GetObjectsByUIDs(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected empty map, got %d entries", len(result))
	}

	result, err = repo.GetObjectsByUIDs(context.Background(), []types.ObjectUIDType{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected empty map, got %d entries", len(result))
	}
}

func TestGetObjectsByUIDs_SingleBatch(t *testing.T) {
	gormDB, mock := setupMockDB(t)
	repo := &repository{db: gormDB}

	uid1 := types.ObjectUIDType(uuid.Must(uuid.NewV4()))
	uid2 := types.ObjectUIDType(uuid.Must(uuid.NewV4()))

	rows := sqlmock.NewRows([]string{"uid", "id", "display_name", "namespace_uid", "creator_uid", "is_uploaded", "storage_path", "size", "content_type"}).
		AddRow(uid1, "obj-abc", "file1.txt", uuid.Must(uuid.NewV4()), uuid.Must(uuid.NewV4()), true, "ns-xxx/obj-abc", int64(100), "text/plain")

	mock.ExpectQuery(`SELECT .* FROM "object"`).
		WillReturnRows(rows)

	result, err := repo.GetObjectsByUIDs(context.Background(), []types.ObjectUIDType{uid1, uid2})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 result (uid2 missing), got %d", len(result))
	}
	if result[uid1] == nil {
		t.Fatal("expected uid1 in result map")
	}
	if result[uid2] != nil {
		t.Fatal("expected uid2 to be absent from result map")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

func TestGetObjectsByUIDs_ChunkBoundary(t *testing.T) {
	gormDB, mock := setupMockDB(t)
	repo := &repository{db: gormDB}

	uids := make([]types.ObjectUIDType, 1001)
	for i := range uids {
		uids[i] = types.ObjectUIDType(uuid.Must(uuid.NewV4()))
	}

	// First chunk of 1000
	rows1 := sqlmock.NewRows([]string{"uid", "id", "display_name", "namespace_uid", "creator_uid", "is_uploaded", "storage_path", "size", "content_type"})
	mock.ExpectQuery(`SELECT .* FROM "object"`).WillReturnRows(rows1)

	// Second chunk of 1
	rows2 := sqlmock.NewRows([]string{"uid", "id", "display_name", "namespace_uid", "creator_uid", "is_uploaded", "storage_path", "size", "content_type"}).
		AddRow(uids[1000], "obj-last", "last.txt", uuid.Must(uuid.NewV4()), uuid.Must(uuid.NewV4()), true, "ns-xxx/obj-last", int64(50), "text/plain")
	mock.ExpectQuery(`SELECT .* FROM "object"`).WillReturnRows(rows2)

	result, err := repo.GetObjectsByUIDs(context.Background(), uids)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result))
	}
	if result[uids[1000]] == nil {
		t.Fatal("expected last UID in result")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}
