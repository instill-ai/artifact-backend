package repository

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func readRepoSourceFile(t *testing.T, relPath string) string {
	t.Helper()
	_, here, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatalf("runtime.Caller failed")
	}
	full := filepath.Join(filepath.Dir(here), relPath)
	data, err := os.ReadFile(full)
	if err != nil {
		t.Fatalf("failed to read %s: %v", full, err)
	}
	return string(data)
}

// TestInvariant_MigrationUniqueContentSHA256PerKB verifies that the
// migration file creates the per-KB unique index on content_sha256.
func TestInvariant_MigrationUniqueContentSHA256PerKB(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(findMigrationDir(t), "000070_unique_content_sha256_per_kb.up.sql"))
	if err != nil {
		t.Fatalf("migration file not found: %v", err)
	}
	sql := string(data)

	if !strings.Contains(sql, "content_sha256") {
		t.Error("migration must add content_sha256 column to file_knowledge_base")
	}
	if !strings.Contains(sql, "CREATE UNIQUE INDEX") {
		t.Error("migration must create a UNIQUE index")
	}
	if !strings.Contains(sql, "idx_file_kb_unique_content_sha256") {
		t.Error("migration must create index named idx_file_kb_unique_content_sha256")
	}
	if !strings.Contains(sql, "kb_uid") {
		t.Error("unique index must be scoped to kb_uid")
	}
}

// TestInvariant_MigrationDownReversesUp verifies the down migration drops
// both the index and the column.
func TestInvariant_MigrationDownReversesUp(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(findMigrationDir(t), "000070_unique_content_sha256_per_kb.down.sql"))
	if err != nil {
		t.Fatalf("down migration file not found: %v", err)
	}
	sql := string(data)

	if !strings.Contains(sql, "DROP INDEX") {
		t.Error("down migration must drop the unique index")
	}
	if !strings.Contains(sql, "DROP COLUMN") {
		t.Error("down migration must drop the content_sha256 column")
	}
}

func TestInvariant_MigrationFileListTimeIndexes(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(findMigrationDir(t), "000071_add_file_list_time_indexes.up.sql"))
	if err != nil {
		t.Fatalf("migration file not found: %v", err)
	}
	sql := string(data)

	for _, want := range []string{
		"idx_file_namespace_update_time_uid",
		"ON file(namespace_uid, update_time DESC, uid DESC)",
		"WHERE delete_time IS NULL",
		"idx_file_knowledge_base_kb_file",
		"ON file_knowledge_base(kb_uid, file_uid)",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("time-filter migration must contain %q", want)
		}
	}
}

// TestInvariant_FileKnowledgeBaseHasContentSHA256Field verifies the GORM
// model includes the denormalized content_sha256 field.
func TestInvariant_FileKnowledgeBaseHasContentSHA256Field(t *testing.T) {
	src := readRepoSourceFile(t, "file.go")

	const marker = "type FileKnowledgeBase struct"
	idx := strings.Index(src, marker)
	if idx < 0 {
		t.Fatalf("FileKnowledgeBase struct not found in file.go")
	}
	body := src[idx:]
	end := strings.Index(body, "\n}")
	if end > 0 {
		body = body[:end]
	}

	if !strings.Contains(body, "ContentSHA256") {
		t.Error("FileKnowledgeBase must have a ContentSHA256 field")
	}
	if !strings.Contains(body, `column:content_sha256`) {
		t.Error("ContentSHA256 field must map to column content_sha256")
	}
}

// TestInvariant_CreateFilePopulatesContentSHA256 verifies that CreateFile
// copies content_sha256 from the file model to the junction entry.
func TestInvariant_CreateFilePopulatesContentSHA256(t *testing.T) {
	src := readRepoSourceFile(t, "file.go")

	const marker = "func (r *repository) CreateFile("
	idx := strings.Index(src, marker)
	if idx < 0 {
		t.Fatalf("CreateFile signature not found in file.go")
	}
	body := src[idx:]
	end := strings.Index(body, "\n}\n")
	if end > 0 {
		body = body[:end]
	}

	if !strings.Contains(body, "ContentSHA256: file.ContentSHA256") {
		t.Error("CreateFile must populate ContentSHA256 on the FileKnowledgeBase association")
	}
	if !strings.Contains(body, "isDuplicateContentSHA256Error") {
		t.Error("CreateFile must check for isDuplicateContentSHA256Error on association insert")
	}
	if !strings.Contains(body, "ErrDuplicateContentSHA256") {
		t.Error("CreateFile must return ErrDuplicateContentSHA256 on unique violation")
	}
}

// TestInvariant_DeleteFileClearsAssociationContentSHA256 verifies that a
// soft-deleted file releases the partial unique index slot for re-upload.
func TestInvariant_DeleteFileClearsAssociationContentSHA256(t *testing.T) {
	src := readRepoSourceFile(t, "file.go")

	const marker = "func (r *repository) DeleteFileAndDecreaseUsage("
	idx := strings.Index(src, marker)
	if idx < 0 {
		t.Fatalf("DeleteFileAndDecreaseUsage signature not found in file.go")
	}
	body := src[idx:]
	next := strings.Index(body, "\n// GetFileByKBUIDAndFileID")
	if next > 0 {
		body = body[:next]
	}

	if !strings.Contains(body, "tx.Model(&FileKnowledgeBase{})") {
		t.Error("DeleteFileAndDecreaseUsage must update file_knowledge_base associations")
	}
	if !strings.Contains(body, `Update("content_sha256", "")`) {
		t.Error("DeleteFileAndDecreaseUsage must clear association content_sha256")
	}
}

// TestInvariant_ErrDuplicateContentSHA256Defined verifies the sentinel error
// and helper function exist.
func TestInvariant_ErrDuplicateContentSHA256Defined(t *testing.T) {
	src := readRepoSourceFile(t, "file.go")

	if !strings.Contains(src, "var ErrDuplicateContentSHA256") {
		t.Error("file.go must define ErrDuplicateContentSHA256 sentinel error")
	}
	if !strings.Contains(src, "func isDuplicateContentSHA256Error(") {
		t.Error("file.go must define isDuplicateContentSHA256Error helper")
	}
}

// TestInvariant_TargetSchemaVersionIncludesMigration071 verifies that the
// target schema version has been bumped to include migration 071.
func TestInvariant_TargetSchemaVersionIncludesMigration071(t *testing.T) {
	src := readRepoSourceFile(t, "../db/migration/migration.go")
	if !strings.Contains(src, "TargetSchemaVersion uint = 71") {
		t.Error("TargetSchemaVersion must be >= 71 to include the file list time indexes migration")
	}
}

func findMigrationDir(t *testing.T) string {
	t.Helper()
	_, here, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatalf("runtime.Caller failed")
	}
	return filepath.Join(filepath.Dir(here), "..", "db", "migration")
}
