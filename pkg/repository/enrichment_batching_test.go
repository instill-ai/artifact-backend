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

// TestGetTotalTextChunksBySources_Batched pins the OR-chain → indexable-IN
// rewrite: counts must be grouped per source_uid, scoped by source_table
// (a chunk in a different source_table must NOT count toward a file), and
// files with no chunks must be absent from the result. This is the behaviour
// that the previous `(source_table=? AND source_uid=?) OR …` chain provided —
// preserved while making the query index-friendly.
func TestGetTotalTextChunksBySources_Batched(t *testing.T) {
	c := qt.New(t)

	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	c.Assert(err, qt.IsNil)
	// Minimal `chunk` schema — GetTotalTextChunksBySources only references
	// source_table + source_uid (Select/Where/Group).
	c.Assert(db.Exec(`CREATE TABLE chunk (uid TEXT, source_table TEXT, source_uid TEXT)`).Error, qt.IsNil)

	repo := &repository{db: db}
	ctx := context.Background()

	srcA := uuid.Must(uuid.NewV4()) // 2 chunks in converted_file
	srcB := uuid.Must(uuid.NewV4()) // 3 chunks in converted_file
	srcC := uuid.Must(uuid.NewV4()) // no chunks

	insert := func(table string, src uuid.UUID) {
		c.Assert(db.Exec(`INSERT INTO chunk (uid, source_table, source_uid) VALUES (?, ?, ?)`,
			uuid.Must(uuid.NewV4()).String(), table, src.String()).Error, qt.IsNil)
	}
	insert("converted_file", srcA)
	insert("converted_file", srcA)
	insert("converted_file", srcB)
	insert("converted_file", srcB)
	insert("converted_file", srcB)
	// A chunk for srcA under a DIFFERENT source_table — must be excluded by
	// the source_table predicate (would wrongly inflate srcA to 3 if dropped).
	insert("other_table", srcA)

	fA := uuid.Must(uuid.NewV4())
	fB := uuid.Must(uuid.NewV4())
	fC := uuid.Must(uuid.NewV4())

	sources := map[types.FileUIDType]struct {
		SourceTable types.SourceTableType
		SourceUID   types.SourceUIDType
	}{
		fA: {SourceTable: "converted_file", SourceUID: srcA},
		fB: {SourceTable: "converted_file", SourceUID: srcB},
		fC: {SourceTable: "converted_file", SourceUID: srcC},
	}

	counts, err := repo.GetTotalTextChunksBySources(ctx, sources)
	c.Assert(err, qt.IsNil)
	c.Check(counts[fA], qt.Equals, 2, qt.Commentf("source_table predicate must exclude the other_table chunk"))
	c.Check(counts[fB], qt.Equals, 3)
	_, hasC := counts[fC]
	c.Check(hasC, qt.IsFalse, qt.Commentf("a file with no chunks must be absent from the map"))

	// Empty input short-circuits.
	empty, err := repo.GetTotalTextChunksBySources(ctx, nil)
	c.Assert(err, qt.IsNil)
	c.Check(len(empty), qt.Equals, 0)
}
