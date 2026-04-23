package repository

import (
	"testing"

	"github.com/gofrs/uuid"
	qt "github.com/frankban/quicktest"
	"github.com/lib/pq"
)

// All tests target the SQL-fragment + bind-args contract that
// repository.ListFiles relies on. They never touch a database — the goal is
// to lock the compiled output so EE callers (and any future consumers) can
// reason about exactly what they get when they assemble a permission filter.

func TestFilePermissionClause_compile_empty(t *testing.T) {
	c := qt.New(t)

	clause := FilePermissionClause{}
	frag, args := clause.compile()

	c.Check(frag, qt.Equals, "")
	c.Check(args, qt.IsNil)
	c.Check(clause.isEmpty(), qt.IsTrue)
}

func TestFilePermissionClause_compile_tagsOverlap(t *testing.T) {
	c := qt.New(t)

	tags := []string{"agent:collection:abc", "agent:collection:def"}
	clause := FilePermissionClause{TagsOverlap: tags}
	frag, args := clause.compile()

	c.Check(frag, qt.Equals, "(file.tags && ?)")
	c.Assert(args, qt.HasLen, 1)
	// pq.Array wraps the slice; equality via reflect.DeepEqual on the wrapper.
	c.Check(args[0], qt.DeepEquals, pq.Array(tags))
}

func TestFilePermissionClause_compile_uidsIn(t *testing.T) {
	c := qt.New(t)

	u1 := uuid.Must(uuid.NewV4())
	u2 := uuid.Must(uuid.NewV4())
	clause := FilePermissionClause{UIDsIn: []uuid.UUID{u1, u2}}
	frag, args := clause.compile()

	c.Check(frag, qt.Equals, "(file.uid = ANY(?))")
	c.Assert(args, qt.HasLen, 1)
	c.Check(args[0], qt.DeepEquals, pq.Array([]string{u1.String(), u2.String()}))
}

func TestFilePermissionClause_compile_tagsLikeNone(t *testing.T) {
	c := qt.New(t)

	clause := FilePermissionClause{TagsLikeNone: []string{"agent:collection:%", "agent:project:%"}}
	frag, args := clause.compile()

	// One NOT EXISTS per pattern, AND-joined inside the clause parens.
	c.Check(frag, qt.Equals,
		"(NOT EXISTS (SELECT 1 FROM unnest(file.tags) t WHERE t LIKE ?) "+
			"AND NOT EXISTS (SELECT 1 FROM unnest(file.tags) t WHERE t LIKE ?))")
	c.Check(args, qt.DeepEquals, []any{"agent:collection:%", "agent:project:%"})
}

func TestFilePermissionClause_compile_visibilityIn(t *testing.T) {
	c := qt.New(t)

	clause := FilePermissionClause{VisibilityIn: []string{"VISIBILITY_WORKSPACE", "VISIBILITY_PUBLIC"}}
	frag, args := clause.compile()

	c.Check(frag, qt.Equals, "(file.visibility = ANY(?))")
	c.Assert(args, qt.HasLen, 1)
	c.Check(args[0], qt.DeepEquals, pq.Array([]string{"VISIBILITY_WORKSPACE", "VISIBILITY_PUBLIC"}))
}

func TestFilePermissionClause_compile_orphanShape(t *testing.T) {
	// Mirrors the EE "orphan reads" clause: file has no agent:collection: tag
	// AND visibility is in WORKSPACE. Locks the AND-composition order so the
	// EE side can rely on a stable plan shape.
	c := qt.New(t)

	clause := FilePermissionClause{
		TagsLikeNone: []string{"agent:collection:%"},
		VisibilityIn: []string{"VISIBILITY_WORKSPACE"},
	}
	frag, args := clause.compile()

	c.Check(frag, qt.Equals,
		"(NOT EXISTS (SELECT 1 FROM unnest(file.tags) t WHERE t LIKE ?) "+
			"AND file.visibility = ANY(?))")
	c.Assert(args, qt.HasLen, 2)
	c.Check(args[0], qt.Equals, "agent:collection:%")
	c.Check(args[1], qt.DeepEquals, pq.Array([]string{"VISIBILITY_WORKSPACE"}))
}

func TestFilePermissionFilter_compile_nil(t *testing.T) {
	c := qt.New(t)

	var f *FilePermissionFilter
	frag, args := f.compile()

	c.Check(frag, qt.Equals, "")
	c.Check(args, qt.IsNil)
}

func TestFilePermissionFilter_compile_emptyClauses(t *testing.T) {
	c := qt.New(t)

	f := &FilePermissionFilter{}
	frag, args := f.compile()

	c.Check(frag, qt.Equals, "")
	c.Check(args, qt.IsNil)
}

func TestFilePermissionFilter_compile_dropsEmptyClauses(t *testing.T) {
	// A clause with zero populated dimensions must NOT compile to TRUE — that
	// would silently widen the disjunction to match every row. Empty clauses
	// are dropped; if the whole filter ends up empty we return an empty
	// fragment (no-op).
	c := qt.New(t)

	f := &FilePermissionFilter{Clauses: []FilePermissionClause{{}, {}}}
	frag, args := f.compile()

	c.Check(frag, qt.Equals, "")
	c.Check(args, qt.IsNil)
}

func TestFilePermissionFilter_compile_singleClauseWrapping(t *testing.T) {
	c := qt.New(t)

	f := &FilePermissionFilter{
		Clauses: []FilePermissionClause{{TagsOverlap: []string{"x"}}},
	}
	frag, args := f.compile()

	// Outer parens are added even with one clause so the term composes safely
	// inside an AND-chain in the surrounding query.
	c.Check(frag, qt.Equals, "((file.tags && ?))")
	c.Assert(args, qt.HasLen, 1)
}

func TestFilePermissionFilter_compile_threePathOR(t *testing.T) {
	// The canonical EE shape: cascade (TagsOverlap), direct (UIDsIn), orphan
	// (TagsLikeNone + VisibilityIn). Ordering and parenthesisation are part of
	// the contract — EE relies on the OR composition for correctness.
	c := qt.New(t)

	cascadeTags := []string{"agent:collection:c1", "agent:collection:c2"}
	u1 := uuid.Must(uuid.NewV4())

	f := &FilePermissionFilter{
		Clauses: []FilePermissionClause{
			{TagsOverlap: cascadeTags},
			{UIDsIn: []uuid.UUID{u1}},
			{TagsLikeNone: []string{"agent:collection:%"}, VisibilityIn: []string{"VISIBILITY_WORKSPACE"}},
		},
	}
	frag, args := f.compile()

	c.Check(frag, qt.Equals,
		"((file.tags && ?) "+
			"OR (file.uid = ANY(?)) "+
			"OR (NOT EXISTS (SELECT 1 FROM unnest(file.tags) t WHERE t LIKE ?) "+
			"AND file.visibility = ANY(?)))")

	c.Assert(args, qt.HasLen, 4)
	c.Check(args[0], qt.DeepEquals, pq.Array(cascadeTags))
	c.Check(args[1], qt.DeepEquals, pq.Array([]string{u1.String()}))
	c.Check(args[2], qt.Equals, "agent:collection:%")
	c.Check(args[3], qt.DeepEquals, pq.Array([]string{"VISIBILITY_WORKSPACE"}))
}

func TestFilePermissionFilter_compile_mixedEmptyAndPopulated(t *testing.T) {
	// Empty clauses interleaved with populated ones must be silently dropped,
	// not turn the OR into a no-op.
	c := qt.New(t)

	f := &FilePermissionFilter{
		Clauses: []FilePermissionClause{
			{},
			{TagsOverlap: []string{"agent:collection:c1"}},
			{},
		},
	}
	frag, args := f.compile()

	c.Check(frag, qt.Equals, "((file.tags && ?))")
	c.Assert(args, qt.HasLen, 1)
}
