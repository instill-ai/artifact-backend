package handler

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/gofrs/uuid"
	"go.uber.org/zap"

	artifactpb "github.com/instill-ai/protogen-go/artifact/v1alpha"
)

func TestConvertPermissionClauses_NilInput(t *testing.T) {
	c := qt.New(t)
	c.Assert(convertPermissionClauses(nil, zap.NewNop()), qt.IsNil)
}

func TestConvertPermissionClauses_EmptySlice(t *testing.T) {
	c := qt.New(t)
	c.Assert(convertPermissionClauses([]*artifactpb.FilePermissionClause{}, zap.NewNop()), qt.IsNil)
}

func TestConvertPermissionClauses_CascadeClause(t *testing.T) {
	c := qt.New(t)

	clauses := []*artifactpb.FilePermissionClause{
		{TagsOverlap: []string{"agent:collection:col-abc", "agent:collection:col-def"}},
	}

	filter := convertPermissionClauses(clauses, zap.NewNop())
	c.Assert(filter, qt.IsNotNil)
	c.Assert(len(filter.Clauses), qt.Equals, 1)
	c.Assert(filter.Clauses[0].TagsOverlap, qt.DeepEquals, []string{"agent:collection:col-abc", "agent:collection:col-def"})
	c.Assert(filter.Clauses[0].UIDsIn, qt.IsNil)
	c.Assert(filter.Clauses[0].ParentFolderUIDsIn, qt.IsNil)
	c.Assert(filter.Clauses[0].TagsLikeNone, qt.IsNil)
	c.Assert(filter.Clauses[0].VisibilityIn, qt.IsNil)
}

func TestConvertPermissionClauses_OrphanClause(t *testing.T) {
	c := qt.New(t)

	clauses := []*artifactpb.FilePermissionClause{
		{
			TagsLikeNone: []string{"agent:collection:%"},
			VisibilityIn: []string{"VISIBILITY_WORKSPACE"},
		},
	}

	filter := convertPermissionClauses(clauses, zap.NewNop())
	c.Assert(filter, qt.IsNotNil)
	c.Assert(len(filter.Clauses), qt.Equals, 1)
	c.Assert(filter.Clauses[0].TagsLikeNone, qt.DeepEquals, []string{"agent:collection:%"})
	c.Assert(filter.Clauses[0].VisibilityIn, qt.DeepEquals, []string{"VISIBILITY_WORKSPACE"})
}

func TestConvertPermissionClauses_DirectGrantWithValidAndInvalidUIDs(t *testing.T) {
	c := qt.New(t)

	validUID := uuid.Must(uuid.NewV4())
	clauses := []*artifactpb.FilePermissionClause{
		{UidsIn: []string{validUID.String(), "not-a-uuid", "also-bad"}},
	}

	filter := convertPermissionClauses(clauses, zap.NewNop())
	c.Assert(filter, qt.IsNotNil)
	c.Assert(len(filter.Clauses), qt.Equals, 1)
	c.Assert(len(filter.Clauses[0].UIDsIn), qt.Equals, 1)
	c.Assert(filter.Clauses[0].UIDsIn[0], qt.Equals, validUID)
}

func TestConvertPermissionClauses_ParentFolderUIDs(t *testing.T) {
	c := qt.New(t)

	validUID := uuid.Must(uuid.NewV4())
	clauses := []*artifactpb.FilePermissionClause{
		{ParentFolderUidIn: []string{validUID.String(), "not-a-uuid"}},
	}

	filter := convertPermissionClauses(clauses, zap.NewNop())
	c.Assert(filter, qt.IsNotNil)
	c.Assert(len(filter.Clauses), qt.Equals, 1)
	c.Assert(filter.Clauses[0].UIDsIn, qt.IsNil)
	c.Assert(len(filter.Clauses[0].ParentFolderUIDsIn), qt.Equals, 1)
	c.Assert(filter.Clauses[0].ParentFolderUIDsIn[0], qt.Equals, validUID)
}

func TestConvertPermissionClauses_ThreePathCombined(t *testing.T) {
	c := qt.New(t)

	fileUID := uuid.Must(uuid.NewV4())
	folderUID := uuid.Must(uuid.NewV4())
	clauses := []*artifactpb.FilePermissionClause{
		{ParentFolderUidIn: []string{folderUID.String()}},
		{UidsIn: []string{fileUID.String()}},
		{
			TagsLikeNone: []string{"agent:collection:%"},
			VisibilityIn: []string{"VISIBILITY_WORKSPACE"},
		},
	}

	filter := convertPermissionClauses(clauses, zap.NewNop())
	c.Assert(filter, qt.IsNotNil)
	c.Assert(len(filter.Clauses), qt.Equals, 3)

	// Folder cascade
	c.Assert(len(filter.Clauses[0].ParentFolderUIDsIn), qt.Equals, 1)
	c.Assert(filter.Clauses[0].ParentFolderUIDsIn[0], qt.Equals, folderUID)

	// Direct
	c.Assert(len(filter.Clauses[1].UIDsIn), qt.Equals, 1)
	c.Assert(filter.Clauses[1].UIDsIn[0], qt.Equals, fileUID)

	// Orphan
	c.Assert(filter.Clauses[2].TagsLikeNone, qt.DeepEquals, []string{"agent:collection:%"})
	c.Assert(filter.Clauses[2].VisibilityIn, qt.DeepEquals, []string{"VISIBILITY_WORKSPACE"})
}

func TestConvertPermissionClauses_PublicVisibilityClause(t *testing.T) {
	c := qt.New(t)

	clauses := []*artifactpb.FilePermissionClause{
		{VisibilityIn: []string{"VISIBILITY_PUBLIC"}},
	}

	filter := convertPermissionClauses(clauses, zap.NewNop())
	c.Assert(filter, qt.IsNotNil)
	c.Assert(len(filter.Clauses), qt.Equals, 1)
	c.Assert(filter.Clauses[0].VisibilityIn, qt.DeepEquals, []string{"VISIBILITY_PUBLIC"})
}
