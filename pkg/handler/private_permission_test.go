package handler

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/gofrs/uuid"

	"github.com/instill-ai/artifact-backend/pkg/repository"
	artifactpb "github.com/instill-ai/protogen-go/artifact/v1alpha"
)

// convertPermissionClauses mirrors the conversion logic in ListFilesAdmin.
// Extracted here for testability without requiring a full gRPC context.
func convertPermissionClauses(clauses []*artifactpb.FilePermissionClause) *repository.FilePermissionFilter {
	if len(clauses) == 0 {
		return nil
	}
	repoClauses := make([]repository.FilePermissionClause, 0, len(clauses))
	for _, c := range clauses {
		rc := repository.FilePermissionClause{
			TagsOverlap:  c.GetTagsOverlap(),
			TagsLikeNone: c.GetTagsLikeNone(),
			VisibilityIn: c.GetVisibilityIn(),
		}
		if uids := c.GetUidsIn(); len(uids) > 0 {
			parsed := make([]uuid.UUID, 0, len(uids))
			for _, u := range uids {
				uid, parseErr := uuid.FromString(u)
				if parseErr != nil {
					continue
				}
				parsed = append(parsed, uid)
			}
			rc.UIDsIn = parsed
		}
		repoClauses = append(repoClauses, rc)
	}
	return &repository.FilePermissionFilter{Clauses: repoClauses}
}

func TestConvertPermissionClauses_NilInput(t *testing.T) {
	c := qt.New(t)
	c.Assert(convertPermissionClauses(nil), qt.IsNil)
}

func TestConvertPermissionClauses_EmptySlice(t *testing.T) {
	c := qt.New(t)
	c.Assert(convertPermissionClauses([]*artifactpb.FilePermissionClause{}), qt.IsNil)
}

func TestConvertPermissionClauses_CascadeClause(t *testing.T) {
	c := qt.New(t)

	clauses := []*artifactpb.FilePermissionClause{
		{TagsOverlap: []string{"agent:collection:col-abc", "agent:collection:col-def"}},
	}

	filter := convertPermissionClauses(clauses)
	c.Assert(filter, qt.IsNotNil)
	c.Assert(len(filter.Clauses), qt.Equals, 1)
	c.Assert(filter.Clauses[0].TagsOverlap, qt.DeepEquals, []string{"agent:collection:col-abc", "agent:collection:col-def"})
	c.Assert(filter.Clauses[0].UIDsIn, qt.IsNil)
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

	filter := convertPermissionClauses(clauses)
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

	filter := convertPermissionClauses(clauses)
	c.Assert(filter, qt.IsNotNil)
	c.Assert(len(filter.Clauses), qt.Equals, 1)
	c.Assert(len(filter.Clauses[0].UIDsIn), qt.Equals, 1)
	c.Assert(filter.Clauses[0].UIDsIn[0], qt.Equals, validUID)
}

func TestConvertPermissionClauses_ThreePathCombined(t *testing.T) {
	c := qt.New(t)

	fileUID := uuid.Must(uuid.NewV4())
	clauses := []*artifactpb.FilePermissionClause{
		{TagsOverlap: []string{"agent:collection:col-abc"}},
		{UidsIn: []string{fileUID.String()}},
		{
			TagsLikeNone: []string{"agent:collection:%"},
			VisibilityIn: []string{"VISIBILITY_WORKSPACE"},
		},
	}

	filter := convertPermissionClauses(clauses)
	c.Assert(filter, qt.IsNotNil)
	c.Assert(len(filter.Clauses), qt.Equals, 3)

	// Cascade
	c.Assert(filter.Clauses[0].TagsOverlap, qt.DeepEquals, []string{"agent:collection:col-abc"})

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

	filter := convertPermissionClauses(clauses)
	c.Assert(filter, qt.IsNotNil)
	c.Assert(len(filter.Clauses), qt.Equals, 1)
	c.Assert(filter.Clauses[0].VisibilityIn, qt.DeepEquals, []string{"VISIBILITY_PUBLIC"})
}
