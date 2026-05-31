package repository

import (
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"go.einride.tech/aip/filtering"
	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

type testFilterRequest struct {
	filter string
}

func (r testFilterRequest) GetFilter() string {
	return r.filter
}

func TestTranspileFilter_TimestampRange(t *testing.T) {
	c := qt.New(t)

	declarations, err := filtering.NewDeclarations(
		filtering.DeclareStandardFunctions(),
		filtering.DeclareIdent("update_time", filtering.TypeTimestamp),
	)
	c.Assert(err, qt.IsNil)

	parsed, err := filtering.ParseFilter(testFilterRequest{filter: `update_time >= timestamp("2026-01-01T00:00:00.000Z") AND update_time < timestamp("2027-01-01T00:00:00.000Z")`}, declarations)
	c.Assert(err, qt.IsNil)

	expr, err := (&repository{}).TranspileFilter(parsed)
	c.Assert(err, qt.IsNil)
	c.Assert(expr.SQL, qt.Equals, "update_time >= ? AND update_time < ?")
	c.Assert(expr.Vars, qt.HasLen, 2)
	c.Assert(expr.Vars[0], qt.Equals, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	c.Assert(expr.Vars[1], qt.Equals, time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC))
}

func TestTranspileFilter_NotInCollection(t *testing.T) {
	c := qt.New(t)

	declarations, err := filtering.NewDeclarations(
		filtering.DeclareStandardFunctions(),
		filtering.DeclareIdent("in_collection", filtering.TypeBool),
	)
	c.Assert(err, qt.IsNil)

	parsed, err := filtering.ParseFilter(testFilterRequest{filter: `NOT in_collection`}, declarations)
	c.Assert(err, qt.IsNil)

	expr, err := (&repository{}).TranspileFilter(parsed)
	c.Assert(err, qt.IsNil)
	c.Assert(expr.SQL, qt.Equals, "NOT EXISTS (SELECT 1 FROM unnest(file.tags) t WHERE t LIKE ?)")
	c.Assert(expr.Vars, qt.DeepEquals, []interface{}{"agent:collection:%"})
}

func TestTranspileFilter_NotInCollectionOrCollectionTag(t *testing.T) {
	c := qt.New(t)

	declarations, err := filtering.NewDeclarations(
		filtering.DeclareStandardFunctions(),
		filtering.DeclareIdent("in_collection", filtering.TypeBool),
		filtering.DeclareIdent("tags", &exprpb.Type{
			TypeKind: &exprpb.Type_ListType_{
				ListType: &exprpb.Type_ListType{
					ElemType: filtering.TypeString,
				},
			},
		}),
	)
	c.Assert(err, qt.IsNil)

	parsed, err := filtering.ParseFilter(testFilterRequest{filter: `NOT in_collection OR tags:"agent:collection:col-a"`}, declarations)
	c.Assert(err, qt.IsNil)

	expr, err := (&repository{}).TranspileFilter(parsed)
	c.Assert(err, qt.IsNil)
	c.Assert(expr.SQL, qt.Equals, "NOT EXISTS (SELECT 1 FROM unnest(file.tags) t WHERE t LIKE ?) OR ? = ANY(tags)")
	c.Assert(expr.Vars, qt.DeepEquals, []interface{}{"agent:collection:%", "agent:collection:col-a"})
}
