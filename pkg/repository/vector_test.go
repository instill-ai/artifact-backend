package repository

import (
	"testing"

	"github.com/frankban/quicktest"
	"github.com/gofrs/uuid"
)

func TestGroupByFile(t *testing.T) {
	c := quicktest.New(t)

	fileA := uuid.Must(uuid.NewV4())
	fileB := uuid.Must(uuid.NewV4())
	fileC := uuid.Must(uuid.NewV4())

	mkEmb := func(fileUID uuid.UUID, score float32) SimilarVectorEmbedding {
		return SimilarVectorEmbedding{
			VectorEmbedding: VectorEmbedding{FileUID: fileUID},
			Score:           score,
		}
	}

	t.Run("groupSize=1 keeps best per file", func(t *testing.T) {
		input := []SimilarVectorEmbedding{
			mkEmb(fileA, 0.95),
			mkEmb(fileA, 0.90),
			mkEmb(fileB, 0.88),
			mkEmb(fileA, 0.85),
			mkEmb(fileB, 0.80),
			mkEmb(fileC, 0.75),
		}
		got := groupByFile(input, 1)
		c.Assert(len(got), quicktest.Equals, 3)
		c.Assert(got[0].FileUID, quicktest.Equals, fileA)
		c.Assert(got[0].Score, quicktest.Equals, float32(0.95))
		c.Assert(got[1].FileUID, quicktest.Equals, fileB)
		c.Assert(got[2].FileUID, quicktest.Equals, fileC)
	})

	t.Run("groupSize=2 keeps top 2 per file", func(t *testing.T) {
		input := []SimilarVectorEmbedding{
			mkEmb(fileA, 0.95),
			mkEmb(fileA, 0.90),
			mkEmb(fileB, 0.88),
			mkEmb(fileA, 0.85),
			mkEmb(fileB, 0.80),
			mkEmb(fileC, 0.75),
		}
		got := groupByFile(input, 2)
		c.Assert(len(got), quicktest.Equals, 5)
		c.Assert(got[0].Score, quicktest.Equals, float32(0.95))
		c.Assert(got[1].Score, quicktest.Equals, float32(0.90))
		c.Assert(got[2].Score, quicktest.Equals, float32(0.88))
		c.Assert(got[3].Score, quicktest.Equals, float32(0.80))
		c.Assert(got[4].Score, quicktest.Equals, float32(0.75))
	})

	t.Run("empty input", func(t *testing.T) {
		got := groupByFile(nil, 1)
		c.Assert(len(got), quicktest.Equals, 0)
	})

	t.Run("single file many chunks", func(t *testing.T) {
		input := []SimilarVectorEmbedding{
			mkEmb(fileA, 0.95),
			mkEmb(fileA, 0.90),
			mkEmb(fileA, 0.85),
		}
		got := groupByFile(input, 1)
		c.Assert(len(got), quicktest.Equals, 1)
		c.Assert(got[0].Score, quicktest.Equals, float32(0.95))
	})
}
