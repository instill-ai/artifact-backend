package worker

import (
	"context"
	"testing"

	"google.golang.org/protobuf/types/known/structpb"

	qt "github.com/frankban/quicktest"

	"github.com/instill-ai/artifact-backend/pkg/constant"
)

// NOTE: Tests for getFileByUID were removed as the function was removed during dead code cleanup.

func TestExtractRequestMetadata_Success(t *testing.T) {
	c := qt.New(t)

	// Create metadata with request info
	metadataStruct, err := structpb.NewStruct(map[string]interface{}{
		constant.MetadataRequestKey: map[string]interface{}{
			"authorization": []interface{}{"Bearer token123"},
			"user-agent":    []interface{}{"test-agent"},
		},
	})
	c.Assert(err, qt.IsNil)

	md, err := extractRequestMetadata(metadataStruct)
	c.Assert(err, qt.IsNil)
	c.Assert(len(md) > 0, qt.IsTrue)
	c.Assert(md["authorization"], qt.Not(qt.IsNil))
	c.Assert(md["user-agent"], qt.Not(qt.IsNil))
}

func TestExtractRequestMetadata_Nil(t *testing.T) {
	c := qt.New(t)

	md, err := extractRequestMetadata(nil)
	c.Assert(err, qt.IsNil)
	c.Assert(md, qt.HasLen, 0)
}

func TestExtractRequestMetadata_NoRequestKey(t *testing.T) {
	c := qt.New(t)

	metadataStruct, err := structpb.NewStruct(map[string]interface{}{
		"other-key": "other-value",
	})
	c.Assert(err, qt.IsNil)

	md, err := extractRequestMetadata(metadataStruct)
	c.Assert(err, qt.IsNil)
	c.Assert(md, qt.HasLen, 0)
}

func TestCreateAuthenticatedContext_Success(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()

	metadataStruct, err := structpb.NewStruct(map[string]interface{}{
		constant.MetadataRequestKey: map[string]interface{}{
			"authorization": []interface{}{"Bearer token123"},
		},
	})
	c.Assert(err, qt.IsNil)

	authCtx, err := CreateAuthenticatedContext(ctx, metadataStruct)
	c.Assert(err, qt.IsNil)
	c.Assert(authCtx, qt.Not(qt.IsNil))
}

func TestCreateAuthenticatedContext_NilMetadata(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()

	authCtx, err := CreateAuthenticatedContext(ctx, nil)
	c.Assert(err, qt.IsNil)
	c.Assert(authCtx, qt.Equals, ctx) // Should return original context
}
