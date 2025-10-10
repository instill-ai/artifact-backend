package worker

import (
	"context"
	"testing"

	"github.com/gofrs/uuid"
	"github.com/gojuno/minimock/v3"
	"google.golang.org/protobuf/types/known/structpb"

	qt "github.com/frankban/quicktest"

	"github.com/instill-ai/artifact-backend/pkg/constant"
	"github.com/instill-ai/artifact-backend/pkg/mock"
	"github.com/instill-ai/artifact-backend/pkg/repository"
)

func TestGetFileByUID_Success(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	fileUID := uuid.Must(uuid.NewV4())
	expectedFile := repository.KnowledgeBaseFile{
		UID:  fileUID,
		Name: "test.pdf",
	}

	mockRepo := mock.NewRepositoryIMock(mc)
	mockRepo.GetKnowledgeBaseFilesByFileUIDsMock.
		When(minimock.AnyContext, []uuid.UUID{fileUID}).
		Then([]repository.KnowledgeBaseFile{expectedFile}, nil)

	file, err := getFileByUID(ctx, mockRepo, fileUID)
	c.Assert(err, qt.IsNil)
	c.Assert(file, qt.DeepEquals, expectedFile)
}

func TestGetFileByUID_NotFound(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	fileUID := uuid.Must(uuid.NewV4())

	mockRepo := mock.NewRepositoryIMock(mc)
	mockRepo.GetKnowledgeBaseFilesByFileUIDsMock.
		When(minimock.AnyContext, []uuid.UUID{fileUID}).
		Then([]repository.KnowledgeBaseFile{}, nil)

	file, err := getFileByUID(ctx, mockRepo, fileUID)
	c.Assert(err, qt.Not(qt.IsNil))
	c.Assert(err.Error(), qt.Contains, "not found")
	c.Assert(file, qt.DeepEquals, repository.KnowledgeBaseFile{})
}

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

	authCtx, err := createAuthenticatedContext(ctx, metadataStruct)
	c.Assert(err, qt.IsNil)
	c.Assert(authCtx, qt.Not(qt.IsNil))
}

func TestCreateAuthenticatedContext_NilMetadata(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()

	authCtx, err := createAuthenticatedContext(ctx, nil)
	c.Assert(err, qt.IsNil)
	c.Assert(authCtx, qt.Equals, ctx) // Should return original context
}
