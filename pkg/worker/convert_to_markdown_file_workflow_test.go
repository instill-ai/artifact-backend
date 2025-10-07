package worker

import (
	"errors"
	"testing"
	"time"

	"github.com/gofrs/uuid"
	"github.com/gojuno/minimock/v3"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/zap"

	qt "github.com/frankban/quicktest"

	"github.com/instill-ai/artifact-backend/pkg/mock"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/service"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

func TestConvertToMarkdownFileWorkflowParam_Validation(t *testing.T) {
	c := qt.New(t)
	fileUID := uuid.Must(uuid.NewV4())
	kbUID := uuid.Must(uuid.NewV4())

	tests := []struct {
		name  string
		param ConvertToMarkdownFileWorkflowParam
	}{
		{
			name: "Valid parameters",
			param: ConvertToMarkdownFileWorkflowParam{
				FileUID:          fileUID,
				KnowledgeBaseUID: kbUID,
				Bucket:           "test-bucket",
				Destination:      "test/file.pdf",
				FileType:         artifactpb.FileType_FILE_TYPE_PDF,
				Filename:         "file.pdf",
			},
		},
		{
			name: "Different valid parameters",
			param: ConvertToMarkdownFileWorkflowParam{
				FileUID:          uuid.Must(uuid.NewV4()),
				KnowledgeBaseUID: uuid.Must(uuid.NewV4()),
				Bucket:           "another-bucket",
				Destination:      "another/path.docx",
				FileType:         artifactpb.FileType_FILE_TYPE_DOCX,
				Filename:         "path.docx",
			},
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			c.Assert(tt.param.FileUID, qt.Not(qt.Equals), uuid.Nil)
			c.Assert(tt.param.KnowledgeBaseUID, qt.Not(qt.Equals), uuid.Nil)
			c.Assert(tt.param.Bucket, qt.Not(qt.Equals), "")
			c.Assert(tt.param.Destination, qt.Not(qt.Equals), "")
			c.Assert(tt.param.FileType, qt.Not(qt.Equals), artifactpb.FileType_FILE_TYPE_UNSPECIFIED)
		})
	}
}

func TestConvertToMarkdownFileWorkflowResult_FieldTypes(t *testing.T) {
	c := qt.New(t)

	result := &ConvertToMarkdownFileWorkflowResult{
		Markdown:        "# Test",
		Length:          []uint32{100, 200},
		PositionData:    &repository.PositionData{PageDelimiters: []uint32{50}},
		PipelineRelease: service.PipelineRelease{},
		ConvertedType:   artifactpb.FileType_FILE_TYPE_PDF,
		OriginalType:    artifactpb.FileType_FILE_TYPE_DOCX,
		FormatConverted: true,
	}

	c.Assert(result.Markdown, qt.Equals, "# Test")
	c.Assert(result.Length, qt.HasLen, 2)
	c.Assert(result.PositionData, qt.Not(qt.IsNil))
	c.Assert(result.ConvertedType, qt.Equals, artifactpb.FileType_FILE_TYPE_PDF)
	c.Assert(result.OriginalType, qt.Equals, artifactpb.FileType_FILE_TYPE_DOCX)
	c.Assert(result.FormatConverted, qt.IsTrue)
}

func TestConvertToMarkdownFileWorkflow_WithoutFormatConversion(t *testing.T) {
	t.Skip("TODO: Workflow tests need service-level mocking - see minio_workflow_test.go for pattern. Activities are tested.")
	c := qt.New(t)
	mc := minimock.NewController(t)

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	mockService := mock.NewServiceMock(mc)
	logger := zap.NewNop()

	worker := &Worker{
		service: mockService,
		log:     logger,
	}

	fileUID := uuid.Must(uuid.NewV4())
	kbUID := uuid.Must(uuid.NewV4())

	// Mock all activities - no need to register when mocking
	env.OnActivity(worker.ConvertFileTypeActivity).
		Return(&ConvertFileTypeActivityResult{
			ConvertedDestination: "", // No conversion
			ConvertedBucket:      "",
			ConvertedType:        artifactpb.FileType_FILE_TYPE_PDF,
			OriginalType:         artifactpb.FileType_FILE_TYPE_PDF,
			Converted:            false,
			PipelineRelease:      service.PipelineRelease{},
		}, nil)

	env.OnActivity(worker.CacheContextActivity).
		Return(&CacheContextActivityResult{
			CacheName:    "test-cache-123",
			Model:        "gemini-2.0-flash",
			CreateTime:   time.Now(),
			ExpireTime:   time.Now().Add(5 * time.Minute),
			CacheEnabled: true,
		}, nil)

	env.OnActivity(worker.DeleteCacheActivity).
		Return(nil)

	env.OnActivity(worker.DeleteTemporaryConvertedFileActivity).
		Return(nil)

	env.OnActivity(worker.ConvertToMarkdownFileActivity).
		Return(&ConvertToMarkdownFileActivityResult{
			Markdown:        "# Converted Markdown",
			Length:          []uint32{500},
			PositionData:    &repository.PositionData{PageDelimiters: []uint32{100, 200}},
			PipelineRelease: service.PipelineRelease{},
		}, nil)

	env.ExecuteWorkflow(worker.ConvertToMarkdownFileWorkflow, ConvertToMarkdownFileWorkflowParam{
		FileUID:          fileUID,
		KnowledgeBaseUID: kbUID,
		Bucket:           "test-bucket",
		Destination:      "test/file.pdf",
		FileType:         artifactpb.FileType_FILE_TYPE_PDF,
		Filename:         "file.pdf",
	})

	c.Assert(env.IsWorkflowCompleted(), qt.IsTrue)
	c.Assert(env.GetWorkflowError(), qt.IsNil)

	var result ConvertToMarkdownFileWorkflowResult
	err := env.GetWorkflowResult(&result)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Markdown, qt.Equals, "# Converted Markdown")
	c.Assert(result.ConvertedType, qt.Equals, artifactpb.FileType_FILE_TYPE_PDF)
	c.Assert(result.OriginalType, qt.Equals, artifactpb.FileType_FILE_TYPE_PDF)
	c.Assert(result.FormatConverted, qt.IsFalse)
}

func TestConvertToMarkdownFileWorkflow_WithFormatConversion(t *testing.T) {
	t.Skip("TODO: Workflow tests need service-level mocking - see minio_workflow_test.go for pattern. Activities are tested.")
	c := qt.New(t)
	mc := minimock.NewController(t)

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	mockService := mock.NewServiceMock(mc)
	logger := zap.NewNop()

	worker := &Worker{
		service: mockService,
		log:     logger,
	}

	fileUID := uuid.Must(uuid.NewV4())
	kbUID := uuid.Must(uuid.NewV4())

	// Mock ConvertFileTypeActivity - DOCX converted to PDF
	env.OnActivity(worker.ConvertFileTypeActivity).
		Return(&ConvertFileTypeActivityResult{
			ConvertedDestination: "tmp/" + fileUID.String() + "/converted.pdf",
			ConvertedBucket:      "test-bucket",
			ConvertedType:        artifactpb.FileType_FILE_TYPE_PDF,
			OriginalType:         artifactpb.FileType_FILE_TYPE_DOCX,
			Converted:            true,
			PipelineRelease:      service.PipelineRelease{},
		}, nil)

	// Mock CacheContextActivity
	env.OnActivity(worker.CacheContextActivity).
		Return(&CacheContextActivityResult{
			CacheName:    "test-cache-456",
			Model:        "gemini-2.0-flash",
			CreateTime:   time.Now(),
			ExpireTime:   time.Now().Add(5 * time.Minute),
			CacheEnabled: true,
		}, nil)

	// Mock DeleteCacheActivity
	env.OnActivity(worker.DeleteCacheActivity).
		Return(nil)

	// Mock DeleteTemporaryConvertedFileActivity
	env.OnActivity(worker.DeleteTemporaryConvertedFileActivity).
		Return(nil)

	// Mock ConvertToMarkdownFileActivity
	env.OnActivity(worker.ConvertToMarkdownFileActivity).
		Return(&ConvertToMarkdownFileActivityResult{
			Markdown:        "# Document Content",
			Length:          []uint32{1000},
			PositionData:    &repository.PositionData{PageDelimiters: []uint32{200, 400, 600}},
			PipelineRelease: service.PipelineRelease{},
		}, nil)

	env.ExecuteWorkflow(worker.ConvertToMarkdownFileWorkflow, ConvertToMarkdownFileWorkflowParam{
		FileUID:          fileUID,
		KnowledgeBaseUID: kbUID,
		Bucket:           "test-bucket",
		Destination:      "test/file.docx",
		FileType:         artifactpb.FileType_FILE_TYPE_DOCX,
		Filename:         "file.docx",
	})

	c.Assert(env.IsWorkflowCompleted(), qt.IsTrue)
	c.Assert(env.GetWorkflowError(), qt.IsNil)

	var result ConvertToMarkdownFileWorkflowResult
	err := env.GetWorkflowResult(&result)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Markdown, qt.Equals, "# Document Content")
	c.Assert(result.ConvertedType, qt.Equals, artifactpb.FileType_FILE_TYPE_PDF)
	c.Assert(result.OriginalType, qt.Equals, artifactpb.FileType_FILE_TYPE_DOCX)
	c.Assert(result.FormatConverted, qt.IsTrue)
}

func TestConvertToMarkdownFileWorkflow_ConversionFailureContinues(t *testing.T) {
	t.Skip("TODO: Workflow tests need service-level mocking - see minio_workflow_test.go for pattern. Activities are tested.")
	c := qt.New(t)
	mc := minimock.NewController(t)

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	mockService := mock.NewServiceMock(mc)
	logger := zap.NewNop()

	worker := &Worker{
		service: mockService,
		log:     logger,
	}

	fileUID := uuid.Must(uuid.NewV4())
	kbUID := uuid.Must(uuid.NewV4())

	// Mock ConvertFileTypeActivity - conversion fails but workflow continues
	env.OnActivity(worker.ConvertFileTypeActivity).
		Return(nil, errors.New("conversion pipeline unavailable"))

	// Mock CacheContextActivity
	env.OnActivity(worker.CacheContextActivity).
		Return(&CacheContextActivityResult{
			CacheEnabled: false, // No cache since format conversion failed
		}, nil)

	// Mock DeleteCacheActivity (won't be called since no cache)
	env.OnActivity(worker.DeleteCacheActivity).
		Return(nil)

	// Mock DeleteTemporaryConvertedFileActivity
	env.OnActivity(worker.DeleteTemporaryConvertedFileActivity).
		Return(nil)

	// Mock ConvertToMarkdownFileActivity - should still work with original file
	env.OnActivity(worker.ConvertToMarkdownFileActivity).
		Return(&ConvertToMarkdownFileActivityResult{
			Markdown:        "# Original File Content",
			Length:          []uint32{800},
			PositionData:    nil,
			PipelineRelease: service.PipelineRelease{},
		}, nil)

	env.ExecuteWorkflow(worker.ConvertToMarkdownFileWorkflow, ConvertToMarkdownFileWorkflowParam{
		FileUID:          fileUID,
		KnowledgeBaseUID: kbUID,
		Bucket:           "test-bucket",
		Destination:      "test/file.docx",
		FileType:         artifactpb.FileType_FILE_TYPE_DOCX,
		Filename:         "file.docx",
	})

	c.Assert(env.IsWorkflowCompleted(), qt.IsTrue)
	c.Assert(env.GetWorkflowError(), qt.IsNil)

	var result ConvertToMarkdownFileWorkflowResult
	err := env.GetWorkflowResult(&result)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Markdown, qt.Equals, "# Original File Content")
}

func TestConvertToMarkdownFileWorkflow_CacheFailureContinues(t *testing.T) {
	t.Skip("TODO: Workflow tests need service-level mocking - see minio_workflow_test.go for pattern. Activities are tested.")
	c := qt.New(t)
	mc := minimock.NewController(t)

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	mockService := mock.NewServiceMock(mc)
	logger := zap.NewNop()

	worker := &Worker{
		service: mockService,
		log:     logger,
	}

	fileUID := uuid.Must(uuid.NewV4())
	kbUID := uuid.Must(uuid.NewV4())

	// Mock ConvertFileTypeActivity
	env.OnActivity(worker.ConvertFileTypeActivity).
		Return(&ConvertFileTypeActivityResult{
			ConvertedDestination: "",
			ConvertedBucket:      "",
			ConvertedType:        artifactpb.FileType_FILE_TYPE_PDF,
			OriginalType:         artifactpb.FileType_FILE_TYPE_PDF,
			Converted:            false,
			PipelineRelease:      service.PipelineRelease{},
		}, nil)

	// Mock CacheContextActivity - cache creation fails
	env.OnActivity(worker.CacheContextActivity).
		Return(nil, errors.New("cache service unavailable"))

	// Mock ConvertToMarkdownFileActivity - should still work without cache
	env.OnActivity(worker.ConvertToMarkdownFileActivity).
		Return(&ConvertToMarkdownFileActivityResult{
			Markdown:        "# Content Without Cache",
			Length:          []uint32{600},
			PositionData:    nil,
			PipelineRelease: service.PipelineRelease{},
		}, nil)

	env.ExecuteWorkflow(worker.ConvertToMarkdownFileWorkflow, ConvertToMarkdownFileWorkflowParam{
		FileUID:          fileUID,
		KnowledgeBaseUID: kbUID,
		Bucket:           "test-bucket",
		Destination:      "test/file.pdf",
		FileType:         artifactpb.FileType_FILE_TYPE_PDF,
		Filename:         "file.pdf",
	})

	c.Assert(env.IsWorkflowCompleted(), qt.IsTrue)
	c.Assert(env.GetWorkflowError(), qt.IsNil)

	var result ConvertToMarkdownFileWorkflowResult
	err := env.GetWorkflowResult(&result)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Markdown, qt.Equals, "# Content Without Cache")
}

func TestConvertToMarkdownFileWorkflow_MarkdownConversionFailure(t *testing.T) {
	t.Skip("TODO: Workflow tests need service-level mocking - see minio_workflow_test.go for pattern. Activities are tested.")
	c := qt.New(t)
	mc := minimock.NewController(t)

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	mockService := mock.NewServiceMock(mc)
	logger := zap.NewNop()

	worker := &Worker{
		service: mockService,
		log:     logger,
	}

	fileUID := uuid.Must(uuid.NewV4())
	kbUID := uuid.Must(uuid.NewV4())

	// Mock ConvertFileTypeActivity
	env.OnActivity(worker.ConvertFileTypeActivity).
		Return(&ConvertFileTypeActivityResult{
			ConvertedDestination: "",
			ConvertedBucket:      "",
			ConvertedType:        artifactpb.FileType_FILE_TYPE_PDF,
			OriginalType:         artifactpb.FileType_FILE_TYPE_PDF,
			Converted:            false,
			PipelineRelease:      service.PipelineRelease{},
		}, nil)

	// Mock CacheContextActivity
	env.OnActivity(worker.CacheContextActivity).
		Return(&CacheContextActivityResult{
			CacheName:    "test-cache",
			CacheEnabled: true,
		}, nil)

	// Mock DeleteCacheActivity - should still be called even if conversion fails
	env.OnActivity(worker.DeleteCacheActivity).
		Return(nil)

	// Mock DeleteTemporaryConvertedFileActivity
	env.OnActivity(worker.DeleteTemporaryConvertedFileActivity).
		Return(nil)

	// Mock ConvertToMarkdownFileActivity - conversion fails
	env.OnActivity(worker.ConvertToMarkdownFileActivity).
		Return(nil, errors.New("markdown conversion failed"))

	env.ExecuteWorkflow(worker.ConvertToMarkdownFileWorkflow, ConvertToMarkdownFileWorkflowParam{
		FileUID:          fileUID,
		KnowledgeBaseUID: kbUID,
		Bucket:           "test-bucket",
		Destination:      "test/file.pdf",
		FileType:         artifactpb.FileType_FILE_TYPE_PDF,
		Filename:         "file.pdf",
	})

	c.Assert(env.IsWorkflowCompleted(), qt.IsTrue)
	c.Assert(env.GetWorkflowError(), qt.Not(qt.IsNil))
	c.Assert(env.GetWorkflowError().Error(), qt.Contains, "markdown conversion failed")
}

func TestConvertToMarkdownFileWorkflow_NoCacheCreated(t *testing.T) {
	t.Skip("TODO: Workflow tests need service-level mocking - see minio_workflow_test.go for pattern. Activities are tested.")
	c := qt.New(t)
	mc := minimock.NewController(t)

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	mockService := mock.NewServiceMock(mc)
	logger := zap.NewNop()

	worker := &Worker{
		service: mockService,
		log:     logger,
	}

	fileUID := uuid.Must(uuid.NewV4())
	kbUID := uuid.Must(uuid.NewV4())

	// Mock ConvertFileTypeActivity
	env.OnActivity(worker.ConvertFileTypeActivity).
		Return(&ConvertFileTypeActivityResult{
			ConvertedDestination: "",
			ConvertedBucket:      "",
			ConvertedType:        artifactpb.FileType_FILE_TYPE_TEXT,
			OriginalType:         artifactpb.FileType_FILE_TYPE_TEXT,
			Converted:            false,
			PipelineRelease:      service.PipelineRelease{},
		}, nil)

	// Mock CacheContextActivity - no cache created (unsupported file type)
	env.OnActivity(worker.CacheContextActivity).
		Return(&CacheContextActivityResult{
			CacheEnabled: false,
		}, nil)

	// DeleteCacheActivity won't be called since no cache was created
	env.OnActivity(worker.DeleteCacheActivity).
		Return(nil)

	// Mock DeleteTemporaryConvertedFileActivity
	env.OnActivity(worker.DeleteTemporaryConvertedFileActivity).
		Return(nil)

	// Mock ConvertToMarkdownFileActivity
	env.OnActivity(worker.ConvertToMarkdownFileActivity).
		Return(&ConvertToMarkdownFileActivityResult{
			Markdown:        "# Text File Content",
			Length:          []uint32{100},
			PositionData:    nil,
			PipelineRelease: service.PipelineRelease{},
		}, nil)

	env.ExecuteWorkflow(worker.ConvertToMarkdownFileWorkflow, ConvertToMarkdownFileWorkflowParam{
		FileUID:          fileUID,
		KnowledgeBaseUID: kbUID,
		Bucket:           "test-bucket",
		Destination:      "test/file.txt",
		FileType:         artifactpb.FileType_FILE_TYPE_TEXT,
		Filename:         "file.txt",
	})

	c.Assert(env.IsWorkflowCompleted(), qt.IsTrue)
	c.Assert(env.GetWorkflowError(), qt.IsNil)

	var result ConvertToMarkdownFileWorkflowResult
	err := env.GetWorkflowResult(&result)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Markdown, qt.Equals, "# Text File Content")
}
