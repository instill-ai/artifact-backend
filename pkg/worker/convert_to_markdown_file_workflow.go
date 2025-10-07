package worker

import (
	"time"

	"github.com/gofrs/uuid"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/service"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

// ConvertToMarkdownFileWorkflowParam defines the input parameters for ConvertToMarkdownFileWorkflow
type ConvertToMarkdownFileWorkflowParam struct {
	FileUID          uuid.UUID
	KnowledgeBaseUID uuid.UUID
	Bucket           string
	Destination      string
	FileType         artifactpb.FileType
	Filename         string
	Pipelines        []service.PipelineRelease // For both format conversion and markdown conversion
	Metadata         *structpb.Struct
}

// ConvertToMarkdownFileWorkflowResult defines the output of ConvertToMarkdownFileWorkflow
type ConvertToMarkdownFileWorkflowResult struct {
	Markdown        string
	Length          []uint32
	PositionData    *repository.PositionData
	PipelineRelease service.PipelineRelease
	ConvertedType   artifactpb.FileType // The file type used for conversion (after format conversion)
	OriginalType    artifactpb.FileType // The original file type
	FormatConverted bool                // Whether format conversion was performed
}

// ConvertToMarkdownFileWorkflow is a child workflow that handles the complete process of
// converting a file to Markdown, including:
// 1. Format conversion (if needed) - converts non-Gemini-native formats
// 2. Caching - creates Gemini cache for efficient processing
// 3. Markdown conversion - converts the file to Markdown using Gemini or pipelines
//
// This workflow encapsulates the entire conversion pipeline and can be optimized independently.
func (w *Worker) ConvertToMarkdownFileWorkflow(ctx workflow.Context, param ConvertToMarkdownFileWorkflowParam) (*ConvertToMarkdownFileWorkflowResult, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("ConvertToMarkdownFileWorkflow started",
		"fileUID", param.FileUID.String(),
		"fileType", param.FileType.String())

	// Configure activity options for all activities in this workflow
	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    3,
		},
	})

	// Step 1: Convert file format if needed for Gemini (DOCX→PDF, HEIC→JPEG, etc.)
	// This uses ConvertFileTypePipeline specifically for AI preprocessing
	var convertResult ConvertFileTypeActivityResult
	if err := workflow.ExecuteActivity(activityCtx, w.ConvertFileTypeActivity, &ConvertFileTypeActivityParam{
		FileUID:          param.FileUID,
		KnowledgeBaseUID: param.KnowledgeBaseUID,
		Bucket:           param.Bucket,
		Destination:      param.Destination,
		FileType:         param.FileType,
		Filename:         param.Filename,
		Pipelines:        []service.PipelineRelease{service.ConvertFileTypePipeline},
		Metadata:         param.Metadata,
	}).Get(activityCtx, &convertResult); err != nil {
		// Format conversion failure is not fatal - continue with original file
		logger.Warn("Format conversion failed, continuing with original file",
			"error", err,
			"fileType", param.FileType.String())
		convertResult.Converted = false
		convertResult.ConvertedType = param.FileType
		convertResult.OriginalType = param.FileType
	}

	// Determine the effective file type and location (converted or original)
	effectiveFileType := convertResult.ConvertedType
	effectiveBucket := param.Bucket
	effectiveDestination := param.Destination

	if convertResult.Converted {
		logger.Info("File format converted successfully",
			"originalType", convertResult.OriginalType.String(),
			"convertedType", effectiveFileType.String(),
			"pipeline", convertResult.PipelineRelease.Name(),
			"convertedDestination", convertResult.ConvertedDestination)

		// Use converted file's location
		effectiveBucket = convertResult.ConvertedBucket
		effectiveDestination = convertResult.ConvertedDestination
	}

	// Step 2: Create Gemini cache for efficient processing
	var cacheResult CacheContextActivityResult
	if err := workflow.ExecuteActivity(activityCtx, w.CacheContextActivity, &CacheContextActivityParam{
		FileUID:          param.FileUID,
		KnowledgeBaseUID: param.KnowledgeBaseUID,
		Bucket:           effectiveBucket,      // Use converted file's bucket if conversion happened
		Destination:      effectiveDestination, // Use converted file's destination if conversion happened
		FileType:         effectiveFileType,
		Filename:         param.Filename,
		Metadata:         param.Metadata,
	}).Get(activityCtx, &cacheResult); err != nil {
		// Cache creation is optional - log and continue without cache
		logger.Warn("Cache creation failed, continuing without cache", "error", err)
		cacheResult.CacheEnabled = false
	}

	// Schedule cache cleanup if cache was created
	var cacheName string
	if cacheResult.CacheEnabled {
		cacheName = cacheResult.CacheName
		logger.Info("Cache created successfully", "cacheName", cacheName)

		// Defer cache cleanup to ensure it runs at the end
		defer func() {
			// Use disconnected context for cleanup to ensure it runs even if workflow is cancelled
			cleanupCtx, _ := workflow.NewDisconnectedContext(ctx)
			cleanupCtx = workflow.WithActivityOptions(cleanupCtx, workflow.ActivityOptions{
				StartToCloseTimeout: time.Minute,
				RetryPolicy: &temporal.RetryPolicy{
					InitialInterval:    time.Second,
					BackoffCoefficient: 2.0,
					MaximumInterval:    time.Minute,
					MaximumAttempts:    3,
				},
			})

			if err := workflow.ExecuteActivity(cleanupCtx, w.DeleteCacheActivity, &DeleteCacheActivityParam{
				CacheName: cacheName,
			}).Get(cleanupCtx, nil); err != nil {
				logger.Warn("Cache cleanup failed", "error", err, "cacheName", cacheName)
			} else {
				logger.Info("Cache cleaned up successfully", "cacheName", cacheName)
			}
		}()
	}

	// Schedule cleanup of temporary converted file if conversion happened
	if convertResult.Converted && convertResult.ConvertedDestination != "" {
		logger.Info("Temporary converted file will be cleaned up at workflow end",
			"destination", convertResult.ConvertedDestination)

		// Defer cleanup to ensure it runs at the end (after markdown conversion)
		defer func() {
			cleanupCtx, _ := workflow.NewDisconnectedContext(ctx)
			cleanupCtx = workflow.WithActivityOptions(cleanupCtx, workflow.ActivityOptions{
				StartToCloseTimeout: time.Minute,
				RetryPolicy: &temporal.RetryPolicy{
					InitialInterval:    time.Second,
					BackoffCoefficient: 2.0,
					MaximumInterval:    30 * time.Second,
					MaximumAttempts:    3,
				},
			})

			if err := workflow.ExecuteActivity(cleanupCtx, w.DeleteTemporaryConvertedFileActivity, &DeleteTemporaryConvertedFileActivityParam{
				Bucket:      convertResult.ConvertedBucket,
				Destination: convertResult.ConvertedDestination,
			}).Get(cleanupCtx, nil); err != nil {
				logger.Warn("Temporary file cleanup failed (file will remain in MinIO)",
					"error", err,
					"destination", convertResult.ConvertedDestination)
			} else {
				logger.Info("Temporary converted file cleaned up successfully",
					"destination", convertResult.ConvertedDestination)
			}
		}()
	}

	// Step 3: Convert to Markdown using Gemini or pipelines
	var conversionResult ConvertToMarkdownFileActivityResult
	if err := workflow.ExecuteActivity(activityCtx, w.ConvertToMarkdownFileActivity, &ConvertToMarkdownFileActivityParam{
		Bucket:      effectiveBucket,      // Use converted file's bucket if conversion happened
		Destination: effectiveDestination, // Use converted file's destination if conversion happened
		FileType:    effectiveFileType,
		Pipelines:   param.Pipelines,
		Metadata:    param.Metadata,
		CacheName:   cacheName,
	}).Get(activityCtx, &conversionResult); err != nil {
		logger.Error("Markdown conversion failed", "error", err)
		return nil, err
	}

	logger.Info("ConvertToMarkdownFileWorkflow completed successfully",
		"fileUID", param.FileUID.String(),
		"markdownLength", len(conversionResult.Markdown))

	return &ConvertToMarkdownFileWorkflowResult{
		Markdown:        conversionResult.Markdown,
		Length:          conversionResult.Length,
		PositionData:    conversionResult.PositionData,
		PipelineRelease: conversionResult.PipelineRelease,
		ConvertedType:   effectiveFileType,
		OriginalType:    convertResult.OriginalType,
		FormatConverted: convertResult.Converted,
	}, nil
}
