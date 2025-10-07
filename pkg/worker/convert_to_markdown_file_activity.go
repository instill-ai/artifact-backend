package worker

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gofrs/uuid"
	"go.temporal.io/sdk/temporal"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/instill-ai/artifact-backend/internal/ai"
	"github.com/instill-ai/artifact-backend/pkg/minio"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/service"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	pipelinepb "github.com/instill-ai/protogen-go/pipeline/pipeline/v1beta"
	errorsx "github.com/instill-ai/x/errors"
)

// This file contains the activities used by ConvertToMarkdownFileWorkflow:
// - ConvertFileTypeActivity - Converts file formats (GIF→PNG, MKV→MP4, etc.)
// - CacheContextActivity - Creates Gemini cache for efficient processing
// - DeleteCacheActivity - Cleans up Gemini cache
// - DeleteTemporaryConvertedFileActivity - Cleans up temporary converted files from MinIO
// - ConvertToMarkdownFileActivity - Converts files to Markdown

// ===== FILE TYPE CONVERSION ACTIVITY =====

// ConvertFileTypeActivityParam defines the parameters for ConvertFileTypeActivity
type ConvertFileTypeActivityParam struct {
	FileUID          uuid.UUID
	KnowledgeBaseUID uuid.UUID
	Bucket           string
	Destination      string
	FileType         artifactpb.FileType
	Filename         string
	Pipelines        []service.PipelineRelease // convert-file-type pipeline
	Metadata         *structpb.Struct
}

// ConvertFileTypeActivityResult defines the result of ConvertFileTypeActivity
type ConvertFileTypeActivityResult struct {
	ConvertedDestination string                  // MinIO path to converted file (empty if no conversion)
	ConvertedBucket      string                  // MinIO bucket for converted file (empty if no conversion)
	ConvertedType        artifactpb.FileType     // New file type after conversion
	OriginalType         artifactpb.FileType     // Original file type
	Converted            bool                    // Whether conversion was performed
	PipelineRelease      service.PipelineRelease // Pipeline used for conversion
}

// ConvertFileTypeActivity converts non-Gemini-native file types to Gemini-supported formats
// Following pipeline-backend/pkg/component/ai/gemini/v0/common.go format mappings
func (w *Worker) ConvertFileTypeActivity(ctx context.Context, param *ConvertFileTypeActivityParam) (*ConvertFileTypeActivityResult, error) {
	w.log.Info("ConvertFileTypeActivity: Checking if file needs conversion",
		zap.String("fileType", param.FileType.String()),
		zap.String("filename", param.Filename))

	// Check if file needs conversion
	needsConversion, targetFormat := needsFileConversion(param.FileType)
	if !needsConversion {
		w.log.Info("ConvertFileTypeActivity: File type is Gemini-native, no conversion needed",
			zap.String("fileType", param.FileType.String()))
		return &ConvertFileTypeActivityResult{
			ConvertedDestination: "", // No conversion, use original file
			ConvertedBucket:      "",
			ConvertedType:        param.FileType,
			OriginalType:         param.FileType,
			Converted:            false,
		}, nil
	}

	w.log.Info("ConvertFileTypeActivity: File type needs conversion",
		zap.String("originalType", param.FileType.String()),
		zap.String("targetFormat", targetFormat))

	// Create authenticated context if metadata provided
	authCtx := ctx
	if param.Metadata != nil {
		var err error
		authCtx, err = createAuthenticatedContext(ctx, param.Metadata)
		if err != nil {
			return nil, temporal.NewApplicationErrorWithCause(
				fmt.Sprintf("Failed to create authenticated context: %s", errorsx.MessageOrErr(err)),
				convertFileTypeActivityError,
				err,
			)
		}
	}

	// Fetch original file content from MinIO
	content, err := w.service.MinIO().GetFile(authCtx, param.Bucket, param.Destination)
	if err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to retrieve file from storage: %s", errorsx.MessageOrErr(err)),
			convertFileTypeActivityError,
			err,
		)
	}

	w.log.Info("ConvertFileTypeActivity: File content retrieved from MinIO",
		zap.Int("contentSize", len(content)))

	// Determine which pipeline to use and prepare input
	var convertedContent []byte
	var usedPipeline service.PipelineRelease

	// Try convert-file-type pipeline
	if len(param.Pipelines) > 0 {
		convertedContent, usedPipeline, err = w.convertUsingPipeline(authCtx, content, param.FileType, targetFormat, param.Pipelines)
		if err != nil {
			w.log.Warn("ConvertFileTypeActivity: Pipeline conversion failed",
				zap.Error(err),
				zap.String("pipeline", usedPipeline.Name()))
			return nil, temporal.NewApplicationErrorWithCause(
				fmt.Sprintf("Failed to convert file: %s", errorsx.MessageOrErr(err)),
				convertFileTypeActivityError,
				err,
			)
		}
	} else {
		return nil, temporal.NewApplicationError(
			"No conversion pipeline available",
			convertFileTypeActivityError,
		)
	}

	w.log.Info("ConvertFileTypeActivity: File conversion successful",
		zap.String("originalType", param.FileType.String()),
		zap.String("convertedType", targetFormat),
		zap.Int("originalSize", len(content)),
		zap.Int("convertedSize", len(convertedContent)))

	// Map target format string to FileType enum
	convertedFileType := mapFormatToFileType(targetFormat)

	// Upload converted content to MinIO (avoid passing large blobs through Temporal)
	// Use a temporary path in the blob bucket for intermediate conversion results
	// Generate unique destination for converted file in tmp directory
	convertedDestination := fmt.Sprintf("tmp/%s/%s.%s",
		param.FileUID.String(),
		uuid.Must(uuid.NewV4()).String(),
		targetFormat)

	// Encode content to base64 for MinIO upload
	base64Content := base64.StdEncoding.EncodeToString(convertedContent)

	// Get MIME type for the converted file type
	mimeType := ai.FileTypeToMIME(convertedFileType)

	err = w.service.MinIO().UploadBase64File(authCtx, minio.BlobBucketName, convertedDestination, base64Content, mimeType)
	if err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to upload converted file to MinIO: %s", errorsx.MessageOrErr(err)),
			convertFileTypeActivityError,
			err,
		)
	}

	w.log.Info("ConvertFileTypeActivity: Converted file uploaded to MinIO",
		zap.String("destination", convertedDestination),
		zap.String("bucket", minio.BlobBucketName),
		zap.String("mimeType", mimeType))

	return &ConvertFileTypeActivityResult{
		ConvertedDestination: convertedDestination,
		ConvertedBucket:      minio.BlobBucketName,
		ConvertedType:        convertedFileType,
		OriginalType:         param.FileType,
		Converted:            true,
		PipelineRelease:      usedPipeline,
	}, nil
}

// ===== CACHE CONTEXT ACTIVITY =====

// CacheContextActivityParam defines the parameters for CacheContextActivity
type CacheContextActivityParam struct {
	FileUID          uuid.UUID
	KnowledgeBaseUID uuid.UUID
	Bucket           string // MinIO bucket (original or converted file)
	Destination      string // MinIO path (original or converted file)
	FileType         artifactpb.FileType
	Filename         string
	Metadata         *structpb.Struct
}

// CacheContextActivityResult defines the result of CacheContextActivity
type CacheContextActivityResult struct {
	CacheName  string
	Model      string
	CreateTime time.Time
	ExpireTime time.Time
	// Flag indicating if cache was created (false means caching is disabled)
	CacheEnabled bool
}

// CacheContextActivity creates a cached context for the input file
// This enables efficient subsequent operations like conversion, analysis, etc.
// Supports: Documents (PDF, DOCX, DOC, PPTX, PPT), Images, Audio, Video
func (w *Worker) CacheContextActivity(ctx context.Context, param *CacheContextActivityParam) (*CacheContextActivityResult, error) {
	w.log.Info("CacheContextActivity: Creating cache for input content",
		zap.String("fileUID", param.FileUID.String()),
		zap.String("fileType", param.FileType.String()),
		zap.String("destination", param.Destination))

	// Check if AI provider is available
	if w.aiProvider == nil {
		w.log.Info("CacheContextActivity: AI provider not available, skipping cache creation")
		return &CacheContextActivityResult{
			CacheEnabled: false,
		}, nil
	}

	// Check if file type is supported for caching
	if !w.aiProvider.SupportsFileType(param.FileType) {
		w.log.Info("CacheContextActivity: File type not supported for caching",
			zap.String("fileType", param.FileType.String()))
		return &CacheContextActivityResult{
			CacheEnabled: false,
		}, nil
	}

	// Create authenticated context if metadata provided
	authCtx := ctx
	if param.Metadata != nil {
		var err error
		authCtx, err = createAuthenticatedContext(ctx, param.Metadata)
		if err != nil {
			w.log.Warn("CacheContextActivity: Failed to create authenticated context, proceeding without auth",
				zap.Error(err))
		}
	}

	// Fetch file content from MinIO (either original or converted file)
	content, err := w.service.MinIO().GetFile(authCtx, param.Bucket, param.Destination)
	if err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to retrieve file from storage: %s", errorsx.MessageOrErr(err)),
			cacheContextActivityError,
			err,
		)
	}

	w.log.Info("CacheContextActivity: File content retrieved from MinIO",
		zap.String("bucket", param.Bucket),
		zap.String("destination", param.Destination),
		zap.Int("contentSize", len(content)))

	// Set cache TTL (5 minutes default)
	cacheTTL := 5 * time.Minute

	// Create the cache using AI provider
	// Note: Cache creation is optional - if it fails, we continue without cache
	cacheOutput, err := w.aiProvider.CreateCache(authCtx, content, param.FileType, param.Filename, cacheTTL)
	if err != nil {
		// Log the error but don't fail the activity
		// Common reasons for cache failure:
		// - Content too small (< 1024 tokens minimum)
		// - Content too large (> maximum cache size)
		// - API quota exceeded
		// - Network issues
		w.log.Warn("CacheContextActivity: Cache creation failed, continuing without cache",
			zap.Error(err),
			zap.String("fileUID", param.FileUID.String()),
			zap.String("fileType", param.FileType.String()))

		return &CacheContextActivityResult{
			CacheEnabled: false,
		}, nil
	}

	w.log.Info("CacheContextActivity: Cache created successfully",
		zap.String("cacheName", cacheOutput.CacheName),
		zap.String("model", cacheOutput.Model),
		zap.Time("expireTime", cacheOutput.ExpireTime))

	return &CacheContextActivityResult{
		CacheName:    cacheOutput.CacheName,
		Model:        cacheOutput.Model,
		CreateTime:   cacheOutput.CreateTime,
		ExpireTime:   cacheOutput.ExpireTime,
		CacheEnabled: true,
	}, nil
}

// DeleteCacheActivityParam defines the parameters for DeleteCacheActivity
type DeleteCacheActivityParam struct {
	CacheName string
}

// DeleteCacheActivity deletes a cached context
func (w *Worker) DeleteCacheActivity(ctx context.Context, param *DeleteCacheActivityParam) error {
	if param.CacheName == "" {
		w.log.Info("DeleteCacheActivity: No cache name provided, skipping deletion")
		return nil
	}

	w.log.Info("DeleteCacheActivity: Deleting cache",
		zap.String("cacheName", param.CacheName))

	// Check if AI provider is available
	if w.aiProvider == nil {
		w.log.Warn("DeleteCacheActivity: AI provider not available, cannot delete cache")
		return nil
	}

	err := w.aiProvider.DeleteCache(ctx, param.CacheName)
	if err != nil {
		// Log error but don't fail the activity - cache will expire automatically
		w.log.Warn("DeleteCacheActivity: Failed to delete cache (will expire automatically)",
			zap.String("cacheName", param.CacheName),
			zap.Error(err))
		return nil
	}

	w.log.Info("DeleteCacheActivity: Cache deleted successfully",
		zap.String("cacheName", param.CacheName))

	return nil
}

// DeleteTemporaryConvertedFileActivityParam defines the parameters for DeleteTemporaryConvertedFileActivity
type DeleteTemporaryConvertedFileActivityParam struct {
	Bucket      string // MinIO bucket (usually blob)
	Destination string // MinIO path to temporary converted file
}

// DeleteTemporaryConvertedFileActivity deletes a temporary converted file from MinIO
// This cleans up intermediate conversion results (e.g., core-blob/tmp/fileUID/uuid.pdf) after they've been used
// Note: This is different from DeleteConvertedFileActivity which deletes the final markdown from DB
func (w *Worker) DeleteTemporaryConvertedFileActivity(ctx context.Context, param *DeleteTemporaryConvertedFileActivityParam) error {
	if param.Destination == "" {
		w.log.Info("DeleteTemporaryConvertedFileActivity: No destination provided, skipping deletion")
		return nil
	}

	w.log.Info("DeleteTemporaryConvertedFileActivity: Deleting temporary converted file",
		zap.String("bucket", param.Bucket),
		zap.String("destination", param.Destination))

	err := w.service.MinIO().DeleteFile(ctx, param.Bucket, param.Destination)
	if err != nil {
		// Log error but don't fail the activity - temporary files can accumulate but won't break functionality
		w.log.Warn("DeleteTemporaryConvertedFileActivity: Failed to delete temporary file (will remain in MinIO)",
			zap.String("destination", param.Destination),
			zap.Error(err))
		return nil
	}

	w.log.Info("DeleteTemporaryConvertedFileActivity: Temporary file deleted successfully",
		zap.String("destination", param.Destination))

	return nil
}

// ===== MARKDOWN CONVERSION ACTIVITY =====

// ConvertToMarkdownFileActivityParam for external pipeline conversion call
type ConvertToMarkdownFileActivityParam struct {
	Bucket      string // MinIO bucket
	Destination string // MinIO file path
	FileType    artifactpb.FileType
	Pipelines   []service.PipelineRelease
	Metadata    *structpb.Struct // For authentication context
	CacheName   string           // Optional: Gemini cache name for efficient conversion
}

// ConvertToMarkdownFileActivityResult contains the conversion result
type ConvertToMarkdownFileActivityResult struct {
	Markdown        string
	PositionData    *repository.PositionData
	Length          []uint32
	PipelineRelease service.PipelineRelease
}

// ConvertToMarkdownFileActivity calls external pipeline to convert file to markdown
// For TEXT/MARKDOWN files, it extracts the file length without calling the pipeline
// This is a single external API call - idempotent (pipeline should be idempotent)
// Note: This activity fetches content directly from MinIO to avoid passing large files through Temporal
func (w *Worker) ConvertToMarkdownFileActivity(ctx context.Context, param *ConvertToMarkdownFileActivityParam) (*ConvertToMarkdownFileActivityResult, error) {
	w.log.Info("ConvertToMarkdownFileActivity: Converting file to markdown",
		zap.String("fileType", param.FileType.String()),
		zap.String("destination", param.Destination),
		zap.Int("pipelineCount", len(param.Pipelines)))

	// Create authenticated context if metadata provided
	authCtx := ctx
	if param.Metadata != nil {
		var err error
		authCtx, err = createAuthenticatedContext(ctx, param.Metadata)
		if err != nil {
			return nil, temporal.NewApplicationErrorWithCause(
				fmt.Sprintf("Failed to create authenticated context: %s", errorsx.MessageOrErr(err)),
				convertToMarkdownFileActivityError,
				err,
			)
		}
	}

	// Fetch file content directly from MinIO (avoids passing large files through Temporal)
	content, err := w.service.MinIO().GetFile(authCtx, param.Bucket, param.Destination)
	if err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to retrieve file from storage: %s", errorsx.MessageOrErr(err)),
			convertToMarkdownFileActivityError,
			err,
		)
	}

	w.log.Info("ConvertToMarkdownFileActivity: File content retrieved from MinIO",
		zap.Int("contentSize", len(content)))

	// Try AI provider conversion first for supported file types (faster and more efficient)
	if w.aiProvider != nil && w.aiProvider.SupportsFileType(param.FileType) {
		// If cache name is provided, use cached context for much faster conversion
		if param.CacheName != "" {
			w.log.Info("ConvertToMarkdownFileActivity: Attempting AI conversion with cache",
				zap.String("fileType", param.FileType.String()),
				zap.String("cacheName", param.CacheName),
				zap.String("provider", w.aiProvider.Name()))

			// Pass empty prompt to let the AI provider use its own default prompt
			// (Gemini, OpenAI, and Anthropic have different optimal prompting styles)
			conversion, err := w.aiProvider.ConvertToMarkdownWithCache(
				authCtx,
				param.CacheName,
				"", // Use provider's default prompt
			)
			if err != nil {
				// Log the error but fall back to non-cached conversion
				w.log.Warn("ConvertToMarkdownFileActivity: Cached AI conversion failed, trying direct conversion",
					zap.Error(err))
			} else {
				w.log.Info("ConvertToMarkdownFileActivity: Cached AI conversion successful",
					zap.Int("markdownLength", len(conversion.Markdown)),
					zap.String("provider", conversion.Provider))

				return &ConvertToMarkdownFileActivityResult{
					Markdown:        conversion.Markdown,
					PositionData:    conversion.PositionData,
					Length:          conversion.Length,
					PipelineRelease: service.PipelineRelease{}, // No pipeline used (AI cached)
				}, nil
			}
		}

		// Try direct AI conversion (without cache)
		w.log.Info("ConvertToMarkdownFileActivity: Attempting direct AI conversion",
			zap.String("fileType", param.FileType.String()),
			zap.String("provider", w.aiProvider.Name()))

		// Extract filename from destination if not provided
		filename := param.Destination
		if idx := strings.LastIndex(filename, "/"); idx >= 0 {
			filename = filename[idx+1:]
		}

		conversion, err := w.aiProvider.ConvertToMarkdown(authCtx, content, param.FileType, filename)
		if err != nil {
			// Log the error but fall back to pipeline conversion
			w.log.Warn("ConvertToMarkdownFileActivity: AI conversion failed, falling back to pipeline",
				zap.Error(err))
		} else {
			w.log.Info("ConvertToMarkdownFileActivity: AI conversion successful",
				zap.Int("markdownLength", len(conversion.Markdown)),
				zap.String("provider", conversion.Provider))

			return &ConvertToMarkdownFileActivityResult{
				Markdown:        conversion.Markdown,
				PositionData:    conversion.PositionData,
				Length:          conversion.Length,
				PipelineRelease: service.PipelineRelease{}, // No pipeline used (AI direct)
			}, nil
		}
	}

	// Fall back to pipeline conversion
	w.log.Info("ConvertToMarkdownFileActivity: Using pipeline conversion",
		zap.String("fileType", param.FileType.String()))

	// Call the conversion pipeline - THIS IS THE KEY EXTERNAL CALL
	// Note: Use authCtx to pass authentication credentials to the pipeline service
	conversion, err := w.service.ConvertToMarkdownPipe(authCtx, service.MarkdownConversionParams{
		Base64Content: base64.StdEncoding.EncodeToString(content),
		Type:          param.FileType,
		Pipelines:     param.Pipelines,
	})
	if err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("File conversion failed: %s", errorsx.MessageOrErr(err)),
			convertToMarkdownFileActivityError,
			err,
		)
	}

	w.log.Info("ConvertToMarkdownFileActivity: Pipeline conversion successful",
		zap.Int("markdownLength", len(conversion.Markdown)))

	return &ConvertToMarkdownFileActivityResult{
		Markdown:        conversion.Markdown,
		PositionData:    conversion.PositionData,
		Length:          conversion.Length,
		PipelineRelease: conversion.PipelineRelease,
	}, nil
}

// ===== HELPER FUNCTIONS FOR FILE TYPE CONVERSION =====

// needsFileConversion checks if a file type needs conversion to Gemini-supported format
// Returns (needsConversion bool, targetFormat string)
// Based on pipeline-backend/pkg/component/ai/gemini/v0/common.go format definitions
func needsFileConversion(fileType artifactpb.FileType) (bool, string) {
	switch fileType {
	// Gemini-native image formats - no conversion needed
	case artifactpb.FileType_FILE_TYPE_PNG,
		artifactpb.FileType_FILE_TYPE_JPEG,
		artifactpb.FileType_FILE_TYPE_JPG,
		artifactpb.FileType_FILE_TYPE_WEBP,
		artifactpb.FileType_FILE_TYPE_HEIC,
		artifactpb.FileType_FILE_TYPE_HEIF:
		return false, ""

	// Convertible image formats - convert to PNG (widely supported)
	case artifactpb.FileType_FILE_TYPE_GIF,
		artifactpb.FileType_FILE_TYPE_BMP,
		artifactpb.FileType_FILE_TYPE_TIFF,
		artifactpb.FileType_FILE_TYPE_AVIF:
		return true, "png"

	// Gemini-native audio formats - no conversion needed
	case artifactpb.FileType_FILE_TYPE_MP3,
		artifactpb.FileType_FILE_TYPE_WAV,
		artifactpb.FileType_FILE_TYPE_AAC,
		artifactpb.FileType_FILE_TYPE_OGG,
		artifactpb.FileType_FILE_TYPE_FLAC,
		artifactpb.FileType_FILE_TYPE_AIFF:
		return false, ""

	// Convertible audio formats - convert to OGG (widely supported)
	case artifactpb.FileType_FILE_TYPE_M4A,
		artifactpb.FileType_FILE_TYPE_WMA:
		return true, "ogg"

	// Gemini-native video formats - no conversion needed
	case artifactpb.FileType_FILE_TYPE_MP4,
		artifactpb.FileType_FILE_TYPE_MPEG,
		artifactpb.FileType_FILE_TYPE_MOV,
		artifactpb.FileType_FILE_TYPE_AVI,
		artifactpb.FileType_FILE_TYPE_FLV,
		artifactpb.FileType_FILE_TYPE_WEBM_VIDEO,
		artifactpb.FileType_FILE_TYPE_WMV:
		return false, ""

	// Convertible video formats - convert to MP4 (widely supported)
	case artifactpb.FileType_FILE_TYPE_MKV:
		return true, "mp4"

	// Gemini-native document format - no conversion needed
	case artifactpb.FileType_FILE_TYPE_PDF:
		return false, ""

	// Convertible document formats - convert to PDF (only visual doc format supported by Gemini)
	case artifactpb.FileType_FILE_TYPE_DOC,
		artifactpb.FileType_FILE_TYPE_DOCX,
		artifactpb.FileType_FILE_TYPE_PPT,
		artifactpb.FileType_FILE_TYPE_PPTX,
		artifactpb.FileType_FILE_TYPE_XLS,
		artifactpb.FileType_FILE_TYPE_XLSX:
		return true, "pdf"

	// Text-based documents - no conversion needed (will be used as text)
	case artifactpb.FileType_FILE_TYPE_HTML,
		artifactpb.FileType_FILE_TYPE_TEXT,
		artifactpb.FileType_FILE_TYPE_MARKDOWN,
		artifactpb.FileType_FILE_TYPE_CSV:
		return false, ""

	default:
		return false, ""
	}
}

// convertUsingPipeline converts a file using the convert-file-type pipeline
func (w *Worker) convertUsingPipeline(ctx context.Context, content []byte, sourceType artifactpb.FileType, targetFormat string, pipelines []service.PipelineRelease) ([]byte, service.PipelineRelease, error) {
	if len(pipelines) == 0 {
		return nil, service.PipelineRelease{}, fmt.Errorf("no pipelines provided")
	}

	// Use the first available pipeline (convert-file-type)
	pipeline := pipelines[0]

	// Determine input variable name based on source type
	inputVarName := getInputVariableName(sourceType)
	if inputVarName == "" {
		return nil, pipeline, fmt.Errorf("unsupported source file type: %s", sourceType.String())
	}

	// Prepare pipeline input
	base64Content := base64.StdEncoding.EncodeToString(content)
	mimeType := ai.FileTypeToMIME(sourceType)
	dataURI := fmt.Sprintf("data:%s;base64,%s", mimeType, base64Content)

	inputs := []*structpb.Struct{
		{
			Fields: map[string]*structpb.Value{
				inputVarName: {
					Kind: &structpb.Value_StringValue{
						StringValue: dataURI,
					},
				},
			},
		},
	}

	// Import the pipeline protobuf package
	pipelineClient := w.service.PipelinePublicClient()

	// Create pipeline request
	req := &pipelinepb.TriggerNamespacePipelineReleaseRequest{
		NamespaceId: pipeline.Namespace,
		PipelineId:  pipeline.ID,
		ReleaseId:   pipeline.Version,
		Inputs:      inputs,
	}

	// Trigger pipeline with timeout
	ctx, cancel := context.WithTimeout(ctx, 300*time.Second)
	defer cancel()

	resp, err := pipelineClient.TriggerNamespacePipelineRelease(ctx, req)
	if err != nil {
		return nil, pipeline, fmt.Errorf("failed to trigger conversion pipeline: %w", err)
	}

	if resp == nil || len(resp.Outputs) == 0 {
		return nil, pipeline, fmt.Errorf("conversion pipeline returned no outputs")
	}

	// Extract converted content from output
	outputFieldName := getOutputFieldName(targetFormat)
	outputValue := resp.Outputs[0].Fields[outputFieldName]
	if outputValue == nil {
		return nil, pipeline, fmt.Errorf("conversion pipeline output missing field: %s", outputFieldName)
	}

	outputURL := outputValue.GetStringValue()
	if outputURL == "" {
		return nil, pipeline, fmt.Errorf("conversion pipeline output is empty")
	}

	// The pipeline returns a MinIO blob URL (not a data URI) for efficient large file handling
	// Format: http://localhost:8080/v1alpha/blob-urls/base64_encoded_presigned_url
	convertedContent, err := w.fetchFromBlobURL(ctx, outputURL)
	if err != nil {
		return nil, pipeline, fmt.Errorf("failed to fetch converted content from blob URL: %w", err)
	}

	return convertedContent, pipeline, nil
}

// fetchFromBlobURL fetches file content from a MinIO blob URL
// The blob URL format is: scheme://host/v1alpha/blob-urls/base64_encoded_presigned_url
// Uses the shared DecodeBlobURL function from the service layer
func (w *Worker) fetchFromBlobURL(ctx context.Context, blobURL string) ([]byte, error) {
	// Check if it's a blob URL or data URI
	if !strings.Contains(blobURL, "/v1alpha/blob-urls/") {
		// If it's a regular data URI, decode it directly
		if strings.HasPrefix(blobURL, "data:") {
			parts := strings.SplitN(blobURL, ",", 2)
			if len(parts) != 2 {
				return nil, fmt.Errorf("invalid data URI format")
			}
			return base64.StdEncoding.DecodeString(parts[1])
		}
		return nil, fmt.Errorf("unsupported URL format: %s", blobURL)
	}

	// Decode the blob URL to get the presigned URL using shared service function
	presignedURL, err := service.DecodeBlobURL(blobURL)
	if err != nil {
		return nil, fmt.Errorf("failed to decode blob URL: %w", err)
	}

	// Fetch the file content from the presigned URL using HTTP client
	return w.fetchFromPresignedURL(ctx, presignedURL)
}

// fetchFromPresignedURL fetches file content from a presigned MinIO URL using HTTP client
func (w *Worker) fetchFromPresignedURL(ctx context.Context, presignedURL string) ([]byte, error) {
	// Create HTTP client with timeout
	client := &http.Client{
		Timeout: 300 * time.Second,
	}

	req, err := http.NewRequestWithContext(ctx, "GET", presignedURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch from presigned URL: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("presigned URL returned non-200 status: %d", resp.StatusCode)
	}

	// Read the response body
	content, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	return content, nil
}

// getInputVariableName returns the pipeline input variable name based on file type
func getInputVariableName(fileType artifactpb.FileType) string {
	switch fileType {
	case artifactpb.FileType_FILE_TYPE_GIF,
		artifactpb.FileType_FILE_TYPE_BMP,
		artifactpb.FileType_FILE_TYPE_TIFF,
		artifactpb.FileType_FILE_TYPE_AVIF:
		return "image"
	case artifactpb.FileType_FILE_TYPE_M4A,
		artifactpb.FileType_FILE_TYPE_WMA:
		return "audio"
	case artifactpb.FileType_FILE_TYPE_MKV:
		return "video"
	case artifactpb.FileType_FILE_TYPE_DOC,
		artifactpb.FileType_FILE_TYPE_DOCX,
		artifactpb.FileType_FILE_TYPE_PPT,
		artifactpb.FileType_FILE_TYPE_PPTX,
		artifactpb.FileType_FILE_TYPE_XLS,
		artifactpb.FileType_FILE_TYPE_XLSX:
		return "document"
	default:
		return ""
	}
}

// getOutputFieldName returns the pipeline output field name based on target format
func getOutputFieldName(targetFormat string) string {
	switch targetFormat {
	case "png":
		return "image"
	case "ogg":
		return "audio"
	case "mp4":
		return "video"
	case "pdf":
		return "document"
	default:
		return ""
	}
}

// mapFormatToFileType maps target format string to FileType enum
func mapFormatToFileType(format string) artifactpb.FileType {
	switch format {
	case "png":
		return artifactpb.FileType_FILE_TYPE_PNG
	case "ogg":
		return artifactpb.FileType_FILE_TYPE_OGG
	case "mp4":
		return artifactpb.FileType_FILE_TYPE_MP4
	case "pdf":
		return artifactpb.FileType_FILE_TYPE_PDF
	default:
		return artifactpb.FileType_FILE_TYPE_UNSPECIFIED
	}
}

// ===== ERROR CONSTANTS =====

const (
	convertFileTypeActivityError       = "ConvertFileTypeActivity"
	cacheContextActivityError          = "CacheContextActivity"
	convertToMarkdownFileActivityError = "ConvertToMarkdownFileActivity"
)
