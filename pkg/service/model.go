package service

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/gofrs/uuid"
	"github.com/pdfcpu/pdfcpu/pkg/pdfcpu/model"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/structpb"

	pdfcpu "github.com/pdfcpu/pdfcpu/pkg/api"

	"github.com/instill-ai/artifact-backend/pkg/constant"
	"github.com/instill-ai/artifact-backend/pkg/logger"
	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	modelpb "github.com/instill-ai/protogen-go/model/model/v1alpha"
)

const ConvertDocToMDModelID = "docling"
const ConvertDocToMDModelVersion = "v0.1.0"

// splitPDFIntoBatches splits a PDF into batches of n pages each and returns a slice of base64-encoded sub-PDFs
func splitPDFIntoBatches(pdfData []byte, batchSize int) ([]string, error) {
	// Prevent pdfcpu from trying to write out $HOME/.config/pdfcpu
	pdfcpu.DisableConfigDir()

	// Create a default configuration
	modelConf := model.NewDefaultConfiguration()

	// Read the PDF from memory
	in := bytes.NewReader(pdfData)
	ctx, err := pdfcpu.ReadContext(in, modelConf)
	if err != nil {
		return nil, err
	}
	nPages := ctx.PageCount

	var batches []string
	for i := 0; i < nPages; i += batchSize {
		end := i + batchSize
		if end > nPages {
			end = nPages
		}
		// Create a range string, e.g. "1-5"
		pageRange := fmt.Sprintf("%d-%d", i+1, end)

		// Reset reader to beginning of file
		if _, err := in.Seek(0, io.SeekStart); err != nil {
			return nil, fmt.Errorf("seeking to start of PDF: %w", err)
		}

		// Create buffer for output
		out := new(bytes.Buffer)

		// Extract the page range directly to memory
		if err := pdfcpu.Trim(in, out, []string{pageRange}, modelConf); err != nil {
			return nil, fmt.Errorf("trimming pages %s: %w", pageRange, err)
		}

		// Encode the PDF data to base64
		batches = append(batches, base64.StdEncoding.EncodeToString(out.Bytes()))
	}

	return batches, nil
}

// ConvertToMDModel using docling model to convert some file type to MD and consume caller's credits
func (s *Service) ConvertToMDModel(ctx context.Context, fileUID uuid.UUID, caller uuid.UUID, requester uuid.UUID, fileBase64 string, fileType artifactpb.FileType) (string, error) {
	// Add overall operation timeout
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	logger, _ := logger.GetZapLogger(ctx)
	var md metadata.MD
	if requester != uuid.Nil {
		md = metadata.New(map[string]string{
			constant.HeaderUserUIDKey:      caller.String(),
			constant.HeaderAuthTypeKey:     "user",
			constant.HeaderRequesterUIDKey: requester.String(),
		})
	} else {
		md = metadata.New(map[string]string{
			constant.HeaderUserUIDKey:  caller.String(),
			constant.HeaderAuthTypeKey: "user",
		})
	}
	ctx = metadata.NewOutgoingContext(ctx, md)

	// Get the appropriate prefix for the file type
	prefix := getFileTypePrefix(fileType)

	// Prepare files to process (as a slice of base64 strings)
	var filesToProcess []string
	var err error

	// For PDFs, split into batches; for other file types, process as a single item
	if fileType == artifactpb.FileType_FILE_TYPE_PDF {
		pdfData, err := base64.StdEncoding.DecodeString(fileBase64)
		if err != nil {
			return "", fmt.Errorf("decoding base64 PDF: %w", err)
		}
		filesToProcess, err = splitPDFIntoBatches(pdfData, 12)
		if err != nil {
			return "", fmt.Errorf("splitting PDF into batches: %w", err)
		}
	} else {
		filesToProcess = []string{fileBase64}
	}

	// TODO: use Temporal to manage the concurrency here - see Instill Core Github issue #1235

	// Calculate appropriate number of workers based on system resources
	// Use 75% of available CPUs or the number of tasks, whichever is smaller
	numWorkers := runtime.NumCPU() * 3 / 4
	if numWorkers < 1 {
		numWorkers = 1
	}
	if len(filesToProcess) < numWorkers {
		numWorkers = len(filesToProcess)
	}

	// Process all files (or batches for PDFs) using a worker pool
	results := make([]string, len(filesToProcess))
	errCh := make(chan error, len(filesToProcess))

	// Add a done channel to handle context cancellation
	done := make(chan struct{})
	defer close(done)

	namespaceID := "admin"

	// Start a goroutine to watch for context cancellation
	go func() {
		select {
		case <-ctx.Done():
			// If context is cancelled, put the error in the error channel
			errCh <- ctx.Err()
		case <-done:
			// Operation completed normally
			return
		}
	}()

	// Create a pool of worker goroutines
	var wg sync.WaitGroup

	// Create job channel with buffer size equal to number of tasks
	jobs := make(chan jobTask, len(filesToProcess))

	// Launch worker pool
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go worker(ctx, &wg, jobs, results, errCh, prefix, namespaceID, ConvertDocToMDModelID, ConvertDocToMDModelVersion, s, logger)
	}

	// Send jobs to the worker pool
	for i, b64 := range filesToProcess {
		jobs <- jobTask{
			index: i,
			data:  b64,
		}
	}
	close(jobs) // All jobs have been sent

	// Use a channel to signal when wait group is done
	waitDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitDone)
	}()

	// Wait for either completion or timeout
	select {
	case <-waitDone:
		// All goroutines completed
	case <-ctx.Done():
		// Context timed out or was cancelled
		return "", ctx.Err()
	}

	close(errCh)

	// Collect all errors instead of just the first one
	var allErrors []error
	for err := range errCh {
		allErrors = append(allErrors, err)
	}

	if len(allErrors) > 0 {
		// Combine errors or return the first one with context
		return "", fmt.Errorf("encountered %d errors during processing, first error: %w",
			len(allErrors), allErrors[0])
	}

	// Save metadata about the model used (only needs to happen once)
	convertingModelMetadata := namespaceID + "/" + ConvertDocToMDModelID + "@" + ConvertDocToMDModelVersion
	err = s.Repository.UpdateKbFileExtraMetaData(ctx, fileUID, "", convertingModelMetadata, "", "", "", nil, nil, nil, nil, nil)
	if err != nil {
		return "", fmt.Errorf("failed to save converting model metadata: %w", err)
	}

	return strings.Join(results, "\n"), nil
}

// jobTask represents a single batch processing task
type jobTask struct {
	index int    // index in the results array
	data  string // base64 encoded batch data
}

// worker processes PDF batch conversion tasks from the jobs channel
func worker(
	ctx context.Context,
	wg *sync.WaitGroup,
	jobs <-chan jobTask,
	results []string,
	errCh chan<- error,
	prefix string,
	namespaceID string,
	modelID string,
	modelVersion string,
	s *Service,
	logger *zap.Logger,
) {
	defer wg.Done()

	for job := range jobs {
		// Check if context is cancelled before processing
		select {
		case <-ctx.Done():
			errCh <- ctx.Err()
			return
		default:
			// Continue with processing
		}

		// Set timeout for this specific task
		triggerCtx, cancel := context.WithTimeout(ctx, 60*time.Second)

		// Process the batch
		result, err := processBatch(triggerCtx, job.data, prefix, namespaceID, modelID, modelVersion, job.index, s, logger)
		cancel() // Always cancel the context

		if err != nil {
			errCh <- err
			continue
		}

		// Store result
		results[job.index] = result
	}
}

// processBatch handles the conversion of a single PDF batch to markdown
func processBatch(
	ctx context.Context,
	base64Data string,
	prefix string,
	namespaceID string,
	modelID string,
	modelVersion string,
	idx int,
	s *Service,
	logger *zap.Logger,
) (string, error) {
	req := &modelpb.TriggerNamespaceModelRequest{
		NamespaceId: namespaceID,
		ModelId:     modelID,
		Version:     modelVersion,
		TaskInputs: []*structpb.Struct{
			{
				Fields: map[string]*structpb.Value{
					"data": {Kind: &structpb.Value_StructValue{
						StructValue: &structpb.Struct{
							Fields: map[string]*structpb.Value{
								"doc_content": {Kind: &structpb.Value_StringValue{StringValue: prefix + base64Data}},
							},
						},
					}},
				},
			},
		},
	}

	// Make the API call
	resp, err := s.ModelPub.TriggerNamespaceModel(ctx, req)
	if err != nil {
		// If admin namespace fails, try instill-ai namespace as fallback
		if namespaceID == "admin" {
			if idx == 0 { // Only log once for the first batch
				logger.Warn("Failed to trigger admin/docling, falling back to instill-ai/docling",
					zap.Error(err))
			}

			req.NamespaceId = "instill-ai"
			resp, err = s.ModelPub.TriggerNamespaceModel(ctx, req)
			if err != nil {
				return "", fmt.Errorf("failed to trigger %s model with fallback: %w", modelID, err)
			}
		} else {
			return "", fmt.Errorf("failed to trigger %s model: %w", modelID, err)
		}
	}

	return getModelConvertResult(resp)
}

func getModelConvertResult(resp *modelpb.TriggerNamespaceModelResponse) (string, error) {
	if resp == nil || len(resp.TaskOutputs) == 0 {
		return "", fmt.Errorf("response is nil or has no outputs. resp: %v", resp)
	}
	fields := resp.TaskOutputs[0].GetFields()["data"].GetStructValue().GetFields()
	if fields == nil {
		return "", fmt.Errorf("fields in the output are nil. resp: %v", resp)
	}
	markdownPages, ok := fields["markdown_pages"]
	if !ok {
		return "", fmt.Errorf("markdown_pages not found in the output fields. resp: %v", resp)
	}

	// Not used at the moment
	// extractedImages, _ := fields["extracted_images"]
	// pagesWithImages, _ := fields["pages_with_images"]

	markdownPagesSlice := []string{}
	for _, v := range markdownPages.GetListValue().GetValues() {
		markdownPagesSlice = append(markdownPagesSlice, v.GetStringValue())
	}

	return strings.Join(markdownPagesSlice, "\n"), nil
}
