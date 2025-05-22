package service

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gofrs/uuid"
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

// Maximum number of concurrent goroutines to use when processing batches
const maxConcurrentWorkers = 20

// splitPDFIntoBatches splits a PDF into batches of n pages each and returns a slice of base64-encoded sub-PDFs
func splitPDFIntoBatches(pdfData []byte, batchSize int) ([]string, error) {
	// Create a temporary directory
	tempDir, err := os.MkdirTemp("", "pdfcpu-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer os.RemoveAll(tempDir)

	// Prevent pdfcpu from trying to write out $HOME/.config/pdfcpu
	pdfcpu.DisableConfigDir()

	// Write the input PDF to a temporary file
	inputFile, err := os.CreateTemp(tempDir, "input-*.pdf")
	if err != nil {
		return nil, err
	}
	inputFilePath := inputFile.Name()
	if _, err := inputFile.Write(pdfData); err != nil {
		inputFile.Close()
		return nil, err
	}
	inputFile.Close()
	defer os.Remove(inputFilePath)

	// Read number of pages
	ctx, err := pdfcpu.ReadContextFile(inputFilePath)
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
		outputFilePath := filepath.Join(tempDir, fmt.Sprintf("output-%d-%d.pdf", i+1, end))

		// Extract the page range using TrimFile instead of ExtractPagesFile
		if err := pdfcpu.TrimFile(inputFilePath, outputFilePath, []string{pageRange}, nil); err != nil {
			return nil, err
		}
		// Read the output PDF and encode as base64
		outData, err := os.ReadFile(outputFilePath)
		if err != nil {
			return nil, err
		}
		batches = append(batches, base64.StdEncoding.EncodeToString(outData))
		os.Remove(outputFilePath)
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
		filesToProcess, err = splitPDFIntoBatches(pdfData, 8)
		if err != nil {
			return "", fmt.Errorf("splitting PDF into batches: %w", err)
		}
	} else {
		filesToProcess = []string{fileBase64}
	}

	// Process all files (or batches for PDFs)
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

	// Create a worker pool with limited concurrency
	type job struct {
		idx int
		b64 string
	}

	// Create a jobs channel with enough buffer to hold all jobs
	jobs := make(chan job, len(filesToProcess))

	// Fill the jobs channel with work
	for i, b64 := range filesToProcess {
		jobs <- job{idx: i, b64: b64}
	}
	close(jobs)

	// Use wait group to track when all workers are done
	var wg sync.WaitGroup

	// Start a limited number of worker goroutines
	workerCount := maxConcurrentWorkers
	if len(filesToProcess) < workerCount {
		workerCount = len(filesToProcess)
	}

	logger.Info("Starting worker pool", zap.Int("numWorkers", workerCount), zap.Int("totalBatches", len(filesToProcess)))

	for i := range workerCount {
		wg.Add(1)
		go func(workerId int) {
			defer wg.Done()

			// Process jobs until the channel is closed
			for j := range jobs {
				select {
				case <-ctx.Done():
					// If parent context is already cancelled, don't proceed
					errCh <- ctx.Err()
					return
				default:
					// Continue with processing
				}

				// Increase the timeout for larger files (optional)
				triggerCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
				defer cancel()

				req := &modelpb.TriggerNamespaceModelRequest{
					NamespaceId: namespaceID,
					ModelId:     ConvertDocToMDModelID,
					Version:     ConvertDocToMDModelVersion,
					TaskInputs: []*structpb.Struct{
						{
							Fields: map[string]*structpb.Value{
								"data": {Kind: &structpb.Value_StructValue{
									StructValue: &structpb.Struct{
										Fields: map[string]*structpb.Value{
											"doc_content": {Kind: &structpb.Value_StringValue{StringValue: prefix + j.b64}},
										},
									},
								}},
							},
						},
					},
				}

				// Retry mechanism parameters
				maxRetries := 3
				retryCount := 0
				var resp *modelpb.TriggerNamespaceModelResponse
				var err error

				// Retry loop for admin namespace
				for retryCount < maxRetries {
					// Check if we're getting close to the timeout and extend it for retries
					if retryCount > 0 {
						// Create a new context with extended timeout for retries
						cancel() // Cancel the previous context
						// Scale timeout based on retry count - add 30 seconds per retry
						extendedTimeout := 60*time.Second + time.Duration(retryCount)*30*time.Second
						triggerCtx, cancel = context.WithTimeout(ctx, extendedTimeout)
						defer cancel()

						logger.Info("Extended timeout for retry",
							zap.Int("worker", workerId),
							zap.Int("retry", retryCount),
							zap.Duration("timeout", extendedTimeout))
					}

					resp, err = s.ModelPub.TriggerNamespaceModel(triggerCtx, req)
					if err == nil {
						// If successful, break out of the retry loop
						if j.idx == 0 || retryCount > 0 { // Log on first batch or when we had to retry
							logger.Info("Successfully triggered admin/docling model",
								zap.Int("worker", workerId),
								zap.Int("batch", j.idx),
								zap.Int("retry", retryCount))
						}
						break
					}

					retryCount++
					if retryCount < maxRetries {
						// Log retry attempt
						logger.Warn("Retrying admin/docling model trigger",
							zap.Int("worker", workerId),
							zap.Int("batch", j.idx),
							zap.Int("attempt", retryCount),
							zap.Int("maxRetries", maxRetries),
							zap.Error(err))

						// Wait before retrying (with exponential backoff)
						backoffTime := time.Duration(retryCount*retryCount) * 500 * time.Millisecond
						time.Sleep(backoffTime)
					}
				}

				// If all retries failed, attempt fallback to instill-ai namespace
				if err != nil {
					// If admin namespace fails after all retries, try instill-ai namespace
					if j.idx == 0 { // Only log once for the first batch
						logger.Warn("Failed to trigger admin/docling after retries, falling back to instill-ai/docling",
							zap.Int("worker", workerId),
							zap.Int("batch", j.idx),
							zap.Int("retries", retryCount),
							zap.Error(err))
					}

					req.NamespaceId = "instill-ai"

					// Reset retry count for instill-ai namespace
					retryCount = 0

					// Retry loop for instill-ai namespace
					for retryCount < maxRetries {
						// Check if we're getting close to the timeout and extend it for retries
						if retryCount > 0 {
							// Create a new context with extended timeout for retries
							cancel() // Cancel the previous context
							// Scale timeout based on retry count - add 30 seconds per retry
							extendedTimeout := 60*time.Second + time.Duration(retryCount)*30*time.Second
							triggerCtx, cancel = context.WithTimeout(ctx, extendedTimeout)
							defer cancel()

							logger.Info("Extended timeout for instill-ai retry",
								zap.Int("worker", workerId),
								zap.Int("retry", retryCount),
								zap.Duration("timeout", extendedTimeout))
						}

						resp, err = s.ModelPub.TriggerNamespaceModel(triggerCtx, req)
						if err == nil {
							// If successful, break out of the retry loop
							namespaceID = "instill-ai" // Update for metadata
							break
						}

						retryCount++
						if retryCount < maxRetries {
							// Log retry attempt
							logger.Warn("Retrying instill-ai/docling model trigger",
								zap.Int("worker", workerId),
								zap.Int("batch", j.idx),
								zap.Int("attempt", retryCount),
								zap.Int("maxRetries", maxRetries),
								zap.Error(err))

							// Wait before retrying (with exponential backoff)
							backoffTime := time.Duration(retryCount*retryCount) * 500 * time.Millisecond
							time.Sleep(backoffTime)
						}
					}

					// If all retries failed for both namespaces
					if err != nil {
						errCh <- fmt.Errorf("failed to trigger %s model after all retries: %w", ConvertDocToMDModelID, err)
						return
					}
				}

				result, err := getModelConvertResult(resp)
				if err != nil {
					errCh <- err
					return
				}
				results[j.idx] = result
			}
		}(i)
	}

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
