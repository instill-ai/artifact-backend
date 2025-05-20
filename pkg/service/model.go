package service

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/instill-ai/artifact-backend/pkg/constant"
	"github.com/instill-ai/artifact-backend/pkg/logger"

	"encoding/base64"
	"sync"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	modelpb "github.com/instill-ai/protogen-go/model/model/v1alpha"

	"github.com/pdfcpu/pdfcpu/pkg/api"
)

const ConvertDocToMDModelID = "docling"
const ConvertDocToMDModelVersion = "v0.1.0"

// splitPDFIntoBatches splits a PDF into batches of n pages each and returns a slice of base64-encoded sub-PDFs
func splitPDFIntoBatches(pdfData []byte, batchSize int) ([]string, error) {
	// Create a temporary directory
	tempDir, err := os.MkdirTemp("", "pdfcpu-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer os.RemoveAll(tempDir)

	// Prevent pdfcpu from trying to write out $HOME/.config/pdfcpu
	api.DisableConfigDir()

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
	ctx, err := api.ReadContextFile(inputFilePath)
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
		if err := api.TrimFile(inputFilePath, outputFilePath, []string{pageRange}, nil); err != nil {
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
		filesToProcess, err = splitPDFIntoBatches(pdfData, 5)
		if err != nil {
			return "", fmt.Errorf("splitting PDF into batches: %w", err)
		}
	} else {
		filesToProcess = []string{fileBase64}
	}

	// Process all files (or batches for PDFs)
	results := make([]string, len(filesToProcess))
	errCh := make(chan error, len(filesToProcess))
	var wg sync.WaitGroup

	namespaceID := "admin"

	for i, b64 := range filesToProcess {
		wg.Add(1)
		go func(idx int, b64 string) {
			defer wg.Done()

			// Add timeout to the model trigger requests
			triggerCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
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
										"doc_content": {Kind: &structpb.Value_StringValue{StringValue: prefix + b64}},
									},
								},
							}},
						},
					},
				},
			}

			resp, err := s.ModelPub.TriggerNamespaceModel(triggerCtx, req)
			if err != nil {
				// If admin namespace fails, try instill-ai namespace
				if idx == 0 { // Only log once for the first batch
					logger.Warn("Failed to trigger admin/docling, falling back to instill-ai/docling", zap.Error(err))
				}
				req.NamespaceId = "instill-ai"
				resp, err = s.ModelPub.TriggerNamespaceModel(triggerCtx, req)
				if err != nil {
					errCh <- fmt.Errorf("failed to trigger %s model: %w", ConvertDocToMDModelID, err)
					return
				}
				namespaceID = "instill-ai" // Update for metadata
			} else if idx == 0 { // Only log once for the first batch
				logger.Info("Successfully triggered admin/docling model")
			}

			result, err := getModelConvertResult(resp)
			if err != nil {
				errCh <- err
				return
			}
			results[idx] = result
		}(i, b64)
	}

	wg.Wait()
	close(errCh)

	if len(errCh) > 0 {
		return "", <-errCh
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
