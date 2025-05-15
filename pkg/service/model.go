package service

import (
	"context"
	"fmt"
	"strings"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/instill-ai/artifact-backend/pkg/constant"
	"github.com/instill-ai/artifact-backend/pkg/logger"

	"bytes"
	"encoding/base64"
	"sync"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	modelpb "github.com/instill-ai/protogen-go/model/model/v1alpha"

	"github.com/unidoc/unipdf/v3/model"
)

const ConvertDocToMDModelID = "docling"
const ConvertDocToMDModelVersion = "v0.1.0"

// splitPDFIntoBatches splits a PDF into batches of n pages each and returns a slice of base64-encoded sub-PDFs
func splitPDFIntoBatches(pdfData []byte, batchSize int) ([]string, error) {
	r, err := model.NewPdfReader(bytes.NewReader(pdfData))
	if err != nil {
		return nil, err
	}
	nPages, err := r.GetNumPages()
	if err != nil {
		return nil, err
	}
	var batches []string
	for i := 0; i < nPages; i += batchSize {
		end := i + batchSize
		if end > nPages {
			end = nPages
		}
		w := model.NewPdfWriter()
		for j := i + 1; j <= end; j++ {
			page, err := r.GetPage(j)
			if err != nil {
				return nil, err
			}
			if err := w.AddPage(page); err != nil {
				return nil, err
			}
		}
		buf := new(strings.Builder)
		if err := w.Write(buf); err != nil {
			return nil, err
		}
		batches = append(batches, base64.StdEncoding.EncodeToString([]byte(buf.String())))
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

	// If PDF, split into batches and process in parallel
	if fileType == artifactpb.FileType_FILE_TYPE_PDF {
		pdfData, err := base64.StdEncoding.DecodeString(fileBase64)
		if err != nil {
			logger.Error("Failed to decode base64 PDF", zap.Error(err))
			return "", err
		}
		batches, err := splitPDFIntoBatches(pdfData, 5)
		if err != nil {
			logger.Error("Failed to split PDF into batches", zap.Error(err))
			return "", err
		}
		results := make([]string, len(batches))
		errCh := make(chan error, len(batches))
		var wg sync.WaitGroup
		for i, b64 := range batches {
			wg.Add(1)
			go func(idx int, b64 string) {
				defer wg.Done()
				req := &modelpb.TriggerNamespaceModelRequest{
					NamespaceId: "admin",
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
				resp, err := s.ModelPub.TriggerNamespaceModel(ctx, req)
				if err != nil {
					errCh <- err
					return
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
		return strings.Join(results, "\n"), nil
	}

	namespaceID := "admin"
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
								"doc_content": {Kind: &structpb.Value_StringValue{StringValue: prefix + fileBase64}},
							},
						},
					}},
				},
			},
		},
	}

	resp, err := s.ModelPub.TriggerNamespaceModel(ctx, req)
	if err != nil {
		namespaceID = "instill-ai"
		req.NamespaceId = namespaceID
		resp, err = s.ModelPub.TriggerNamespaceModel(ctx, req)
		if err != nil {
			logger.Error(fmt.Sprintf("failed to trigger %s model", ConvertDocToMDModelID), zap.Error(err))
			return "", fmt.Errorf("failed to trigger %s model: %w", ConvertDocToMDModelID, err)
		}
	}

	convertingModelMetadata := namespaceID + "/" + ConvertDocToMDModelID + "@" + ConvertDocToMDModelVersion
	err = s.Repository.UpdateKbFileExtraMetaData(ctx, fileUID, "", convertingModelMetadata, "", "", "", nil, nil, nil, nil, nil)
	if err != nil {
		logger.Error("Failed to save converting pipeline metadata.", zap.String("File uid:", fileUID.String()))
		return "", fmt.Errorf("failed to save converting model metadata: %w", err)
	}

	result, err := getModelConvertResult(resp)
	if err != nil {
		logger.Error("failed to get convert result", zap.Error(err))
		return "", fmt.Errorf("failed to get convert result: %w", err)
	}
	return result, nil
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
