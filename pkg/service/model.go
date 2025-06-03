package service

import (
	"context"
	"fmt"
	"strings"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/instill-ai/artifact-backend/pkg/logger"
	constantx "github.com/instill-ai/x/constant"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	modelpb "github.com/instill-ai/protogen-go/model/model/v1alpha"
)

const ConvertDocToMDModelID = "docling"
const ConvertDocToMDModelVersion = "v0.1.0"

// ConvertToMDModel using docling model to convert some file type to MD and consume caller's credits
func (s *Service) ConvertToMDModel(ctx context.Context, fileUID uuid.UUID, caller uuid.UUID, requester uuid.UUID, fileBase64 string, fileType artifactpb.FileType) (string, error) {
	logger, _ := logger.GetZapLogger(ctx)
	var md metadata.MD
	if requester != uuid.Nil {
		md = metadata.New(map[string]string{
			constantx.HeaderUserUIDKey:      caller.String(),
			constantx.HeaderAuthTypeKey:     "user",
			constantx.HeaderRequesterUIDKey: requester.String(),
		})
	} else {
		md = metadata.New(map[string]string{
			constantx.HeaderUserUIDKey:  caller.String(),
			constantx.HeaderAuthTypeKey: "user",
		})
	}
	ctx = metadata.NewOutgoingContext(ctx, md)

	// Get the appropriate prefix for the file type
	prefix := getFileTypePrefix(fileType)

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
