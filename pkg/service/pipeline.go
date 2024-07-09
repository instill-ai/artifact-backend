package service

import (
	"context"
	"errors"


	"github.com/google/uuid"
	"github.com/instill-ai/artifact-backend/pkg/logger"
	pipelinev1beta "github.com/instill-ai/protogen-go/vdp/pipeline/v1beta"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/structpb"
)


// ConvertPDFToMD using converting pipeline to convert PDF to MD and consume caller's credits
func (s *Service) ConvertPDFToMD(ctx context.Context, caller uuid.UUID, pdfBase64 string) (string, error) {
	logger, _ := logger.GetZapLogger(ctx)
	md := metadata.New(map[string]string{"Instill-User-Uid": caller.String(), "Instill-Auth-Type": "user"})
	ctx = metadata.NewOutgoingContext(ctx, md)

	req := &pipelinev1beta.TriggerOrganizationPipelineReleaseRequest{
		Name: "organizations/preset/pipelines/indexing-convert-pdf/releases/v1.0.0",
		Inputs: []*structpb.Struct{
			{
				Fields: map[string]*structpb.Value{
					"document_input": {Kind: &structpb.Value_StringValue{StringValue: pdfBase64}},
				},
			},
		},
	}
	resp, err := s.PipelinePub.TriggerOrganizationPipelineRelease(ctx, req)
	if err != nil {
		logger.Error("failed to trigger pipeline", zap.Error(err))
		return "", err
	}
	result, err := getConvertResult(resp)
	if err != nil {
		logger.Error("failed to get convert result", zap.Error(err))
		return "", err
	}
	return result, nil
}

// Helper function to safely extract the "convert_result" from the response.
// It checks if the index and key are available to avoid nil pointer issues.
func getConvertResult(resp *pipelinev1beta.TriggerOrganizationPipelineReleaseResponse) (string, error) {
	if resp == nil || len(resp.Outputs) == 0 {
		return "", errors.New("response is nil or has no outputs")
	}
	fields := resp.Outputs[0].GetFields()
	if fields == nil {
		return "", errors.New("fields in the output are nil")
	}
	convertResult, ok := fields["convert_result"]
	if !ok {
		return "", errors.New("convert_result not found in the output fields")
	}
	return convertResult.GetStringValue(), nil
}

type Chunk = struct {
	End    int
	Start  int
	Text   string
	Tokens int
}

// SplitMarkdown using splitting pipeline to split markdown and consume caller's credits
func (s *Service) SplitMarkdown(ctx context.Context, caller uuid.UUID, markdown string) ([]Chunk, error) {
	md := metadata.New(map[string]string{"Instill-User-Uid": caller.String(), "Instill-Auth-Type": "user"})
	ctx = metadata.NewOutgoingContext(ctx, md)
	req := &pipelinev1beta.TriggerOrganizationPipelineReleaseRequest{
		Name: "organizations/preset/pipelines/indexing-split-markdown/releases/v0.0.1",

		Inputs: []*structpb.Struct{
			{
				Fields: map[string]*structpb.Value{
					"text_input":    {Kind: &structpb.Value_StringValue{StringValue: markdown}},
					"chunk_length":  {Kind: &structpb.Value_NumberValue{NumberValue: 200}},
					"chunk_overlap": {Kind: &structpb.Value_NumberValue{NumberValue: 50}},
				},
			},
		},
	}
	res, err := s.PipelinePub.TriggerOrganizationPipelineRelease(ctx, req)
	if err != nil {
		return nil, err
	}
	result, err := GetChunksFromResponse(res)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// GetChunksFromResponse converts the pipeline response into a slice of Chunk.
func GetChunksFromResponse(resp *pipelinev1beta.TriggerOrganizationPipelineReleaseResponse) ([]Chunk, error) {
	if resp == nil || len(resp.Outputs) == 0 {
		return nil, errors.New("response is nil or has no outputs")
	}
	splitResult, ok := resp.Outputs[0].GetFields()["split_result"]
	if !ok {
		return nil, errors.New("split_result not found in the output fields")
	}
	listValue := splitResult.GetListValue()
	if listValue == nil {
		return nil, errors.New("split_result is not a list")
	}

	var chunks []Chunk
	for _, v := range listValue.GetValues() {
		endPos := int(v.GetStructValue().Fields["end-position"].GetNumberValue())
		startPos := int(v.GetStructValue().Fields["start-position"].GetNumberValue())
		text := v.GetStructValue().Fields["text"].GetStringValue()
		chunks = append(chunks, Chunk{
			End:   endPos,
			Start: startPos,
			Text:  text,
		})
	}
	return chunks, nil
}

// SplitText using splitting pipeline to split text and consume caller's credits
func (s *Service) SplitText(ctx context.Context, caller uuid.UUID, text string) ([]Chunk, error) {
	md := metadata.New(map[string]string{"Instill-User-Uid": caller.String(), "Instill-Auth-Type": "user"})
	ctx = metadata.NewOutgoingContext(ctx, md)
	req := &pipelinev1beta.TriggerOrganizationPipelineReleaseRequest{
		Name: "organizations/preset/pipelines/indexing-split-text/releases/v0.0.1",

		Inputs: []*structpb.Struct{
			{
				Fields: map[string]*structpb.Value{
					"text_input":    {Kind: &structpb.Value_StringValue{StringValue: text}},
					"chunk_length":  {Kind: &structpb.Value_NumberValue{NumberValue: 200}},
					"chunk_overlap": {Kind: &structpb.Value_NumberValue{NumberValue: 50}},
				},
			},
		},
	}
	res, err := s.PipelinePub.TriggerOrganizationPipelineRelease(ctx, req)
	if err != nil {
		return nil, err
	}
	result, err := GetChunksFromResponse(res)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// TODO VectorizeText - waiting for CE to implement the global secret
// VectorizeText using embedding pipeline to vectorize text and consume caller's credits
func (s *Service) VectorizeText(ctx context.Context, caller uuid.UUID, texts []string) ([][]float32, error) {
	md := metadata.New(map[string]string{"Instill-User-Uid": caller.String(), "Instill-Auth-Type": "user"})
	ctx = metadata.NewOutgoingContext(ctx, md)
	inputs := make([]*structpb.Struct, 0, len(texts))
	for _, text := range texts {
		inputs = append(inputs, &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"chunk_input": {Kind: &structpb.Value_StringValue{StringValue: text}},
			},
		})
	}

	req := &pipelinev1beta.TriggerOrganizationPipelineReleaseRequest{
		Name: "organizations/preset/pipelines/indexing-embed-text/releases/v0.0.1",

		Inputs: inputs,
	}
	res, err := s.PipelinePub.TriggerOrganizationPipelineRelease(ctx, req)
	if err != nil {
		return nil, err
	}
	result, err := GetVectorFromResponse(res)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// GetVectorFromResponse converts the pipeline response into a slice of float32.
func GetVectorFromResponse(resp *pipelinev1beta.TriggerOrganizationPipelineReleaseResponse) ([][]float32, error) {
	if resp == nil || len(resp.Outputs) == 0 {
		return nil, errors.New("response is nil or has no outputs")
	}

	var vectors [][]float32
	for _, output := range resp.Outputs {
		embedResult, ok := output.GetFields()["embed_result"]
		if !ok {
			return nil, errors.New("embed_result not found in the output fields")
		}
		listValue := embedResult.GetListValue()
		if listValue == nil {
			return nil, errors.New("embed_result is not a list")
		}

		vector := make([]float32, 0, len(listValue.GetValues()))
		for _, v := range listValue.GetValues() {
			vector = append(vector, float32(v.GetNumberValue()))
		}
		vectors = append(vectors, vector)
	}

	return vectors, nil
}
