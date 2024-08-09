package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/gofrs/uuid"
	"github.com/instill-ai/artifact-backend/pkg/constant"
	"github.com/instill-ai/artifact-backend/pkg/logger"
	artifactv1alpha "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	pipelinev1beta "github.com/instill-ai/protogen-go/vdp/pipeline/v1beta"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/structpb"
)

const chunkLength = 800
const chunkOverlap = 200
const NamespaceID = "preset"
const PDFToMDVersion = "v1.1.1"
const MdSplitVersion = "v2.0.0"
const TextSplitVersion = "v2.0.0"
const TextEmbedVersion = "v1.1.0"
const QAVersion = "v1.1.0"
const ConverPDFToMDPipelineID = "indexing-convert-pdf"
const MdSplitPipelineID = "indexing-split-markdown"
const TextSplitPipelineID = "indexing-split-text"
const TextEmbedPipelineID = "indexing-embed"
const RetrievingQnA = "retrieving-qna"

// ConvertPDFToMDPipe using converting pipeline to convert PDF to MD and consume caller's credits
func (s *Service) ConvertPDFToMDPipe(ctx context.Context, caller uuid.UUID, requester uuid.UUID, pdfBase64 string, fileType artifactv1alpha.FileType) (string, error) {
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
	prefix := ""
	if fileType == artifactv1alpha.FileType_FILE_TYPE_PDF {
		prefix = "data:application/pdf;base64,"
	} else if fileType == artifactv1alpha.FileType_FILE_TYPE_DOCX {
		prefix = "data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,"
	} else if fileType == artifactv1alpha.FileType_FILE_TYPE_DOC {
		prefix = "data:application/msword;base64,"
	} else if fileType == artifactv1alpha.FileType_FILE_TYPE_PPT {
		prefix = "data:application/vnd.ms-powerpoint;base64,"
	} else if fileType == artifactv1alpha.FileType_FILE_TYPE_PPTX {
		prefix = "data:application/vnd.openxmlformats-officedocument.presentationml.presentation;base64,"
	} else if fileType == artifactv1alpha.FileType_FILE_TYPE_HTML {
		prefix = "data:text/html;base64,"
	} else if fileType == artifactv1alpha.FileType_FILE_TYPE_TEXT {
		prefix = "data:text/plain;base64,"
	}

	req := &pipelinev1beta.TriggerNamespacePipelineReleaseRequest{
		NamespaceId: NamespaceID,
		PipelineId:  ConverPDFToMDPipelineID,
		ReleaseId:   PDFToMDVersion,
		Inputs: []*structpb.Struct{
			{
				Fields: map[string]*structpb.Value{
					"document_input": {Kind: &structpb.Value_StringValue{StringValue: prefix + pdfBase64}},
				},
			},
		},
	}
	resp, err := s.PipelinePub.TriggerNamespacePipelineRelease(ctx, req)
	if err != nil {
		logger.Error("failed to trigger pipeline", zap.Error(err))
		return "", fmt.Errorf("failed to trigger %s pipeline: %w", ConverPDFToMDPipelineID, err)
	}
	result, err := getConvertResult(resp)
	if err != nil {
		logger.Error("failed to get convert result", zap.Error(err))
		return "", fmt.Errorf("failed to get convert result: %w", err)
	}
	return result, nil
}

// Helper function to safely extract the "convert_result" from the response.
// It checks if the index and key are available to avoid nil pointer issues.
func getConvertResult(resp *pipelinev1beta.TriggerNamespacePipelineReleaseResponse) (string, error) {
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

// SplitMarkdownPipe using splitting pipeline to split markdown and consume caller's credits
func (s *Service) SplitMarkdownPipe(ctx context.Context, caller uuid.UUID, requester uuid.UUID, markdown string) ([]Chunk, error) {
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
	req := &pipelinev1beta.TriggerNamespacePipelineReleaseRequest{
		NamespaceId: NamespaceID,
		PipelineId:  MdSplitPipelineID,
		ReleaseId:   MdSplitVersion,
		Inputs: []*structpb.Struct{
			{
				Fields: map[string]*structpb.Value{
					"md_input":         {Kind: &structpb.Value_StringValue{StringValue: markdown}},
					"max_chunk_length": {Kind: &structpb.Value_NumberValue{NumberValue: chunkLength}},
					"chunk_overlap":    {Kind: &structpb.Value_NumberValue{NumberValue: chunkOverlap}},
				},
			},
		},
	}
	res, err := s.PipelinePub.TriggerNamespacePipelineRelease(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to trigger %s pipeline. err:%w", MdSplitPipelineID, err)
	}
	result, err := GetChunksFromResponse(res)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// GetChunksFromResponse converts the pipeline response into a slice of Chunk.
func GetChunksFromResponse(resp *pipelinev1beta.TriggerNamespacePipelineReleaseResponse) ([]Chunk, error) {
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
		tokenCount := int(v.GetStructValue().Fields["token-count"].GetNumberValue())
		chunks = append(chunks, Chunk{
			End:    endPos,
			Start:  startPos,
			Text:   text,
			Tokens: tokenCount,
		})
	}
	return chunks, nil
}

// SplitTextPipe using splitting pipeline to split text and consume caller's credits
func (s *Service) SplitTextPipe(ctx context.Context, caller uuid.UUID, requester uuid.UUID, text string) ([]Chunk, error) {
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
	req := &pipelinev1beta.TriggerNamespacePipelineReleaseRequest{
		NamespaceId: NamespaceID,
		PipelineId:  TextSplitPipelineID,
		ReleaseId:   TextSplitVersion,

		Inputs: []*structpb.Struct{
			{
				Fields: map[string]*structpb.Value{
					"text_input":       {Kind: &structpb.Value_StringValue{StringValue: text}},
					"max_chunk_length": {Kind: &structpb.Value_NumberValue{NumberValue: chunkLength}},
					"chunk_overlap":    {Kind: &structpb.Value_NumberValue{NumberValue: chunkOverlap}},
				},
			},
		},
	}
	res, err := s.PipelinePub.TriggerNamespacePipelineRelease(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to trigger %s pipeline. err:%w", TextSplitPipelineID, err)
	}
	result, err := GetChunksFromResponse(res)
	if err != nil {
		return nil, fmt.Errorf("failed to get chunks from response: %w", err)
	}
	return result, nil
}

// VectorizeTextPipe using embedding pipeline to vectorize text and consume caller's credits
func (s *Service) VectorizeTextPipe(ctx context.Context, caller uuid.UUID, requester uuid.UUID, texts []string) ([][]float32, error) {
	const maxBatchSize = 32
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
	var allResults [][]float32

	for i := 0; i < len(texts); i += maxBatchSize {
		end := i + maxBatchSize
		if end > len(texts) {
			end = len(texts)
		}
		batch := texts[i:end]

		inputs := make([]*structpb.Struct, 0, len(batch))
		for _, text := range batch {
			inputs = append(inputs, &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"chunk_input": {Kind: &structpb.Value_StringValue{StringValue: text}},
				},
			})
		}

		req := &pipelinev1beta.TriggerNamespacePipelineReleaseRequest{
			NamespaceId: NamespaceID,
			PipelineId:  TextEmbedPipelineID,
			ReleaseId:   TextEmbedVersion,
			Inputs:      inputs,
		}
		res, err := s.PipelinePub.TriggerNamespacePipelineRelease(ctx, req)
		if err != nil {
			return nil, fmt.Errorf("failed to trigger %s pipeline. err:%w", TextEmbedPipelineID, err)
		}
		result, err := GetVectorFromResponse(res)
		if err != nil {
			return nil, fmt.Errorf("failed to get vector from response: %w", err)
		}
		allResults = append(allResults, result...)
	}

	return allResults, nil
}

// GetVectorFromResponse converts the pipeline response into a slice of float32.
func GetVectorFromResponse(resp *pipelinev1beta.TriggerNamespacePipelineReleaseResponse) ([][]float32, error) {
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

// VectorizeText using embedding pipeline to vectorize text and consume caller's credits
func (s *Service) QuestionAnsweringPipe(ctx context.Context, caller uuid.UUID, requester uuid.UUID, question string, simchunks []string) (string, error) {
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
	// create a retired chunk var that combines all the chunks by /n/n
	retrievedChunk := ""
	for _, chunk := range simchunks {
		retrievedChunk += chunk + "\n\n"
	}
	ctx = metadata.NewOutgoingContext(ctx, md)
	req := &pipelinev1beta.TriggerNamespacePipelineReleaseRequest{
		NamespaceId: NamespaceID,
		PipelineId:  RetrievingQnA,
		ReleaseId:   QAVersion,
		Inputs: []*structpb.Struct{
			{
				Fields: map[string]*structpb.Value{
					"retrieved_chunk": {Kind: &structpb.Value_StringValue{StringValue: retrievedChunk}},
					"user_question":   {Kind: &structpb.Value_StringValue{StringValue: question}},
				},
			},
		},
	}
	res, err := s.PipelinePub.TriggerNamespacePipelineRelease(ctx, req)
	if err != nil {
		return "", fmt.Errorf("failed to trigger %s pipeline. err:%w", RetrievingQnA, err)
	}
	reply := res.Outputs[0].GetFields()["assistant_reply"].GetStringValue()
	return reply, nil
}
