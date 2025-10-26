package pipeline

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/structpb"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	pipelinepb "github.com/instill-ai/protogen-go/pipeline/pipeline/v1beta"
)

// GenerateContentPipe converts a file to Markdown using OpenAI pipeline
func GenerateContentPipe(ctx context.Context, pipelineClient pipelinepb.PipelinePublicServiceClient, params GenerateContentParams) (*GenerateContentResult, error) {
	ctx, cancel := context.WithTimeout(ctx, 300*time.Second)
	defer cancel()

	prefix := GetFileTypePrefix(params.Type)
	input := &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"document_input": structpb.NewStringValue(prefix + params.Base64Content),
		},
	}

	req := &pipelinepb.TriggerNamespacePipelineReleaseRequest{
		NamespaceId: GenerateContentPipeline.Namespace,
		PipelineId:  GenerateContentPipeline.ID,
		ReleaseId:   GenerateContentPipeline.Version,
		Inputs:      []*structpb.Struct{input},
	}

	resp, err := pipelineClient.TriggerNamespacePipelineRelease(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("triggering pipeline: %w", err)
	}

	result, err := convertResultParser(resp)
	if err != nil {
		return nil, err
	}
	result.PipelineRelease = GenerateContentPipeline
	return result, nil
}

// GenerateSummaryPipe generates summary using OpenAI pipeline
func GenerateSummaryPipe(ctx context.Context, pipelineClient pipelinepb.PipelinePublicServiceClient, content string, fileType artifactpb.File_Type) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 300*time.Second)
	defer cancel()

	req := &pipelinepb.TriggerNamespacePipelineReleaseRequest{
		NamespaceId: GenerateSummaryPipeline.Namespace,
		PipelineId:  GenerateSummaryPipeline.ID,
		ReleaseId:   GenerateSummaryPipeline.Version,
		Data: []*pipelinepb.TriggerData{
			{
				Variable: &structpb.Struct{
					Fields: map[string]*structpb.Value{
						"file_type": structpb.NewStringValue(fileType.String()),
						"context":   structpb.NewStringValue(content),
						"llm_model": structpb.NewStringValue("gpt-4o-mini"),
					},
				},
			},
		},
	}

	resp, err := pipelineClient.TriggerNamespacePipelineRelease(ctx, req)
	if err != nil {
		return "", fmt.Errorf("triggering pipeline: %w", err)
	}

	return getGenerateSummaryResult(resp)
}

// EmbedPipe generates embeddings using OpenAI pipeline
// Returns embedding vectors for a batch of texts
func EmbedPipe(ctx context.Context, pipelineClient pipelinepb.PipelinePublicServiceClient, texts []string) ([][]float32, error) {
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()

	if len(texts) == 0 {
		return [][]float32{}, nil
	}

	// Build inputs for batch processing
	inputs := make([]*structpb.Struct, len(texts))
	for i, text := range texts {
		inputs[i] = &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"text": structpb.NewStringValue(text),
			},
		}
	}

	req := &pipelinepb.TriggerNamespacePipelineReleaseRequest{
		NamespaceId: EmbedPipeline.Namespace,
		PipelineId:  EmbedPipeline.ID,
		ReleaseId:   EmbedPipeline.Version,
		Inputs:      inputs,
	}

	resp, err := pipelineClient.TriggerNamespacePipelineRelease(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("triggering embedding pipeline: %w", err)
	}

	// Parse embeddings from response
	embeddings := make([][]float32, len(resp.Outputs))
	for i, output := range resp.Outputs {
		fields := output.GetFields()
		if fields == nil {
			return nil, fmt.Errorf("output %d has no fields", i)
		}

		embeddingValue := fields["embedding"]
		if embeddingValue == nil {
			return nil, fmt.Errorf("output %d missing 'embedding' field", i)
		}

		// Extract float array from list value
		listValue := embeddingValue.GetListValue()
		if listValue == nil {
			return nil, fmt.Errorf("output %d embedding is not a list", i)
		}

		embedding := make([]float32, len(listValue.Values))
		for j, val := range listValue.Values {
			embedding[j] = float32(val.GetNumberValue())
		}
		embeddings[i] = embedding
	}

	return embeddings, nil
}

// ChatPipe processes a prompt with retrieved chunks using the QA pipeline
func ChatPipe(ctx context.Context, pipelineClient pipelinepb.PipelinePublicServiceClient, prompt string, simChunks []string) (string, error) {
	// create a retired chunk var that combines all the chunks by /n/n
	retrievedChunk := ""
	for _, chunk := range simChunks {
		retrievedChunk += chunk + "\n\n"
	}

	req := &pipelinepb.TriggerNamespacePipelineReleaseRequest{
		NamespaceId: QAPipeline.Namespace,
		PipelineId:  QAPipeline.ID,
		ReleaseId:   QAPipeline.Version,
		Inputs: []*structpb.Struct{
			{
				Fields: map[string]*structpb.Value{
					"retrieved_chunk": structpb.NewStringValue(retrievedChunk),
					"user_question":   structpb.NewStringValue(prompt),
				},
			},
		},
	}

	res, err := pipelineClient.TriggerNamespacePipelineRelease(ctx, req)
	if err != nil {
		return "", fmt.Errorf("failed to trigger %s pipeline. err:%w", QAPipeline.ID, err)
	}

	reply := res.Outputs[0].GetFields()["assistant_reply"].GetStringValue()
	return reply, nil
}
