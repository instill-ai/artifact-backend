package pipeline

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/types/known/structpb"

	pipelinepb "github.com/instill-ai/protogen-go/pipeline/pipeline/v1beta"
)

// NOTE: ChunkMarkdownPipe, ChunkTextPipe, GenerateSummaryPipe, and GenerateContentPipe
// have been removed as chunking is now done internally by ChunkContentActivity,
// and content generation/summarization is handled by AI providers (Gemini).

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
