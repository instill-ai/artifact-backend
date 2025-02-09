package service

import (
	"context"
	"embed"
	"fmt"
	"sync"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/instill-ai/artifact-backend/pkg/constant"
	"github.com/instill-ai/artifact-backend/pkg/logger"
	"github.com/instill-ai/artifact-backend/pkg/utils"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	pipelinepb "github.com/instill-ai/protogen-go/pipeline/pipeline/v1beta"
)

//go:embed preset/pipelines/*
var PresetPipelinesFS embed.FS

const maxChunkLengthForPersistentCatalog = 1024
const maxChunkLengthForTempCatalog = 7000

const chunkOverlapForPersistentCatalog = 200
const chunkOverlapForTempCatalog = 700

const NamespaceID = "preset"

// Note: this pipeline is for the old indexing pipeline having convert_result
const ConvertDocToMDPipelineID = "indexing-convert-pdf"
const DocToMDVersion = "v1.1.1"

// Note: this pipeline is for the new indexing pipeline having convert_result or convert_result2
const ConvertDocToMDPipelineID2 = "indexing-advanced-convert-doc"
const DocToMDVersion2 = "v1.3.1"

const ChunkMdPipelineID = "indexing-split-markdown"
const ChunkMdVersion = "v2.0.0"

const ChunkTextPipelineID = "indexing-split-text"
const ChunkTextVersion = "v2.0.0"

const EmbedTextPipelineID = "indexing-embed"
const EmbedTextVersion = "v1.1.0"

const QAPipelineID = "retrieving-qna"
const QAVersion = "v1.2.0"

var PresetPipelinesList = []struct {
	ID      string
	Version string
}{
	{ID: ConvertDocToMDPipelineID, Version: DocToMDVersion},
	{ID: ConvertDocToMDPipelineID2, Version: DocToMDVersion2},
	{ID: ChunkMdPipelineID, Version: ChunkMdVersion},
	{ID: ChunkTextPipelineID, Version: ChunkTextVersion},
	{ID: EmbedTextPipelineID, Version: EmbedTextVersion},
	{ID: QAPipelineID, Version: QAVersion},
}

// ConvertToMDPipeForFilesInPersistentCatalog using converting pipeline to convert some file type to MD and consume caller's credits
func (s *Service) ConvertToMDPipeForFilesInPersistentCatalog(ctx context.Context, fileUID uuid.UUID, caller uuid.UUID, requester uuid.UUID, fileBase64 string, fileType artifactpb.FileType) (string, error) {
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

	// Determine which pipeline and version to use based on file type
	var pipelineID string
	var version string

	switch fileType {
	// Document types use the new pipeline
	case artifactpb.FileType_FILE_TYPE_PDF,
		artifactpb.FileType_FILE_TYPE_DOCX,
		artifactpb.FileType_FILE_TYPE_DOC,
		artifactpb.FileType_FILE_TYPE_PPT,
		artifactpb.FileType_FILE_TYPE_PPTX:
		pipelineID = ConvertDocToMDPipelineID2
		version = DocToMDVersion2

	// Spreadsheet types and others use the original pipeline
	case artifactpb.FileType_FILE_TYPE_XLSX,
		artifactpb.FileType_FILE_TYPE_XLS,
		artifactpb.FileType_FILE_TYPE_CSV,
		artifactpb.FileType_FILE_TYPE_HTML:
		pipelineID = ConvertDocToMDPipelineID
		version = DocToMDVersion

	default:
		return "", fmt.Errorf("unsupported file type: %v", fileType)
	}

	// save the converting pipeline metadata into database
	convertingPipelineMetadata := NamespaceID + "/" + pipelineID + "@" + version
	err := s.Repository.UpdateKbFileExtraMetaData(ctx, fileUID, "", convertingPipelineMetadata, "", "", nil, nil, nil, nil)
	if err != nil {
		logger.Error("Failed to save converting pipeline metadata.", zap.String("File uid:", fileUID.String()))
		return "", fmt.Errorf("failed to save converting pipeline metadata: %w", err)
	}

	req := &pipelinepb.TriggerNamespacePipelineReleaseRequest{
		NamespaceId: NamespaceID,
		PipelineId:  pipelineID,
		ReleaseId:   version,
		Inputs: []*structpb.Struct{
			{
				Fields: map[string]*structpb.Value{
					"document_input": {Kind: &structpb.Value_StringValue{StringValue: prefix + fileBase64}},
					"vlm_model":      {Kind: &structpb.Value_StringValue{StringValue: "gpt-4o"}},
				},
			},
		},
	}

	resp, err := s.PipelinePub.TriggerNamespacePipelineRelease(ctx, req)
	if err != nil {
		logger.Error("failed to trigger pipeline", zap.Error(err))
		return "", fmt.Errorf("failed to trigger %s pipeline: %w", pipelineID, err)
	}

	result, err := getConvertResult(resp)
	if err != nil {
		logger.Error("failed to get convert result", zap.Error(err))
		return "", fmt.Errorf("failed to get convert result: %w", err)
	}
	return result, nil
}

// ConvertToMDPipeForFilesInTempCatalog using converting pipeline to convert some file type to MD and consume caller's credits
func (s *Service) ConvertToMDPipeForFilesInTempCatalog(ctx context.Context, fileUID uuid.UUID, caller uuid.UUID, requester uuid.UUID, fileBase64 string, fileType artifactpb.FileType) (string, error) {
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

	// Determine which pipeline and version to use based on file type
	var pipelineID string
	var version string

	switch fileType {
	// Document types use the new pipeline
	case artifactpb.FileType_FILE_TYPE_PDF,
		artifactpb.FileType_FILE_TYPE_DOCX,
		artifactpb.FileType_FILE_TYPE_DOC,
		artifactpb.FileType_FILE_TYPE_PPT,
		artifactpb.FileType_FILE_TYPE_PPTX,
		artifactpb.FileType_FILE_TYPE_XLSX,
		artifactpb.FileType_FILE_TYPE_XLS,
		artifactpb.FileType_FILE_TYPE_CSV,
		artifactpb.FileType_FILE_TYPE_HTML:
		pipelineID = ConvertDocToMDPipelineID
		version = DocToMDVersion

	default:
		return "", fmt.Errorf("unsupported file type: %v", fileType)
	}

	// save the converting pipeline metadata into database
	convertingPipelineMetadata := NamespaceID + "/" + pipelineID + "@" + version
	err := s.Repository.UpdateKbFileExtraMetaData(ctx, fileUID, "", convertingPipelineMetadata, "", "", nil, nil, nil, nil)
	if err != nil {
		logger.Error("Failed to save converting pipeline metadata.", zap.String("File uid:", fileUID.String()))
		return "", fmt.Errorf("failed to save converting pipeline metadata: %w", err)
	}

	req := &pipelinepb.TriggerNamespacePipelineReleaseRequest{
		NamespaceId: NamespaceID,
		PipelineId:  pipelineID,
		ReleaseId:   version,
		Inputs: []*structpb.Struct{
			{
				Fields: map[string]*structpb.Value{
					"document_input": {Kind: &structpb.Value_StringValue{StringValue: prefix + fileBase64}},
				},
			},
		},
	}

	resp, err := s.PipelinePub.TriggerNamespacePipelineRelease(ctx, req)
	if err != nil {
		logger.Error("failed to trigger pipeline", zap.Error(err))
		return "", fmt.Errorf("failed to trigger %s pipeline: %w", pipelineID, err)
	}

	result, err := getConvertResult(resp)
	if err != nil {
		logger.Error("failed to get convert result", zap.Error(err))
		return "", fmt.Errorf("failed to get convert result: %w", err)
	}
	return result, nil
}

// getFileTypePrefix returns the appropriate prefix for the given file type
func getFileTypePrefix(fileType artifactpb.FileType) string {
	switch fileType {
	case artifactpb.FileType_FILE_TYPE_PDF:
		return "data:application/pdf;base64,"
	case artifactpb.FileType_FILE_TYPE_DOCX:
		return "data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,"
	case artifactpb.FileType_FILE_TYPE_DOC:
		return "data:application/msword;base64,"
	case artifactpb.FileType_FILE_TYPE_PPT:
		return "data:application/vnd.ms-powerpoint;base64,"
	case artifactpb.FileType_FILE_TYPE_PPTX:
		return "data:application/vnd.openxmlformats-officedocument.presentationml.presentation;base64,"
	case artifactpb.FileType_FILE_TYPE_HTML:
		return "data:text/html;base64,"
	case artifactpb.FileType_FILE_TYPE_TEXT:
		return "data:text/plain;base64,"
	case artifactpb.FileType_FILE_TYPE_XLSX:
		return "data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,"
	case artifactpb.FileType_FILE_TYPE_XLS:
		return "data:application/vnd.ms-excel;base64,"
	case artifactpb.FileType_FILE_TYPE_CSV:
		return "data:text/csv;base64,"
	default:
		return ""
	}
}

// getConvertResult extracts the conversion result from the pipeline response.
// It first checks for a non-empty "convert_result" field, then falls back to "convert_result2".
// Returns an error if neither field contains valid data or if the response structure is invalid.
func getConvertResult(resp *pipelinepb.TriggerNamespacePipelineReleaseResponse) (string, error) {
	if resp == nil || len(resp.Outputs) == 0 {
		return "", fmt.Errorf("response is nil or has no outputs. resp: %v", resp)
	}
	fields := resp.Outputs[0].GetFields()
	if fields == nil {
		return "", fmt.Errorf("fields in the output are nil. resp: %v", resp)
	}
	convertResult, ok := fields["convert_result"]
	if ok && convertResult.GetStringValue() != "" {
		return convertResult.GetStringValue(), nil
	}
	convertResult2, ok2 := fields["convert_result2"]
	if ok2 && convertResult2.GetStringValue() != "" {
		return convertResult2.GetStringValue(), nil
	}
	return "", fmt.Errorf("convert_result or convert_result2 not found in the output fields. resp: %v", resp)
}

type Chunk = struct {
	End    int
	Start  int
	Text   string
	Tokens int
}

// ChunkMarkdownPipe triggers the markdown splitting pipeline, processes the markdown text, and deducts credits from the caller's account.
// It sets up the necessary metadata, triggers the pipeline, and processes the response to return the non-empty chunks.
func (s *Service) ChunkMarkdownPipe(ctx context.Context, caller uuid.UUID, requester uuid.UUID, markdown string) ([]Chunk, error) {
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
	req := &pipelinepb.TriggerNamespacePipelineReleaseRequest{
		NamespaceId: NamespaceID,
		PipelineId:  ChunkMdPipelineID,
		ReleaseId:   ChunkMdVersion,
		Inputs: []*structpb.Struct{
			{
				Fields: map[string]*structpb.Value{
					"md_input":         {Kind: &structpb.Value_StringValue{StringValue: markdown}},
					"max_chunk_length": {Kind: &structpb.Value_NumberValue{NumberValue: maxChunkLengthForPersistentCatalog}},
					"chunk_overlap":    {Kind: &structpb.Value_NumberValue{NumberValue: chunkOverlapForPersistentCatalog}},
				},
			},
		},
	}
	res, err := s.PipelinePub.TriggerNamespacePipelineRelease(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to trigger %s pipeline. err:%w", ChunkMdPipelineID, err)
	}
	result, err := GetChunksFromResponse(res)
	if err != nil {
		return nil, err
	}
	// remove the empty chunk.
	// note: this is a workaround for the pipeline bug that sometimes returns empty chunks.
	var filteredResult []Chunk
	for _, chunk := range result {
		if chunk.Text != "" {
			filteredResult = append(filteredResult, chunk)
		}
	}
	return filteredResult, nil
}

// GetChunksFromResponse converts the pipeline response into a slice of Chunk.
func GetChunksFromResponse(resp *pipelinepb.TriggerNamespacePipelineReleaseResponse) ([]Chunk, error) {
	if resp == nil || len(resp.Outputs) == 0 {
		return nil, fmt.Errorf("response is nil or has no outputs. resp: %v", resp)
	}
	splitResult, ok := resp.Outputs[0].GetFields()["split_result"]

	if !ok {
		return nil, fmt.Errorf("split_result not found in the output fields. resp: %v", resp)
	}
	listValue := splitResult.GetListValue()
	if listValue == nil {
		return nil, fmt.Errorf("split_result is not a list. resp: %v", resp)
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

// ChunkTextPipeForPersistentCatalog splits the input text into chunks using the splitting pipeline and consumes the caller's credits.
// It sets up the necessary metadata, triggers the pipeline, and processes the response to return the non-empty chunks.
func (s *Service) ChunkTextPipeForPersistentCatalog(ctx context.Context, caller uuid.UUID, requester uuid.UUID, text string) ([]Chunk, error) {
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
	req := &pipelinepb.TriggerNamespacePipelineReleaseRequest{
		NamespaceId: NamespaceID,
		PipelineId:  ChunkTextPipelineID,
		ReleaseId:   ChunkTextVersion,

		Inputs: []*structpb.Struct{
			{
				Fields: map[string]*structpb.Value{
					"text_input":       {Kind: &structpb.Value_StringValue{StringValue: text}},
					"max_chunk_length": {Kind: &structpb.Value_NumberValue{NumberValue: maxChunkLengthForPersistentCatalog}},
					"chunk_overlap":    {Kind: &structpb.Value_NumberValue{NumberValue: chunkOverlapForPersistentCatalog}},
				},
			},
		},
	}
	res, err := s.PipelinePub.TriggerNamespacePipelineRelease(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to trigger %s pipeline. err:%w", ChunkTextPipelineID, err)
	}
	result, err := GetChunksFromResponse(res)
	if err != nil {
		return nil, fmt.Errorf("failed to get chunks from response: %w", err)
	}
	// remove the empty chunk.
	// note: this is a workaround for the pipeline bug that sometimes returns empty chunks.
	var filteredResult []Chunk
	for _, chunk := range result {
		if chunk.Text != "" {
			filteredResult = append(filteredResult, chunk)
		}
	}
	return filteredResult, nil
}

// ChunkTextPipeForTempCatalog splits the input text into chunks using the splitting pipeline and consumes the caller's credits.
// It sets up the necessary metadata, triggers the pipeline, and processes the response to return the non-empty chunks.
func (s *Service) ChunkTextPipeForTempCatalog(ctx context.Context, caller uuid.UUID, requester uuid.UUID, text string) ([]Chunk, error) {
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
	req := &pipelinepb.TriggerNamespacePipelineReleaseRequest{
		NamespaceId: NamespaceID,
		PipelineId:  ChunkTextPipelineID,
		ReleaseId:   ChunkTextVersion,

		Inputs: []*structpb.Struct{
			{
				Fields: map[string]*structpb.Value{
					"text_input":       {Kind: &structpb.Value_StringValue{StringValue: text}},
					"max_chunk_length": {Kind: &structpb.Value_NumberValue{NumberValue: maxChunkLengthForTempCatalog}},
					"chunk_overlap":    {Kind: &structpb.Value_NumberValue{NumberValue: chunkOverlapForTempCatalog}},
				},
			},
		},
	}
	res, err := s.PipelinePub.TriggerNamespacePipelineRelease(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to trigger %s pipeline. err:%w", ChunkTextPipelineID, err)
	}
	result, err := GetChunksFromResponse(res)
	if err != nil {
		return nil, fmt.Errorf("failed to get chunks from response: %w", err)
	}
	// remove the empty chunk.
	// note: this is a workaround for the pipeline bug that sometimes returns empty chunks.
	var filteredResult []Chunk
	for _, chunk := range result {
		if chunk.Text != "" {
			filteredResult = append(filteredResult, chunk)
		}
	}
	return filteredResult, nil
}

// EmbeddingTextPipe converts multiple text inputs into vector embeddings using a pipeline service.
// It processes texts in parallel batches for efficiency while managing resource usage.
//
// Parameters:
//   - ctx: Context for the operation
//   - caller: UUID of the calling user
//   - requester: UUID of the requesting entity (optional)
//   - texts: Slice of strings to be converted to embeddings
//
// Returns:
//   - [][]float32: 2D slice where each inner slice is a vector embedding
//   - error: Any error encountered during processing
//
// The function:
//   - Processes texts in batches of 32
//   - Limits concurrent processing to 5 goroutines
//   - Maintains input order in the output
//   - Cancels all operations if any batch fails
func (s *Service) EmbeddingTextPipe(ctx context.Context, caller uuid.UUID, requester uuid.UUID, texts []string) ([][]float32, error) {
	ctx, ctxCancel := context.WithCancel(ctx)
	defer ctxCancel()
	const maxBatchSize = 32
	const maxConcurrentGoroutines = 5
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

	var allResults [][]float32
	var wg sync.WaitGroup
	errChan := make(chan error, len(texts)/maxBatchSize+1)
	resultsChan := make(chan struct {
		index  int
		result [][]float32
	}, len(texts)/maxBatchSize+1)

	// Loop through the texts in batches of maxBatchSize (32).
	// For each batch, create a new goroutine to process the batch concurrently.
	// In each goroutine:
	// - Create a new context with metadata.
	// - Prepare the inputs for the pipeline request.
	// - Trigger the pipeline and get the response.
	// - Extract the vector from the response.
	// - Send the result to the results channel.
	// If an error occurs, send the error to the error channel.
	// Create a semaphore channel to limit concurrent goroutines to maxConcurrentGoroutines
	sem := make(chan struct{}, maxConcurrentGoroutines)
	for i := 0; i < len(texts); i += maxBatchSize {

		end := i + maxBatchSize
		if end > len(texts) {
			end = len(texts)
		}
		batch := texts[i:end]
		batchIndex := i / maxBatchSize

		// Acquire semaphore before starting goroutine
		sem <- struct{}{}
		wg.Add(1)
		go utils.GoRecover(func() {
			// Release semaphore when goroutine completes
			defer func() { <-sem }()
			defer wg.Done()

			func(batch []string, index int) {
				ctx_ := metadata.NewOutgoingContext(ctx, md)

				inputs := make([]*structpb.Struct, 0, len(batch))
				for _, text := range batch {
					inputs = append(inputs, &structpb.Struct{
						Fields: map[string]*structpb.Value{
							"chunk_input": {Kind: &structpb.Value_StringValue{StringValue: text}},
						},
					})
				}

				req := &pipelinepb.TriggerNamespacePipelineReleaseRequest{
					NamespaceId: NamespaceID,
					PipelineId:  EmbedTextPipelineID,
					ReleaseId:   EmbedTextVersion,
					Inputs:      inputs,
				}
				res, err := s.PipelinePub.TriggerNamespacePipelineRelease(ctx_, req)
				if err != nil {
					errChan <- fmt.Errorf("failed to trigger %s pipeline. err:%w", EmbedTextPipelineID, err)
					ctxCancel()
					return
				}
				result, err := GetVectorsFromResponse(res)
				if err != nil {
					errChan <- fmt.Errorf("failed to get vector from response: %w", err)
					ctxCancel()
					return
				}

				resultsChan <- struct {
					index  int
					result [][]float32
				}{index: index, result: result}
			}(batch, batchIndex)
		}, fmt.Sprintf("EmbeddingTextPipe %d-%d", i, end))
	}

	// wait for all the goroutines to finish
	wg.Wait()
	close(resultsChan)
	close(errChan)
	// collect the results in the order of the input texts
	orderedResults := make([][][]float32, len(texts)/maxBatchSize+1)
	for res := range resultsChan {
		orderedResults[res.index] = res.result
	}

	// flatten the ordered results
	for _, result := range orderedResults {
		allResults = append(allResults, result...)
	}

	if len(errChan) > 0 {
		return nil, <-errChan
	}
	return allResults, nil
}

// GetVectorsFromResponse converts the pipeline response into a slice of float32.
func GetVectorsFromResponse(resp *pipelinepb.TriggerNamespacePipelineReleaseResponse) ([][]float32, error) {
	if resp == nil || len(resp.Outputs) == 0 {
		return nil, fmt.Errorf("response is nil or has no outputs. resp: %v", resp)
	}

	vectors := make([][]float32, 0, len(resp.Outputs))
	for _, output := range resp.Outputs {
		embedResult, ok := output.GetFields()["embed_result"]
		if !ok {
			return nil, fmt.Errorf("embed_result not found in the output fields. pipeline's output: %v. pipeline's metadata: %v", output, resp.Metadata)
		}
		listValue := embedResult.GetListValue()
		if listValue == nil {
			return nil, fmt.Errorf("embed_result is not a list. pipeline's output: %v. pipeline's metadata: %v", output, resp.Metadata)
		}

		vector := make([]float32, 0, len(listValue.GetValues()))
		for _, v := range listValue.GetValues() {
			vector = append(vector, float32(v.GetNumberValue()))
		}
		vectors = append(vectors, vector)
	}

	return vectors, nil
}

// VectoringText using embedding pipeline to vector text and consume caller's credits
func (s *Service) QuestionAnsweringPipe(ctx context.Context, caller uuid.UUID, requester uuid.UUID, question string, simChunks []string) (string, error) {
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
	for _, chunk := range simChunks {
		retrievedChunk += chunk + "\n\n"
	}
	ctx = metadata.NewOutgoingContext(ctx, md)
	req := &pipelinepb.TriggerNamespacePipelineReleaseRequest{
		NamespaceId: NamespaceID,
		PipelineId:  QAPipelineID,
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
		return "", fmt.Errorf("failed to trigger %s pipeline. err:%w", QAPipelineID, err)
	}
	reply := res.Outputs[0].GetFields()["assistant_reply"].GetStringValue()
	return reply, nil
}
