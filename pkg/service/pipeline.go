package service

import (
	"context"
	"embed"
	"fmt"
	"strings"
	"sync"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"
	"golang.org/x/mod/semver"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/constant"
	"github.com/instill-ai/artifact-backend/pkg/logger"
	"github.com/instill-ai/artifact-backend/pkg/utils"
	"github.com/instill-ai/x/resource"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	pipelinepb "github.com/instill-ai/protogen-go/pipeline/pipeline/v1beta"
)

//go:embed preset/pipelines/*
var PresetPipelinesFS embed.FS

const maxChunkLengthForMarkdownCatalog = 1024
const maxChunkLengthForTextCatalog = 7000

const chunkOverlapForMarkdowntCatalog = 200
const chunkOverlapForTextCatalog = 700

const defaultNamespaceID = "preset"

// PipelineRelease identifies a pipeline used in catalog file processing.
type PipelineRelease struct {
	Namespace string
	ID        string
	Version   string
}

// Name returns a human-readable, unique identifier for a pipeline release.
func (pr PipelineRelease) Name() string {
	return pr.Namespace + "/" + pr.ID + "@" + pr.Version
}

// PipelineReleaseFromName parses a PipelineRelease from its name, with the
// format {namespace}/{id}@{version}.
func PipelineReleaseFromName(name string) (PipelineRelease, error) {
	pr := PipelineRelease{}

	parts := strings.Split(name, "/")
	if len(parts) != 2 {
		return pr, fmt.Errorf("name must have the format {namespace}/{id}@{version}")
	}
	pr.Namespace = parts[0]

	idVersion := strings.Split(parts[1], "@")
	if len(idVersion) != 2 {
		return pr, fmt.Errorf("name must have the format {namespace}/{id}@{version}")
	}

	pr.ID = idVersion[0]
	pr.Version = idVersion[1]

	if !semver.IsValid(pr.Version) {
		return pr, fmt.Errorf("version must be valid SemVer 2.0.0")
	}

	return pr, nil
}

var (
	// ConvertDocToMDRouterPipeline is a pipeline that routes the document
	// conversion to different parsers (heuristic, fast, docling, vlm-ocr,
	// vlm-refinement) depending on the document characteristics.
	// NOTE: this pipeline depends on the existence of pipelines and models, so
	// it will only be used on Instill Agent requests.
	// TODO jvallesm: we need an artifact-backend-ee distribution to avoid such
	// conditional routing.
	ConvertDocToMDRouterPipeline = PipelineRelease{
		Namespace: defaultNamespaceID,
		ID:        "parsing-router",
		Version:   "v1.0.0",
	}

	// ConvertDocVLM is used by ConvertDocToMDRouterPipeline to extract
	// Markdown from a set of images.
	ConvertDocVLM = PipelineRelease{
		Namespace: defaultNamespaceID,
		ID:        "vlm-ocr",
		Version:   "v1.0.0",
	}

	// ConvertDocVLMRefinement is used by ConvertDocToMDRouterPipeline to
	// enhance a heuristic document-to-Markdown conversion using a VLM and
	// a set of images from the document.
	ConvertDocVLMRefinement = PipelineRelease{
		Namespace: defaultNamespaceID,
		ID:        "vlm-refinement",
		Version:   "v1.0.0",
	}

	// ConvertDocToMDPipeline is the default conversion pipeline for documents.
	// Note: this pipeline is for the new indexing pipeline having
	// convert_result or convert_result2
	ConvertDocToMDPipeline = PipelineRelease{
		Namespace: defaultNamespaceID,
		ID:        "indexing-advanced-convert-doc",
		Version:   "v1.3.1",
	}

	// ConvertDocToMDStandardPipeline is the default conversion pipeline for
	// non-document files (e.g. CSV).
	ConvertDocToMDStandardPipeline = PipelineRelease{
		Namespace: defaultNamespaceID,
		ID:        "indexing-convert-pdf",
		Version:   "v1.1.1",
	}

	// GenerateSummaryPipeline is the default pipeline for summarizing text.
	GenerateSummaryPipeline = PipelineRelease{
		Namespace: defaultNamespaceID,
		ID:        "indexing-generate-summary",
		Version:   "v1.0.0",
	}

	// ChunkMDPipeline is the default pipeline for chunking Markdown.
	ChunkMDPipeline = PipelineRelease{
		Namespace: defaultNamespaceID,
		ID:        "indexing-split-markdown",
		Version:   "v2.0.0",
	}

	// ChunkTextPipeline is the default pipeline for chunking text.
	ChunkTextPipeline = PipelineRelease{
		Namespace: defaultNamespaceID,
		ID:        "indexing-split-text",
		Version:   "v2.0.0",
	}

	// EmbedTextPipeline is the defualt pipeline for embedding text.
	EmbedTextPipeline = PipelineRelease{
		Namespace: defaultNamespaceID,
		ID:        "indexing-embed",
		Version:   "v1.1.0",
	}

	// QAPipeline is the default pipeline for question & answering.
	QAPipeline = PipelineRelease{
		Namespace: defaultNamespaceID,
		ID:        "retrieving-qna",
		Version:   "v1.2.0",
	}

	// PresetPipelinesList contains the preset pipelines used in catalogs.
	PresetPipelinesList = []PipelineRelease{
		ConvertDocToMDPipeline,
		ConvertDocToMDStandardPipeline,
		GenerateSummaryPipeline,
		ChunkMDPipeline,
		ChunkTextPipeline,
		EmbedTextPipeline,
		QAPipeline,

		// Agent-only pipelines.
		ConvertDocToMDRouterPipeline,
		ConvertDocVLM,
		ConvertDocVLMRefinement,
	}
)

// ConvertToMDPipe converts a file into Markdown by triggering a converting
// pipeline.
//   - If the file has a document type extension (pdf, doc[x], ppt[x]) the
//     client may specify a slice of pipelines, which will be triggered in
//     order until a successful trigger produces a non-empty result.
//   - If no pipelines are specified, ConvertDocToMDPipeline will be used by
//     default.
//   - Non-document files will use ConvertDocToMDStandardPipeline, as these
//     types tend to be trivial to convert and can use a deterministic pipeline
//     instead of a custom one that improves the conversion performance.
func (s *Service) ConvertToMDPipe(
	ctx context.Context,
	fileUID uuid.UUID,
	fileBase64 string,
	fileType artifactpb.FileType,
	pipelines []PipelineRelease,
) (string, error) {

	logger, _ := logger.GetZapLogger(ctx)

	// Get the appropriate prefix for the file type
	prefix := getFileTypePrefix(fileType)

	input := &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"document_input": structpb.NewStringValue(prefix + fileBase64),
		},
	}

	getConvertResult := simpleConvertResultParser

	// Determine which pipeline and version to use based on file type
	switch fileType {
	// Spreadsheet types and others use the original pipeline
	case artifactpb.FileType_FILE_TYPE_XLSX,
		artifactpb.FileType_FILE_TYPE_XLS,
		artifactpb.FileType_FILE_TYPE_CSV,
		artifactpb.FileType_FILE_TYPE_HTML:

		pipelines = []PipelineRelease{ConvertDocToMDStandardPipeline}

	// Document types use the conversion pipeline configured in the catalog, if
	// present, or the default one for documents (parsing-router if the request
	// comes from Instill Agent, the advanced conversion pipeline otherwise).
	case artifactpb.FileType_FILE_TYPE_PDF,
		artifactpb.FileType_FILE_TYPE_DOCX,
		artifactpb.FileType_FILE_TYPE_DOC,
		artifactpb.FileType_FILE_TYPE_PPT,
		artifactpb.FileType_FILE_TYPE_PPTX:

		if len(pipelines) != 0 {
			break
		}

		// Instill Agent requests are identified by the HeaderBackendKey in the
		// request metadata.
		// TODO jvallesm: a better way to implement this would be by setting
		// the parsing router pipeline as the conversion pipeline when creating
		// the catalog from agent. However, since the input isn't the default
		// one (taking only `document_input`, we'd still need to handle the
		// fields here.
		if resource.GetRequestSingleHeader(ctx, constant.HeaderBackendKey) == constant.AgentBackend {
			pipelines = []PipelineRelease{ConvertDocToMDRouterPipeline}

			// TODO jvallesm: we're relying in an API key set in the
			// configuration, which means all the requests are tied to a single
			// user. We should update the recipe to call the _internal_
			// services and pass the Instill User and Requester headers.
			input.Fields["api_url"] = structpb.NewStringValue(config.Config.APIGateway.URL)
			input.Fields["api_key"] = structpb.NewStringValue(config.Config.APIGateway.Token)

			getConvertResult = routedConvertResultParser

			break
		}

		// Custom pipelines only take the document input, but the default
		// (and historical) one needs the model to be set.
		//
		// NOTE: this means that we can't pass the default
		// ConvertDocToMDPipeline as a custom pipeline, we'd be missing the
		// VLM model setting. Validation when creating a catalog should
		// prevent this.
		pipelines = []PipelineRelease{ConvertDocToMDPipeline}
		input.Fields["vlm_model"] = structpb.NewStringValue("gpt-4o")

	default:
		return "", fmt.Errorf("unsupported file type: %v", fileType)
	}

	for _, pipeline := range pipelines {
		req := &pipelinepb.TriggerNamespacePipelineReleaseRequest{
			NamespaceId: pipeline.Namespace,
			PipelineId:  pipeline.ID,
			ReleaseId:   pipeline.Version,
			Inputs:      []*structpb.Struct{input},
		}

		resp, err := s.PipelinePub.TriggerNamespacePipelineRelease(ctx, req)
		if err != nil {
			return "", fmt.Errorf("triggering %s pipeline: %w", pipeline.ID, err)
		}

		result, err := getConvertResult(resp)
		if err != nil {
			return "", fmt.Errorf("getting conversion result: %w", err)
		}
		if result == "" {
			logger.Info("Conversion pipeline didn't yield results", zap.String("pipeline", pipeline.Name()))
			continue
		}

		// save the converting pipeline metadata into database
		if err := s.Repository.UpdateKbFileExtraMetaData(ctx, fileUID, "", pipeline.Name(), "", "", "", nil, nil, nil, nil, nil); err != nil {
			return "", fmt.Errorf("saving converting pipeline in file metadata: %w", err)
		}

		return result, nil
	}

	return "", fmt.Errorf("conversion pipelines didn't produce any result")
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

// simpleConvertResultParser extracts the conversion result from the pipeline
// response. It first checks for a non-empty "convert_result" field, then falls
// back to "convert_result2".
// Returns an error if neither field contains valid data or if the response
// structure is invalid.
func simpleConvertResultParser(resp *pipelinepb.TriggerNamespacePipelineReleaseResponse) (string, error) {
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

	return "", nil
}

func joinPBListOfStrings(list *structpb.ListValue) string {
	values := list.GetValues()
	asStrings := make([]string, len(values))
	for i, v := range values {
		asStrings[i] = v.GetStringValue()
	}

	return strings.Join(asStrings, "")

}

// routedConvertResultParser extracts the conversion result from the
// parsing-router pipeline response. This pipeline returns the result in
// different fields depending on the chosen conversion method, which is also
// returned so the correct result can be selected.
// Returns an error if neither field contains valid data or if the response
// structure is invalid.
func routedConvertResultParser(resp *pipelinepb.TriggerNamespacePipelineReleaseResponse) (string, error) {
	if resp == nil || len(resp.Outputs) == 0 {
		return "", fmt.Errorf("response is nil or has no outputs")
	}
	fields := resp.Outputs[0].GetFields()
	if fields == nil {
		return "", fmt.Errorf("fields in the output are nil")
	}

	parsingStrategy := fields["parsing-strategy"]

	switch parsingStrategy.GetStringValue() {
	case "Standard Document Operator":
		return joinPBListOfStrings(fields["heuristic"].GetListValue()), nil
	case "Docling Model":
		doclingOutput := fields["docling"].GetStructValue().GetFields()

		// Not used at the moment:
		// extractedImages := fields["extracted_images"]
		// pagesWithImages := fields["pages_with_images"]
		mdPages := doclingOutput["markdown_pages"].GetListValue()

		return joinPBListOfStrings(mdPages), nil
	case "Visual Language Model Pipeline":
		if result := fields["vlm-ocr"].GetStringValue(); result != "" {
			return result, nil
		}

		if result := fields["vlm-refinement"].GetStringValue(); result != "" {
			return result, nil
		}

	default:
		return "", fmt.Errorf("unrecognized parsing strategy %s", parsingStrategy.GetStringValue())
	}

	return "", nil
}

type Chunk = struct {
	End    int
	Start  int
	Text   string
	Tokens int
}

// GenerateSummary triggers the generate summary pipeline, processes markdown/text, and deducts credits from the caller's account.
// It generate summary from content.
func (s *Service) GenerateSummary(ctx context.Context, content, fileType string) (string, error) {
	req := &pipelinepb.TriggerNamespacePipelineReleaseRequest{
		NamespaceId: GenerateSummaryPipeline.Namespace,
		PipelineId:  GenerateSummaryPipeline.ID,
		ReleaseId:   GenerateSummaryPipeline.Version,
		Data: []*pipelinepb.TriggerData{
			{Variable: &structpb.Struct{Fields: map[string]*structpb.Value{
				"file_type": structpb.NewStringValue(fileType),
				"context":   structpb.NewStringValue(content),
				"llm_model": structpb.NewStringValue("gpt-4o-mini"),
			}}}},
	}

	resp, err := s.PipelinePub.TriggerNamespacePipelineRelease(ctx, req)
	if err != nil {
		return "", fmt.Errorf("failed to trigger %s pipeline. err:%w", GenerateSummaryPipeline.ID, err)
	}
	result, err := getGenerateSummaryResult(resp)
	if err != nil {
		return "", err
	}

	return result, nil
}

// getGenerateSummaryResult extracts the conversion result from the pipeline response.
// It first checks for a non-empty "convert_result" field, then falls back to "convert_result2".
// Returns an error if neither field contains valid data or if the response structure is invalid.
func getGenerateSummaryResult(resp *pipelinepb.TriggerNamespacePipelineReleaseResponse) (string, error) {
	if resp == nil || len(resp.Outputs) == 0 {
		return "", fmt.Errorf("response is nil or has no outputs. resp: %v", resp)
	}
	fields := resp.Outputs[0].GetFields()
	if fields == nil {
		return "", fmt.Errorf("fields in the output are nil. resp: %v", resp)
	}
	convertResult, ok := fields["summary_from_long_text"]
	if ok && convertResult.GetStringValue() != "" {
		return convertResult.GetStringValue(), nil
	}
	convertResult2, ok2 := fields["summary_from_short_text"]
	if ok2 && convertResult2.GetStringValue() != "" {
		return convertResult2.GetStringValue(), nil
	}
	return "", fmt.Errorf("summary_from_short_text and summary_from_long_text not found in the output fields. resp: %v", resp)
}

// ChunkMarkdownPipe triggers the markdown splitting pipeline, processes the markdown text, and deducts credits from the caller's account.
// It sets up the necessary metadata, triggers the pipeline, and processes the response to return the non-empty chunks.
func (s *Service) ChunkMarkdownPipe(ctx context.Context, markdown string) ([]Chunk, error) {
	req := &pipelinepb.TriggerNamespacePipelineReleaseRequest{
		NamespaceId: ChunkMDPipeline.Namespace,
		PipelineId:  ChunkMDPipeline.ID,
		ReleaseId:   ChunkMDPipeline.Version,
		Inputs: []*structpb.Struct{
			{
				Fields: map[string]*structpb.Value{
					"md_input":         structpb.NewStringValue(markdown),
					"max_chunk_length": structpb.NewNumberValue(maxChunkLengthForMarkdownCatalog),
					"chunk_overlap":    structpb.NewNumberValue(chunkOverlapForMarkdowntCatalog),
				},
			},
		},
	}
	res, err := s.PipelinePub.TriggerNamespacePipelineRelease(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to trigger %s pipeline. err:%w", ChunkMDPipeline.ID, err)
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

// ChunkTextPipe splits the input text into chunks using the splitting pipeline and consumes the caller's credits.
// It sets up the necessary metadata, triggers the pipeline, and processes the response to return the non-empty chunks.
func (s *Service) ChunkTextPipe(ctx context.Context, text string) ([]Chunk, error) {
	req := &pipelinepb.TriggerNamespacePipelineReleaseRequest{
		NamespaceId: ChunkTextPipeline.Namespace,
		PipelineId:  ChunkTextPipeline.ID,
		ReleaseId:   ChunkTextPipeline.Version,

		Inputs: []*structpb.Struct{
			{
				Fields: map[string]*structpb.Value{
					"text_input":       structpb.NewStringValue(text),
					"max_chunk_length": structpb.NewNumberValue(maxChunkLengthForTextCatalog),
					"chunk_overlap":    structpb.NewNumberValue(chunkOverlapForTextCatalog),
				},
			},
		},
	}
	res, err := s.PipelinePub.TriggerNamespacePipelineRelease(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to trigger %s pipeline. err:%w", ChunkTextPipeline.ID, err)
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
func (s *Service) EmbeddingTextPipe(ctx context.Context, texts []string) ([][]float32, error) {
	ctx, ctxCancel := context.WithCancel(ctx)
	defer ctxCancel()
	const maxBatchSize = 32
	const maxConcurrentGoroutines = 5
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
				inputs := make([]*structpb.Struct, 0, len(batch))
				for _, text := range batch {
					inputs = append(inputs, &structpb.Struct{
						Fields: map[string]*structpb.Value{
							"chunk_input": structpb.NewStringValue(text),
						},
					})
				}

				req := &pipelinepb.TriggerNamespacePipelineReleaseRequest{
					NamespaceId: EmbedTextPipeline.Namespace,
					PipelineId:  EmbedTextPipeline.ID,
					ReleaseId:   EmbedTextPipeline.Version,
					Inputs:      inputs,
				}
				res, err := s.PipelinePub.TriggerNamespacePipelineRelease(ctx, req)
				if err != nil {
					errChan <- fmt.Errorf("failed to trigger %s pipeline. err:%w", EmbedTextPipeline.ID, err)
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
func (s *Service) QuestionAnsweringPipe(ctx context.Context, question string, simChunks []string) (string, error) {
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
					"user_question":   structpb.NewStringValue(question),
				},
			},
		},
	}
	res, err := s.PipelinePub.TriggerNamespacePipelineRelease(ctx, req)
	if err != nil {
		return "", fmt.Errorf("failed to trigger %s pipeline. err:%w", QAPipeline.ID, err)
	}
	reply := res.Outputs[0].GetFields()["assistant_reply"].GetStringValue()
	return reply, nil
}
