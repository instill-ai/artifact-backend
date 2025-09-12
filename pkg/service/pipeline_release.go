package service

import (
	"context"
	"embed"
	"fmt"
	"path/filepath"
	"strings"

	"golang.org/x/mod/semver"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	pipelinepb "github.com/instill-ai/protogen-go/pipeline/pipeline/v1beta"
)

//go:embed preset/pipelines/*
var PresetPipelinesFS embed.FS

// DefaultNamespaceID is the namespace that owns the preset pipelines.
const DefaultNamespaceID = "preset"

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

// PipelineReleases is defined to implement common methods over pipeline
// release lists.
type PipelineReleases []PipelineRelease

func (prs PipelineReleases) Names() []string {
	names := make([]string, len(prs))
	for i, pr := range prs {
		names[i] = pr.Name()
	}

	return names

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
	// ConvertDocToMDPipeline is the default conversion pipeline for documents.
	// Note: this pipeline is for the new indexing pipeline having
	// convert_result or convert_result2
	ConvertDocToMDPipeline = PipelineRelease{
		Namespace: "jota",
		ID:        "deterministic-conv-1",
		Version:   "v1.0.0",
	}

	// ConvertDocToMDStandardPipeline is the default conversion pipeline for
	// non-document files (e.g. CSV).
	ConvertDocToMDStandardPipeline = PipelineRelease{
		Namespace: DefaultNamespaceID,
		ID:        "indexing-convert-pdf",
		Version:   "v1.1.1",
	}

	// GenerateSummaryPipeline is the default pipeline for summarizing text.
	GenerateSummaryPipeline = PipelineRelease{
		Namespace: DefaultNamespaceID,
		ID:        "indexing-generate-summary",
		Version:   "v1.0.0",
	}

	// ChunkMDPipeline is the default pipeline for chunking Markdown.
	ChunkMDPipeline = PipelineRelease{
		Namespace: DefaultNamespaceID,
		ID:        "indexing-split-markdown",
		Version:   "v2.0.0",
	}

	// ChunkTextPipeline is the default pipeline for chunking text.
	ChunkTextPipeline = PipelineRelease{
		Namespace: DefaultNamespaceID,
		ID:        "indexing-split-text",
		Version:   "v2.0.0",
	}

	// EmbedTextPipeline is the default pipeline for embedding text.
	EmbedTextPipeline = PipelineRelease{
		Namespace: DefaultNamespaceID,
		ID:        "indexing-embed",
		Version:   "v1.1.0",
	}

	// QAPipeline is the default pipeline for question & answering.
	QAPipeline = PipelineRelease{
		Namespace: DefaultNamespaceID,
		ID:        "retrieving-qna",
		Version:   "v1.2.0",
	}

	// PresetPipelinesList contains the preset pipelines used in catalogs.
	PresetPipelinesList = PipelineReleases{
		ConvertDocToMDPipeline,
		ConvertDocToMDStandardPipeline,
		GenerateSummaryPipeline,
		ChunkMDPipeline,
		ChunkTextPipeline,
		EmbedTextPipeline,
		QAPipeline,
	}

	// DefaultConversionPipelines is the chain of pipelines used for converting
	// documents to Markdown.
	DefaultConversionPipelines = PipelineReleases{ConvertDocToMDPipeline}
)

// PipelineReleaseUpserter is used to upsert predefined pipeline releases into
// the database.
type PipelineReleaseUpserter struct {
	FS                          embed.FS
	PipelinePublicServiceClient pipelinepb.PipelinePublicServiceClient
}

// Upsert creates or updates a pipeline release according to the information
// (readme, description and recipe) from the filesystem.
func (u *PipelineReleaseUpserter) Upsert(ctx context.Context, pr PipelineRelease) error {
	basePath := filepath.Join(pr.Namespace, "pipelines", pr.ID, pr.Version)
	descriptionContent, err := u.FS.ReadFile(filepath.Join(basePath, "description.md"))
	if err != nil {
		return fmt.Errorf("reading description.md: %w", err)
	}

	readmeContent, err := u.FS.ReadFile(filepath.Join(basePath, "readme.md"))
	if err != nil {
		return fmt.Errorf("reading readme.md: %w", err)
	}

	recipeContent, err := u.FS.ReadFile(filepath.Join(basePath, "recipe.yaml"))
	if err != nil {
		return fmt.Errorf("reading recipe.md: %w", err)
	}

	descriptionContentString := string(descriptionContent)
	readmeContentString := string(readmeContent)
	recipeContentString := string(recipeContent)

	p := &pipelinepb.Pipeline{
		Id:          pr.ID,
		Description: &descriptionContentString,
		Readme:      readmeContentString,
		RawRecipe:   recipeContentString,
		Sharing: &pipelinepb.Sharing{
			Users: map[string]*pipelinepb.Sharing_User{
				"*/*": {
					Role:    pipelinepb.Role_ROLE_EXECUTOR,
					Enabled: true,
				},
			},
		},
	}

	_, err = u.PipelinePublicServiceClient.GetNamespacePipeline(ctx, &pipelinepb.GetNamespacePipelineRequest{
		NamespaceId: pr.Namespace,
		PipelineId:  pr.ID,
	})

	if err != nil {
		_, err = u.PipelinePublicServiceClient.CreateNamespacePipeline(ctx, &pipelinepb.CreateNamespacePipelineRequest{
			NamespaceId: pr.Namespace,
			Pipeline:    p,
		})
		if err != nil {
			return fmt.Errorf("creating namespace pipeline: %w", err)
		}
	} else {
		_, err = u.PipelinePublicServiceClient.UpdateNamespacePipeline(ctx, &pipelinepb.UpdateNamespacePipelineRequest{
			NamespaceId: pr.Namespace,
			PipelineId:  pr.ID,
			Pipeline:    p,
			UpdateMask:  &fieldmaskpb.FieldMask{Paths: []string{"description", "readme", "sharing", "raw_recipe"}},
		})
		if err != nil {
			return fmt.Errorf("updating namespace pipeline: %w", err)
		}
	}

	r := &pipelinepb.PipelineRelease{
		Id:          pr.Version,
		Readme:      readmeContentString,
		Description: &descriptionContentString,
		RawRecipe:   recipeContentString,
	}

	_, err = u.PipelinePublicServiceClient.GetNamespacePipelineRelease(ctx, &pipelinepb.GetNamespacePipelineReleaseRequest{
		NamespaceId: pr.Namespace,
		PipelineId:  pr.ID,
		ReleaseId:   pr.Version,
	})
	if err != nil {
		_, err = u.PipelinePublicServiceClient.CreateNamespacePipelineRelease(ctx, &pipelinepb.CreateNamespacePipelineReleaseRequest{
			NamespaceId: pr.Namespace,
			PipelineId:  pr.ID,
			Release:     r,
		})
		if err != nil {
			return fmt.Errorf("creating pipeline release: %w", err)
		}
	} else {
		_, err = u.PipelinePublicServiceClient.UpdateNamespacePipelineRelease(ctx, &pipelinepb.UpdateNamespacePipelineReleaseRequest{
			NamespaceId: pr.Namespace,
			PipelineId:  pr.ID,
			ReleaseId:   pr.Version,
			Release:     r,
			UpdateMask:  &fieldmaskpb.FieldMask{Paths: []string{"raw_recipe", "readme", "description"}},
		})
		if err != nil {
			return fmt.Errorf("updating pipeline release: %w", err)
		}
	}

	return nil
}
