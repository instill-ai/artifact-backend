package pipeline

import (
	"context"
	"embed"
	"fmt"
	"path/filepath"
	"strings"

	"golang.org/x/mod/semver"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	pipelinepb "github.com/instill-ai/protogen-go/pipeline/v1beta"
)

// displayNameToSlug converts a display name like "Indexing Convert File Type" to
// a slug like "indexing-convert-file-type"
func displayNameToSlug(displayName string) string {
	return strings.ToLower(strings.ReplaceAll(displayName, " ", "-"))
}

//go:embed preset/pipelines/*

// PresetPipelinesFS is the filesystem that contains the preset pipelines to release.
var PresetPipelinesFS embed.FS

// DefaultNamespaceID is the namespace that owns the preset pipelines.
const DefaultNamespaceID = "preset"

// Release identifies a pipeline used in knowledge base file processing.
// DisplayName is the human-readable name (e.g., "Indexing Convert File Type").
// Slug is derived from DisplayName (e.g., "indexing-convert-file-type").
// ID is system-assigned (e.g., "pip-abc123").
type Release struct {
	Namespace   string
	DisplayName string
	Version     string
}

// Slug returns the URL-friendly identifier derived from DisplayName.
func (pr Release) Slug() string {
	return displayNameToSlug(pr.DisplayName)
}

// Name returns a human-readable, unique identifier for a pipeline release.
// Format: {namespace}/{slug}@{version}
func (pr Release) Name() string {
	return pr.Namespace + "/" + pr.Slug() + "@" + pr.Version
}

// Releases is defined to implement common methods over pipeline
// release lists.
type Releases []Release

// ReleaseFromName parses a Release from its name, with the
// format {namespace}/{slug}@{version}.
func ReleaseFromName(name string) (Release, error) {
	pr := Release{}

	parts := strings.Split(name, "/")
	if len(parts) != 2 {
		return pr, fmt.Errorf("name must have the format {namespace}/{slug}@{version}")
	}
	pr.Namespace = parts[0]

	slugVersion := strings.Split(parts[1], "@")
	if len(slugVersion) != 2 {
		return pr, fmt.Errorf("name must have the format {namespace}/{slug}@{version}")
	}

	// Convert slug back to display name (title case)
	slug := slugVersion[0]
	words := strings.Split(slug, "-")
	for i, word := range words {
		if len(word) > 0 {
			words[i] = strings.ToUpper(word[:1]) + word[1:]
		}
	}
	pr.DisplayName = strings.Join(words, " ")
	pr.Version = slugVersion[1]

	if !semver.IsValid(pr.Version) {
		return pr, fmt.Errorf("version must be valid SemVer 2.0.0")
	}

	return pr, nil
}

var (
	// ConvertFileTypePipeline converts various file formats to AI-supported types
	// before AI content understanding. It handles format transformations like:
	// - PPTX/PPT → PDF (for document conversion)
	// - HEIC → JPEG (for image processing)
	// - AVIF → PNG (for web image formats)
	// - Additional conversions for audio, video, and other formats
	ConvertFileTypePipeline = Release{
		Namespace:   DefaultNamespaceID,
		DisplayName: "Indexing Convert File Type",
		Version:     "v1.0.0",
	}

	// GenerateContentPipeline converts documents to markdown using OpenAI GPT-4o
	// VLM for OCR and content extraction. Used for OpenAI model family.
	GenerateContentPipeline = Release{
		Namespace:   DefaultNamespaceID,
		DisplayName: "Indexing Generate Content",
		Version:     "v1.4.0",
	}

	// GenerateSummaryPipeline generates file summaries using OpenAI text models
	// with a map-reduce pattern. Used for OpenAI model family.
	GenerateSummaryPipeline = Release{
		Namespace:   DefaultNamespaceID,
		DisplayName: "Indexing Generate Summary",
		Version:     "v1.0.0",
	}

	// EmbedPipeline generates embeddings using OpenAI text-embedding models.
	// Used for OpenAI model family.
	EmbedPipeline = Release{
		Namespace:   DefaultNamespaceID,
		DisplayName: "Indexing Embed",
		Version:     "v1.0.0",
	}

	// PresetPipelinesList contains the preset pipelines used in knowledge bases.
	PresetPipelinesList = Releases{
		ConvertFileTypePipeline,
		GenerateContentPipeline,
		GenerateSummaryPipeline,
		EmbedPipeline,
	}

	// DefaultConversionPipelines contains the default pipeline used for document conversion.
	DefaultConversionPipelines = Releases{GenerateContentPipeline}
)

// ReleaseUpserter is used to upsert predefined pipeline releases into
// the database.
type ReleaseUpserter struct {
	FS                          embed.FS
	PipelinePublicServiceClient pipelinepb.PipelinePublicServiceClient
}

// Upsert creates or updates a pipeline release according to the information
// (readme, description and recipe) from the filesystem.
func (u *ReleaseUpserter) Upsert(ctx context.Context, pr Release) error {
	// The embedded filesystem uses slug-based paths (e.g., "preset/pipelines/indexing-convert-file-type/v1.0.0")
	slug := pr.Slug()
	basePath := filepath.Join("preset", "pipelines", slug, pr.Version)
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

	// NOTE: Do NOT set Slug field - it's OUTPUT_ONLY and server-generated from DisplayName
	p := &pipelinepb.Pipeline{
		DisplayName: pr.DisplayName,
		Description: &descriptionContentString,
		Readme:      readmeContentString,
		RawRecipe:   recipeContentString,
		Visibility:  pipelinepb.Pipeline_VISIBILITY_PUBLIC,
		Sharing: &pipelinepb.Sharing{
			Users: map[string]*pipelinepb.Sharing_User{
				"*/*": {
					Role:    pipelinepb.Role_ROLE_EXECUTOR,
					Enabled: true,
				},
			},
		},
	}

	// Try to get existing pipeline by slug
	var pipelineID string
	pipelineName := fmt.Sprintf("namespaces/%s/pipelines/%s", pr.Namespace, slug)
	getResp, err := u.PipelinePublicServiceClient.GetNamespacePipeline(ctx, &pipelinepb.GetNamespacePipelineRequest{
		Name: pipelineName, // Look up by AIP-compliant name
	})

	if err != nil {
		// Pipeline doesn't exist, create it
		parentName := fmt.Sprintf("namespaces/%s", pr.Namespace)
		createResp, err := u.PipelinePublicServiceClient.CreateNamespacePipeline(ctx, &pipelinepb.CreateNamespacePipelineRequest{
			Parent:   parentName,
			Pipeline: p,
		})
		if err != nil {
			return fmt.Errorf("creating namespace pipeline: %w", err)
		}
		// Use the server-generated ID for subsequent operations
		pipelineID = createResp.Pipeline.Id
	} else {
		// Pipeline exists, update it using the existing ID
		pipelineID = getResp.Pipeline.Id
		// Set the pipeline name for update (AIP-134 requires name in resource)
		p.Name = fmt.Sprintf("namespaces/%s/pipelines/%s", pr.Namespace, pipelineID)
		_, err = u.PipelinePublicServiceClient.UpdateNamespacePipeline(ctx, &pipelinepb.UpdateNamespacePipelineRequest{
			Pipeline:   p,
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"description", "readme", "sharing", "raw_recipe"}},
		})
		if err != nil {
			return fmt.Errorf("updating namespace pipeline: %w", err)
		}
	}

	// Slug format for version lookup (e.g., v1.0.0 -> v1.0.0)
	// Note: slug is OUTPUT_ONLY (server-generated from display_name)
	expectedSlug := strings.ToLower(pr.Version)

	r := &pipelinepb.PipelineRelease{
		Id:          pr.Version, // IMMUTABLE: semantic version vX.Y.Z
		DisplayName: pr.Version,
		Readme:      readmeContentString,
		Description: &descriptionContentString,
		RawRecipe:   recipeContentString,
	}

	// Look up existing releases by listing and filtering by display_name
	// since the ID is auto-generated (rel-xxx format)
	pipelineParent := fmt.Sprintf("namespaces/%s/pipelines/%s", pr.Namespace, pipelineID)
	listResp, err := u.PipelinePublicServiceClient.ListNamespacePipelineReleases(ctx, &pipelinepb.ListNamespacePipelineReleasesRequest{
		Parent: pipelineParent,
	})
	if err != nil {
		return fmt.Errorf("listing pipeline releases: %w", err)
	}

	// Find existing release by slug (URL-friendly version)
	var existingRelease *pipelinepb.PipelineRelease
	for _, rel := range listResp.GetReleases() {
		if rel.GetSlug() == expectedSlug {
			existingRelease = rel
			break
		}
	}

	if existingRelease == nil {
		// Create new release
		_, err = u.PipelinePublicServiceClient.CreateNamespacePipelineRelease(ctx, &pipelinepb.CreateNamespacePipelineReleaseRequest{
			Parent:  pipelineParent,
			Release: r,
		})
		if err != nil {
			return fmt.Errorf("creating pipeline release: %w", err)
		}
	} else {
		// Update existing release (AIP-134 requires name in resource)
		r.Name = existingRelease.GetName()
		_, err = u.PipelinePublicServiceClient.UpdateNamespacePipelineRelease(ctx, &pipelinepb.UpdateNamespacePipelineReleaseRequest{
			Release:    r,
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"raw_recipe", "readme", "description"}},
		})
		if err != nil {
			return fmt.Errorf("updating pipeline release: %w", err)
		}
	}

	return nil
}
