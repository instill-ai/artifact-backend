package main

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"time"

	"github.com/launchdarkly/go-semver"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/instill-ai/artifact-backend/config"

	asset "github.com/instill-ai/artifact-backend/pkg/asset"
	grpcclient "github.com/instill-ai/artifact-backend/pkg/client/grpc"
	mgmtpb "github.com/instill-ai/protogen-go/core/mgmt/v1beta"
	pipelinepb "github.com/instill-ai/protogen-go/pipeline/pipeline/v1beta"
)

const PresetNamespaceID = "preset"

func main() {

	ctx := context.Background()

	config.Init()

	pipelinePublicGrpcConn, err := grpcclient.NewGRPCConn(
		fmt.Sprintf("%v:%v", config.Config.PipelineBackend.Host,
			config.Config.PipelineBackend.PublicPort),
		config.Config.PipelineBackend.HTTPS.Cert,
		config.Config.PipelineBackend.HTTPS.Key)
	if err != nil {
		log.Fatalf("failed to create pipeline public grpc client: %v", err)
	}
	defer pipelinePublicGrpcConn.Close()

	pipelinePublicServiceClient := pipelinepb.NewPipelinePublicServiceClient(pipelinePublicGrpcConn)

	mgmtPrivateGrpcConn, err := grpcclient.NewGRPCConn(
		fmt.Sprintf("%v:%v", config.Config.MgmtBackend.Host,
			config.Config.MgmtBackend.PrivatePort),
		config.Config.MgmtBackend.HTTPS.Cert,
		config.Config.MgmtBackend.HTTPS.Key)
	if err != nil {
		log.Fatalf("failed to create mgmt private grpc client: %v", err)
	}
	defer mgmtPrivateGrpcConn.Close()

	mgmtPrivateServiceClient := mgmtpb.NewMgmtPrivateServiceClient(mgmtPrivateGrpcConn)

	presetOrgResp, err := mgmtPrivateServiceClient.GetOrganizationAdmin(ctx, &mgmtpb.GetOrganizationAdminRequest{OrganizationId: PresetNamespaceID})
	if err != nil {
		log.Fatalf("Failed to get preset organization: %v", err)
	}

	orgUID := presetOrgResp.Organization.Uid
	fmt.Println(pipelinePublicServiceClient, orgUID)

	// Read all pipeline directories
	entries, err := asset.PresetPipelines.ReadDir("preset/pipelines")
	if err != nil {
		log.Fatalf("Failed to read pipeline directory: %v", err)
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "Instill-Service", "instill")

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		id := entry.Name()

		// Read version directories
		versionEntries, err := asset.PresetPipelines.ReadDir(filepath.Join("preset/pipelines", id))
		if err != nil {
			log.Printf("Failed to read versions for pipeline %s: %v", id, err)
			continue
		}

		// Get latest version's recipe
		var latestVersion semver.Version
		var latestRecipe string
		var latestDescription string
		var latestReadme string

		for _, versionEntry := range versionEntries {
			if !versionEntry.IsDir() {
				continue
			}
			v, err := semver.Parse(versionEntry.Name()[1:])
			if err != nil {
				log.Printf("Failed to parse version %s: %v", versionEntry.Name(), err)
				continue
			}
			if v.ComparePrecedence(latestVersion) == 1 {
				latestVersion = v

				descriptionContent, err := asset.PresetPipelines.ReadFile(filepath.Join("preset/pipelines", id, versionEntry.Name(), "description.md"))
				if err != nil {
					log.Printf("Failed to read description.md for %s/%s: %v", id, versionEntry.Name(), err)
					continue
				}
				latestDescription = string(descriptionContent)

				readmeContent, err := asset.PresetPipelines.ReadFile(filepath.Join("preset/pipelines", id, versionEntry.Name(), "readme.md"))
				if err != nil {
					log.Printf("Failed to read readme.md for %s/%s: %v", id, versionEntry.Name(), err)
					continue
				}
				latestReadme = string(readmeContent)

				recipeContent, err := asset.PresetPipelines.ReadFile(filepath.Join("preset/pipelines", id, versionEntry.Name(), "recipe.yaml"))
				if err != nil {
					log.Printf("Failed to read recipe.yaml for %s/%s: %v", id, versionEntry.Name(), err)
					continue
				}
				latestRecipe = string(recipeContent)

			}
		}

		_, err = pipelinePublicServiceClient.GetNamespacePipeline(ctx, &pipelinepb.GetNamespacePipelineRequest{
			NamespaceId: PresetNamespaceID,
			PipelineId:  id,
		})

		p := &pipelinepb.Pipeline{
			Id:          id,
			Description: &latestDescription,
			Readme:      latestReadme,
			RawRecipe:   latestRecipe,
			Sharing: &pipelinepb.Sharing{
				Users: map[string]*pipelinepb.Sharing_User{
					"*/*": {
						Role:    pipelinepb.Role_ROLE_EXECUTOR,
						Enabled: true,
					},
				},
			},
		}
		if err != nil {
			_, err = pipelinePublicServiceClient.CreateNamespacePipeline(ctx, &pipelinepb.CreateNamespacePipelineRequest{
				NamespaceId: PresetNamespaceID,
				Pipeline:    p,
			})
			if err != nil {
				log.Fatalf("Failed to create namespace pipeline %s: %v", id, err)
			}
		} else {
			_, err = pipelinePublicServiceClient.UpdateNamespacePipeline(ctx, &pipelinepb.UpdateNamespacePipelineRequest{
				NamespaceId: PresetNamespaceID,
				PipelineId:  id,
				Pipeline:    p,
				UpdateMask:  &fieldmaskpb.FieldMask{Paths: []string{"description", "readme", "sharing", "raw_recipe"}},
			})
			if err != nil {
				log.Fatalf("Failed to update namespace pipeline %s: %v", id, err)
			}
		}

		for _, versionEntry := range versionEntries {
			if !versionEntry.IsDir() {
				continue
			}

			descriptionContent, err := asset.PresetPipelines.ReadFile(filepath.Join("preset/pipelines", id, versionEntry.Name(), "description.md"))
			if err != nil {
				log.Printf("Failed to read description.md for %s/%s: %v", id, versionEntry.Name(), err)
				continue
			}
			descriptionContentString := string(descriptionContent)

			readmeContent, err := asset.PresetPipelines.ReadFile(filepath.Join("preset/pipelines", id, versionEntry.Name(), "readme.md"))
			if err != nil {
				log.Printf("Failed to read readme.md for %s/%s: %v", id, versionEntry.Name(), err)
				continue
			}
			readmeContentString := string(readmeContent)

			recipeContent, err := asset.PresetPipelines.ReadFile(filepath.Join("preset/pipelines", id, versionEntry.Name(), "recipe.yaml"))
			if err != nil {
				log.Printf("Failed to read pipeline.yaml for %s/%s: %v", id, versionEntry.Name(), err)
				continue
			}
			recipeContentString := string(recipeContent)
			version := versionEntry.Name()

			r := &pipelinepb.PipelineRelease{
				Id:          version,
				Readme:      readmeContentString,
				Description: &descriptionContentString,
				RawRecipe:   recipeContentString,
			}

			_, err = pipelinePublicServiceClient.GetNamespacePipelineRelease(ctx, &pipelinepb.GetNamespacePipelineReleaseRequest{
				NamespaceId: PresetNamespaceID,
				PipelineId:  id,
				ReleaseId:   version,
			})
			if err != nil {
				_, err = pipelinePublicServiceClient.CreateNamespacePipelineRelease(ctx, &pipelinepb.CreateNamespacePipelineReleaseRequest{
					NamespaceId: PresetNamespaceID,
					PipelineId:  id,
					Release:     r,
				})
				if err != nil {
					log.Printf("Failed to create namespace pipeline release %s/%s: %v", id, version, err)
				}
			} else {
				_, err = pipelinePublicServiceClient.UpdateNamespacePipelineRelease(ctx, &pipelinepb.UpdateNamespacePipelineReleaseRequest{
					NamespaceId: PresetNamespaceID,
					PipelineId:  id,
					ReleaseId:   version,
					Release:     r,
					UpdateMask:  &fieldmaskpb.FieldMask{Paths: []string{"raw_recipe", "readme", "description"}},
				})
				if err != nil {
					log.Printf("Failed to update namespace pipeline release %s/%s: %v", id, version, err)
				}

			}

			fmt.Printf("Processed pipeline: %s version: %s\n", id, version)
		}
	}
}
