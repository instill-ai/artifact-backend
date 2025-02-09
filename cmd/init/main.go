package main

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"time"

	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/instill-ai/artifact-backend/config"

	grpcclient "github.com/instill-ai/artifact-backend/pkg/client/grpc"
	service "github.com/instill-ai/artifact-backend/pkg/service"
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

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "Instill-Service", "instill")

	for _, presetPipeline := range service.PresetPipelinesList {
		id := presetPipeline.ID
		version := presetPipeline.Version

		descriptionContent, err := service.PresetPipelinesFS.ReadFile(filepath.Join("preset/pipelines", id, version, "description.md"))
		if err != nil {
			log.Printf("Failed to read description.md for %s/%s: %v", id, version, err)
			continue
		}
		descriptionContentString := string(descriptionContent)

		readmeContent, err := service.PresetPipelinesFS.ReadFile(filepath.Join("preset/pipelines", id, version, "readme.md"))
		if err != nil {
			log.Printf("Failed to read readme.md for %s/%s: %v", id, version, err)
			continue
		}
		readmeContentString := string(readmeContent)

		recipeContent, err := service.PresetPipelinesFS.ReadFile(filepath.Join("preset/pipelines", id, version, "recipe.yaml"))
		if err != nil {
			log.Printf("Failed to read pipeline.yaml for %s/%s: %v", id, version, err)
			continue
		}
		recipeContentString := string(recipeContent)

		p := &pipelinepb.Pipeline{
			Id:          id,
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

		_, err = pipelinePublicServiceClient.GetNamespacePipeline(ctx, &pipelinepb.GetNamespacePipelineRequest{
			NamespaceId: PresetNamespaceID,
			PipelineId:  id,
		})

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
