package handler

import (
	"context"
	"fmt"

	"google.golang.org/grpc/metadata"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

func (ph *PublicHandler) CreateKnowledgeBase(ctx context.Context, req *artifactpb.CreateKnowledgeBaseRequest) (*artifactpb.CreateKnowledgeBaseResponse, error) {
	fmt.Println("CreateKnowledgeBase")
	return nil, nil
}
func (ph *PublicHandler) GetKnowledgeBases(ctx context.Context, req *artifactpb.GetKnowledgeBasesRequest) (*artifactpb.GetKnowledgeBasesResponse, error) {
	fmt.Println("GetKnowledgeBases")
	return nil, nil
}
func (ph *PublicHandler) UpdateKnowledgeBase(ctx context.Context, req *artifactpb.UpdateKnowledgeBaseRequest) (*artifactpb.UpdateKnowledgeBaseResponse, error) {
	fmt.Println("UpdateKnowledgeBase")
	return nil, nil
}
func (ph *PublicHandler) DeleteKnowledgeBase(ctx context.Context, req *artifactpb.DeleteKnowledgeBaseRequest) (*artifactpb.DeleteKnowledgeBaseResponse, error) {
	// get header
	md, _ := metadata.FromIncomingContext(ctx)
	fmt.Println(md)

	fmt.Println("DeleteKnowledgeBase")
	return nil, nil
}
