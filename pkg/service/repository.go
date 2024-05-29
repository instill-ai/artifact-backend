package service

// import (
// 	"context"

// 	"github.com/instill-ai/artifact-backend/pkg/utils"
// 	pb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
// )

// // Repository implements the storage functions for Artifact.
// // type Repository interface {
// // 	GetRepositoryTag(context.Context, utils.RepositoryTagName) (*pb.RepositoryTag, error)
// // 	UpsertRepositoryTag(context.Context, *pb.RepositoryTag) (*pb.RepositoryTag, error)
// // 	// kb
// // 	CreateKnowledgeBase(ctx context.Context, kb KnowledgeBase) (*KnowledgeBase, error)
// // 	GetKnowledgeBase(ctx context.Context) ([]KnowledgeBase, error)
// // 	UpdateKnowledgeBase(ctx context.Context, kb KnowledgeBase) (*KnowledgeBase, error)
// // 	DeleteKnowledgeBase(ctx context.Context, kb KnowledgeBase) error
// // }

// // type KnowledgeBase struct {
// // 	Name        string   `json:"name"`
// // 	KbID        string   `json:"kb_id"`
// // 	Description string   `json:"description"`
// // 	Tags        []string `json:"tags"`
// // }
