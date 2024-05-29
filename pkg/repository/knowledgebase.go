package repository

import (
	"context"

	"github.com/instill-ai/artifact-backend/pkg/service"
)

func (r *Repository) CreateKnowledgeBase(ctx context.Context, kb service.KnowledgeBase) (*service.KnowledgeBase, error) {
	// TODO: Implement GetKnowledgeBase method
	return nil, nil
}

func (r *Repository) GetKnowledgeBase(ctx context.Context) ([]service.KnowledgeBase, error) {
	// TODO: Implement GetKnowledgeBase method
	return nil, nil
}

func (r *Repository) UpdateKnowledgeBase(ctx context.Context, kb service.KnowledgeBase) (*service.KnowledgeBase, error) {
	// TODO: Implement UpdateKnowledgeBase method
	return nil, nil
}

func (r *Repository) DeleteKnowledgeBase(ctx context.Context, kb service.KnowledgeBase) error {
	// TODO: Implement DeleteKnowledgeBase method
	return nil
}
