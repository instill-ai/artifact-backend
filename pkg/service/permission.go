package service

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/instill-ai/artifact-backend/pkg/acl"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/resource"

	errorsx "github.com/instill-ai/x/errors"
	logx "github.com/instill-ai/x/log"
)

func (s *service) CheckCatalogUserPermission(ctx context.Context, nsID, knowledgeBaseID, authUID string) (*resource.Namespace, *repository.KnowledgeBaseModel, error) {
	logger, _ := logx.GetZapLogger(ctx)
	// ACL - check user's permission to create conversation in the namespace
	ns, err := s.GetNamespaceByNsID(ctx, nsID)
	if err != nil {
		logger.Error(
			"failed to get namespace",
			zap.Error(err),
			zap.String("namespace_id", nsID),
			zap.String("auth_uid", authUID),
		)
		return nil, nil, fmt.Errorf("failed to get namespace: %w", err)
	}

	// Check if the knowledge base exists
	kb, err := s.repository.GetKnowledgeBaseByOwnerAndKbID(ctx, ns.NsUID, knowledgeBaseID)
	if err != nil {
		logger.Error("failed to get knowledge base", zap.Error(err))
		return nil, nil, fmt.Errorf("failed to get knowledge base: %w", err)
	}
	granted, err := s.aclClient.CheckPermission(ctx, acl.CatalogObject, kb.UID, "writer")
	if err != nil {
		logger.Error("failed to check permission", zap.Error(err))

		return nil, nil, fmt.Errorf("failed to check permission. err: %w", err)
	}
	if !granted {
		return nil, nil, fmt.Errorf("no permission. err: %w", errorsx.ErrUnauthorized)
	}

	return ns, kb, nil
}

func (s *service) GetNamespaceAndCheckPermission(ctx context.Context, nsID string) (*resource.Namespace, error) {
	ns, err := s.GetNamespaceByNsID(ctx, nsID)
	if err != nil {
		return nil, err
	}
	err = s.CheckNamespacePermission(ctx, ns)
	if err != nil {
		return nil, err
	}
	return ns, nil
}
