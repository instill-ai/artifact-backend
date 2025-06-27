package service

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/instill-ai/artifact-backend/pkg/acl"
	"github.com/instill-ai/artifact-backend/pkg/customerror"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/resource"
	"github.com/instill-ai/x/log"
)

func (s *Service) CheckCatalogUserPermission(ctx context.Context, nsID, catalogID, authUID string) (*resource.Namespace, *repository.KnowledgeBase, error) {
	log, _ := log.GetZapLogger(ctx)
	// ACL - check user's permission to create conversation in the namespace
	ns, err := s.GetNamespaceByNsID(ctx, nsID)
	if err != nil {
		log.Error(
			"failed to get namespace",
			zap.Error(err),
			zap.String("namespace_id", nsID),
			zap.String("auth_uid", authUID),
		)
		return nil, nil, fmt.Errorf("failed to get namespace: %w", err)
	}

	// Check if the catalog exists
	catalog, err := s.Repository.GetKnowledgeBaseByOwnerAndKbID(ctx, ns.NsUID, catalogID)
	if err != nil {
		log.Error("failed to get catalog", zap.Error(err))
		return nil, nil, fmt.Errorf("failed to get catalog: %w", err)
	}
	granted, err := s.ACLClient.CheckPermission(ctx, acl.CatalogObject, catalog.UID, "writer")
	if err != nil {
		log.Error("failed to check permission", zap.Error(err))

		return nil, nil, fmt.Errorf("failed to check permission. err: %w", err)
	}
	if !granted {
		return nil, nil, fmt.Errorf("no permission. err: %w", customerror.ErrNoPermission)
	}

	return ns, catalog, nil
}

func (s *Service) GetNamespaceAndCheckPermission(ctx context.Context, nsID string) (*resource.Namespace, error) {
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
