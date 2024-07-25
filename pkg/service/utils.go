package service

import (
	"context"
	"fmt"

	"github.com/gofrs/uuid"
	"github.com/instill-ai/artifact-backend/pkg/constant"
	"github.com/instill-ai/artifact-backend/pkg/logger"
	"github.com/instill-ai/artifact-backend/pkg/resource"
	mgmtPB "github.com/instill-ai/protogen-go/core/mgmt/v1beta"
	"go.uber.org/zap"
)

func (s *Service) CheckNamespacePermission(ctx context.Context, ns *resource.Namespace) error {
	// TODO: optimize ACL model
	if ns.NsType == "organizations" {
		granted, err := s.ACLClient.CheckPermission(ctx, "organization", ns.NsUID, "member")
		if err != nil {
			return err
		}
		if !granted {
			return ErrNoPermission
		}
	} else if ns.NsUID != uuid.FromStringOrNil(resource.GetRequestSingleHeader(ctx, constant.HeaderUserUIDKey)) {
		return ErrNoPermission
	}
	return nil
}

func (s *Service) GetNamespaceByNsID(ctx context.Context, nsID string) (*resource.Namespace, error) {
	log, _ := logger.GetZapLogger(ctx)
	nsRes, err := s.MgmtPrv.CheckNamespaceAdmin(ctx, &mgmtPB.CheckNamespaceAdminRequest{
		Id: nsID,
	},
	)
	if err != nil {
		log.Error("failed to check namespace", zap.Error(err))
		return nil, fmt.Errorf("failed to check namespace: %w", err)
	}
	ownerUUID := nsRes.GetUid()
	ownerUUIDParsed := uuid.FromStringOrNil(ownerUUID)

	var nsType string
	if nsRes.GetType().String() == mgmtPB.CheckNamespaceAdminResponse_NAMESPACE_ORGANIZATION.String() {
		nsType = "organizations"
	} else if nsRes.GetType().String() == mgmtPB.CheckNamespaceAdminResponse_NAMESPACE_USER.String() {
		nsType = "users"
	} else {
		err := fmt.Errorf("unknown namespace type: %v", nsRes.GetType().String())
		return nil, fmt.Errorf("failed to check namespace: %w", err)
	}
	ns := resource.Namespace{
		NsUID:  ownerUUIDParsed,
		NsType: resource.NamespaceType(nsType),
		NsID:   nsID,
	}
	return &ns, nil
}
