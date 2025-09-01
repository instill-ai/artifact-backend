package service

import (
	"context"
	"fmt"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"

	"github.com/instill-ai/artifact-backend/pkg/resource"

	pb "github.com/instill-ai/protogen-go/core/mgmt/v1beta"
	logx "github.com/instill-ai/x/log"
)

func (s *service) GetNamespaceByNsID(ctx context.Context, nsID string) (*resource.Namespace, error) {
	logger, _ := logx.GetZapLogger(ctx)
	nsRes, err := s.mgmtPrv.CheckNamespaceAdmin(ctx, &pb.CheckNamespaceAdminRequest{
		Id: nsID,
	},
	)
	if err != nil {
		logger.Error("failed to check namespace", zap.Error(err))
		return nil, fmt.Errorf("failed to check namespace: %w", err)
	}
	ownerUUID := nsRes.GetUid()
	ownerUUIDParsed := uuid.FromStringOrNil(ownerUUID)

	var nsType resource.NamespaceType
	if nsRes.GetType().String() == pb.CheckNamespaceAdminResponse_NAMESPACE_ORGANIZATION.String() {
		nsType = resource.Organization
	} else if nsRes.GetType().String() == pb.CheckNamespaceAdminResponse_NAMESPACE_USER.String() {
		nsType = resource.User
	} else {
		err := fmt.Errorf("unknown namespace type: %v", nsRes.GetType().String())
		return nil, fmt.Errorf("failed to check namespace: %w", err)
	}
	ns := resource.Namespace{
		NsUID:  ownerUUIDParsed,
		NsType: nsType,
		NsID:   nsID,
	}
	return &ns, nil
}
