package service

import (
	"context"
	"fmt"
	"math"

	"github.com/gofrs/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/instill-ai/artifact-backend/pkg/logger"
	"github.com/instill-ai/artifact-backend/pkg/resource"
	mgmtPB "github.com/instill-ai/protogen-go/core/mgmt/v1beta"
	"go.uber.org/zap"
)

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

	var nsType resource.NamespaceType
	if nsRes.GetType().String() == mgmtPB.CheckNamespaceAdminResponse_NAMESPACE_ORGANIZATION.String() {
		nsType = resource.Organization
	} else if nsRes.GetType().String() == mgmtPB.CheckNamespaceAdminResponse_NAMESPACE_USER.String() {
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

// GetNamespaceTierByNsID returns the tier of the namespace given the namespace ID
func (s *Service) GetNamespaceTierByNsID(ctx context.Context, nsID string) (Tier, error) {
	ns, err := s.GetNamespaceByNsID(ctx, nsID)
	if err != nil {
		return "", fmt.Errorf("failed to get namespace: %w", err)
	}
	return s.GetNamespaceTier(ctx, ns)
}

func (s *Service) GetNamespaceTier(ctx context.Context, ns *resource.Namespace) (Tier, error) {
	log, _ := logger.GetZapLogger(ctx)
	switch ns.NsType {
	case resource.User:
		sub, err := s.MgmtPrv.GetUserSubscriptionAdmin(ctx, &mgmtPB.GetUserSubscriptionAdminRequest{
			UserId: ns.NsID,
		})
		if err != nil {
			// because CE does not have subscription, mgmt service will return Unimplemented error
			statusError, ok := status.FromError(err)
			if ok && statusError.Code() == codes.Unimplemented {
				// Handle the case where the method is not implemented on the server
				log.Warn("GetUserSubscriptionAdmin is not implemented. Assuming enterprise tier")
				return TierEntriprise, nil
			} else {
				// Handle other errors
				return "", fmt.Errorf("failed to get user subscription: %w", err)

			}
		}
		if sub.GetSubscription().Plan == mgmtPB.UserSubscription_PLAN_FREE {
			return TierFree, nil
		} else if sub.GetSubscription().Plan == mgmtPB.UserSubscription_PLAN_PRO {
			return TierPro, nil
		}
		return "", fmt.Errorf("unknown user subscription plan: %v", sub.GetSubscription().Plan)
	case resource.Organization:
		sub, err := s.MgmtPrv.GetOrganizationSubscriptionAdmin(ctx, &mgmtPB.GetOrganizationSubscriptionAdminRequest{
			OrganizationId: ns.NsID,
		})
		if err != nil {
			// because CE does not have subscription, mgmt service will return Unimplemented error
			statusError, ok := status.FromError(err)
			if ok && statusError.Code() == codes.Unimplemented {
				// handle the case where the method is not implemented on the server
				log.Warn("GetUserSubscriptionAdmin is not implemented. Assuming enterprise tier")
				return TierEntriprise, nil
			} else {
				// handle other errors
				return "", fmt.Errorf("failed to get organization subscription: %w", err)

			}
		}
		if sub.GetSubscription().Plan == mgmtPB.OrganizationSubscription_PLAN_FREE {
			return TierFree, nil
		} else if sub.GetSubscription().Plan == mgmtPB.OrganizationSubscription_PLAN_TEAM {
			return TierTeam, nil
		} else if sub.GetSubscription().Plan == mgmtPB.OrganizationSubscription_PLAN_ENTERPRISE {
			return TierEntriprise, nil
		}
		return "", fmt.Errorf("unknown organization subscription plan: %v", sub.GetSubscription().Plan)
	default:
		return "", fmt.Errorf("unknown namespace type: %v", ns.NsType)
	}
}

type Tier string

const (
	TierFree       Tier = "free"
	TierPro        Tier = "pro"
	TierTeam       Tier = "team"
	TierEntriprise Tier = "enterprise"
)

func (t Tier) String() string {
	return string(t)
}

func (t Tier) GetPrivateCatalogLimit() int {
	switch t {
	case TierFree:
		return 10
	case TierPro:
		return 50
	case TierTeam:
		// unlimited
		return math.MaxInt
	case TierEntriprise:
		// unlimited
		return math.MaxInt

	}
	return 0
}

const mb = 1024 * 1024
const gb = 1024 * 1024 * 1024

func (t Tier) GetFileStorageTotalQuota() (int, string) {
	switch t {
	case TierFree:
		// 50MB
		return 50 * mb, "50MB"
	case TierPro:
		// 500MB
		return 500 * mb, "500MB"
	case TierTeam:
		// 2GB
		return 2 * gb, "2GB"
	case TierEntriprise:
		// unlimited
		return math.MaxInt, "unlimited"
	}
	return 0, "0"
}

// GetMaxUploadFileSize returns the maximum file size allowed for the given tier
// all tier has the same max file size. 150mb
func (t Tier) GetMaxUploadFileSize() int {
	return 150 * mb
}
