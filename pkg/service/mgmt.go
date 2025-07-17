package service

import (
	"context"
	"fmt"
	"math"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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

func (s *service) GetNamespaceTier(ctx context.Context, ns *resource.Namespace) (Tier, error) {
	logger, _ := logx.GetZapLogger(ctx)
	switch ns.NsType {
	case resource.User:
		sub, err := s.mgmtPrv.GetUserSubscriptionAdmin(ctx, &pb.GetUserSubscriptionAdminRequest{
			UserId: ns.NsID,
		})
		if err != nil {
			// because CE does not have subscription, mgmt service will return Unimplemented error
			// TODO this logic should be moved out of the CE repo.

			statusError, ok := status.FromError(err)
			if ok && statusError.Code() == codes.Unimplemented {
				// Handle the case where the method is not implemented on the server
				logger.Warn("GetUserSubscriptionAdmin is not implemented. Assuming enterprise tier")
				return TierEnterprise, nil
			}

			// Handle other errors
			return "", fmt.Errorf("failed to get user subscription: %w", err)
		}
		switch sub.GetSubscription().GetPlan() {
		case pb.UserSubscription_PLAN_STARTER:
			switch sub.GetSubscription().GetDetail().GetStatus() {
			case pb.StripeSubscriptionDetail_STATUS_TRIALING, pb.StripeSubscriptionDetail_STATUS_ACTIVE:
				return TierStarter, nil
			default:
				return TierFree, nil
			}
		}
	case resource.Organization:
		sub, err := s.mgmtPrv.GetOrganizationSubscriptionAdmin(ctx, &pb.GetOrganizationSubscriptionAdminRequest{
			OrganizationId: ns.NsID,
		})
		if err != nil {
			// because CE does not have subscription, mgmt service will return Unimplemented error
			// TODO this logic should be moved out of the CE repo.

			statusError, ok := status.FromError(err)
			if ok && statusError.Code() == codes.Unimplemented {
				// handle the case where the method is not implemented on the server
				logger.Warn("GetUserSubscriptionAdmin is not implemented. Assuming enterprise tier")
				return TierEnterprise, nil
			}

			// handle other errors
			return "", fmt.Errorf("failed to get organization subscription: %w", err)
		}

		switch sub.GetSubscription().GetPlan() {
		case pb.OrganizationSubscription_PLAN_ENTERPRISE:
			return TierEnterprise, nil
		case pb.OrganizationSubscription_PLAN_TEAM:
			switch sub.GetSubscription().GetDetail().GetStatus() {
			case pb.StripeSubscriptionDetail_STATUS_TRIALING, pb.StripeSubscriptionDetail_STATUS_ACTIVE:
				return TierTeam, nil
			default:
				return TierFree, nil
			}
		default:
			return TierFree, nil
		}
	}

	return "", fmt.Errorf("unknown namespace type: %v", ns.NsType)
}

// Tier defines the subscription plan of an owner.
type Tier string

const (
	// TierFree is a view-only mode for subscriptions that have been cancelled
	// or that have expired their free trial.
	TierFree Tier = "free"
	// TierStarter is the individual subscription.
	TierStarter Tier = "starter"
	// TierTeam is an organization subscription for small teams.
	TierTeam Tier = "team"
	// TierEnterprise is an organization subscription for large teams and
	// volumes.
	TierEnterprise Tier = "enterprise"
)

// String returns the string value of Tier.
func (t Tier) String() string {
	return string(t)
}

// GetPrivateCatalogLimit returns the max files in a catalog.
func (t Tier) GetPrivateCatalogLimit() int {
	switch t {
	case TierFree:
		return 10
	case TierStarter:
		return 50
	case TierTeam:
		// unlimited
		return math.MaxInt
	case TierEnterprise:
		// unlimited
		return math.MaxInt

	}
	return 0
}

const mb = 1024 * 1024
const gb = 1024 * 1024 * 1024

// GetFileStorageTotalQuota returns the storage quota, in bytes.
func (t Tier) GetFileStorageTotalQuota() (int, string) {
	switch t {
	case TierFree:
		// 50MB
		return 50 * mb, "50MB"
	case TierStarter:
		// 500MB
		return 500 * mb, "500MB"
	case TierTeam:
		// 2GB
		return 2 * gb, "2GB"
	case TierEnterprise:
		// unlimited
		return math.MaxInt, "unlimited"
	}
	return 0, "0"
}

// GetMaxUploadFileSize returns the maximum file size allowed for the given
// tier.
// All tiers have the same max file size: 512mb.
func (t Tier) GetMaxUploadFileSize() int {
	return 512 * mb
}
