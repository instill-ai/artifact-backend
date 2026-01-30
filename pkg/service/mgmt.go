package service

import (
	"context"
	"fmt"
	"time"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/instill-ai/artifact-backend/pkg/resource"

	pb "github.com/instill-ai/protogen-go/mgmt/v1beta"
	errorsx "github.com/instill-ai/x/errors"
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
		if status.Code(err) == codes.NotFound {
			return nil, errorsx.ErrNotFound
		}
		return nil, fmt.Errorf("failed to check namespace: %w", err)
	}
	ownerUUID := nsRes.GetUid()
	ownerUUIDParsed := uuid.FromStringOrNil(ownerUUID)

	// Log warning if namespace UID is empty - this indicates mgmt-backend
	// returned a response without a valid UID
	if ownerUUID == "" {
		logger.Warn("CheckNamespaceAdmin returned empty UID",
			zap.String("namespace_id", nsID),
			zap.String("namespace_type", nsRes.GetType().String()))
	}

	var nsType resource.NamespaceType
	if nsRes.GetType() == pb.CheckNamespaceAdminResponse_NAMESPACE_ORGANIZATION {
		nsType = resource.Organization
	} else if nsRes.GetType() == pb.CheckNamespaceAdminResponse_NAMESPACE_USER {
		nsType = resource.User
	} else {
		return nil, errorsx.ErrNotFound
	}
	ns := resource.Namespace{
		NsUID:  ownerUUIDParsed,
		NsType: nsType,
		NsID:   nsID,
	}
	return &ns, nil
}

// FetchUserByUID fetches a user by UID with caching
func (s *service) FetchUserByUID(ctx context.Context, uid string) (*pb.User, error) {
	// Skip fetching if uid is empty or a nil UUID string
	if uid == "" || uid == uuid.Nil.String() {
		return nil, nil
	}

	key := fmt.Sprintf("user_profile:%s", uid)
	if b, err := s.redisClient.Get(ctx, key).Bytes(); err == nil {
		user := &pb.User{}
		if protojson.Unmarshal(b, user) == nil {
			return user, nil
		}
	}

	resp, err := s.mgmtPrv.LookUpUserAdmin(ctx, &pb.LookUpUserAdminRequest{
		Permalink: fmt.Sprintf("users/%s", uid),
	})
	if err != nil {
		return nil, fmt.Errorf("LookUpUserAdmin error: %w", err)
	}

	user := resp.GetUser()
	if b, err := protojson.Marshal(user); err == nil {
		s.redisClient.Set(ctx, key, b, 5*time.Minute)
	}
	return user, nil
}

// FetchUserByID fetches a user by ID (username) with caching
func (s *service) FetchUserByID(ctx context.Context, userID string) (*pb.User, error) {
	if userID == "" {
		return nil, nil
	}

	key := fmt.Sprintf("user_profile_by_id:%s", userID)
	if b, err := s.redisClient.Get(ctx, key).Bytes(); err == nil {
		user := &pb.User{}
		if protojson.Unmarshal(b, user) == nil {
			return user, nil
		}
	}

	resp, err := s.mgmtPrv.GetUserAdmin(ctx, &pb.GetUserAdminRequest{
		Name: fmt.Sprintf("users/%s", userID),
	})
	if err != nil {
		return nil, fmt.Errorf("GetUserAdmin error: %w", err)
	}

	user := resp.GetUser()
	if b, err := protojson.Marshal(user); err == nil {
		s.redisClient.Set(ctx, key, b, 5*time.Minute)
	}
	return user, nil
}

// FetchOwnerByNamespace fetches the owner object for a namespace with caching
func (s *service) FetchOwnerByNamespace(ctx context.Context, ns *resource.Namespace) (*pb.Owner, error) {
	if ns == nil {
		return nil, nil
	}

	// Skip fetching if namespace UID is nil/zero - this can happen when
	// CheckNamespaceAdmin doesn't return a valid UID
	nilUUID := uuid.UUID{}
	if ns.NsUID == nilUUID {
		return nil, nil
	}

	key := fmt.Sprintf("owner_profile:%s", ns.NsUID.String())
	if b, err := s.redisClient.Get(ctx, key).Bytes(); err == nil {
		owner := &pb.Owner{}
		if protojson.Unmarshal(b, owner) == nil {
			return owner, nil
		}
	}

	var owner *pb.Owner
	switch ns.NsType {
	case resource.User:
		resp, err := s.mgmtPrv.LookUpUserAdmin(ctx, &pb.LookUpUserAdminRequest{
			Permalink: fmt.Sprintf("users/%s", ns.NsUID.String()),
		})
		if err != nil {
			return nil, fmt.Errorf("LookUpUserAdmin error: %w", err)
		}
		owner = &pb.Owner{Owner: &pb.Owner_User{User: resp.GetUser()}}
	case resource.Organization:
		// Use full resource name: organizations/{organization}
		resp, err := s.mgmtPrv.LookUpOrganizationAdmin(ctx, &pb.LookUpOrganizationAdminRequest{
			Name: fmt.Sprintf("organizations/%s", ns.NsUID.String()),
		})
		if err != nil {
			return nil, fmt.Errorf("LookUpOrganizationAdmin error: %w", err)
		}
		owner = &pb.Owner{Owner: &pb.Owner_Organization{Organization: resp.GetOrganization()}}
	}

	if owner != nil {
		if b, err := protojson.Marshal(owner); err == nil {
			s.redisClient.Set(ctx, key, b, 5*time.Minute)
		}
	}
	return owner, nil
}
