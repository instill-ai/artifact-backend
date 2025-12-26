package service

import (
	"context"
	"fmt"
	"time"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"

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

// FetchUserByUID fetches a user by UID with caching
func (s *service) FetchUserByUID(ctx context.Context, uid string) (*pb.User, error) {
	if uid == "" {
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
		UserUid: uid,
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

// FetchOwnerByNamespace fetches the owner object for a namespace with caching
func (s *service) FetchOwnerByNamespace(ctx context.Context, ns *resource.Namespace) (*pb.Owner, error) {
	if ns == nil {
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
			UserUid: ns.NsUID.String(),
		})
		if err != nil {
			return nil, fmt.Errorf("LookUpUserAdmin error: %w", err)
		}
		owner = &pb.Owner{Owner: &pb.Owner_User{User: resp.GetUser()}}
	case resource.Organization:
		resp, err := s.mgmtPrv.LookUpOrganizationAdmin(ctx, &pb.LookUpOrganizationAdminRequest{
			OrganizationUid: ns.NsUID.String(),
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
