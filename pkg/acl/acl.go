package acl

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gofrs/uuid"
	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/constant"
	"google.golang.org/grpc/status"

	"github.com/instill-ai/artifact-backend/pkg/resource"
	openfga "github.com/openfga/api/proto/openfga/v1"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
)

type ACLClient struct {
	writeClient          openfga.OpenFGAServiceClient
	readClient           openfga.OpenFGAServiceClient
	redisClient          *redis.Client
	authorizationModelID string
	storeID              string
}

type Mode string

const (
	ReadMode  Mode = "read"
	WriteMode Mode = "write"
)

type Relation struct {
	UID      uuid.UUID
	Relation string
}

func NewACLClient(wc openfga.OpenFGAServiceClient, rc openfga.OpenFGAServiceClient, redisClient *redis.Client) ACLClient {
	if rc == nil {
		rc = wc
	}
	storeResp, err := wc.ListStores(context.Background(), &openfga.ListStoresRequest{})
	if err != nil {
		panic(err)
	}
	if len(storeResp.Stores) == 0 {
		panic(">>No openfga store found<<")
	}
	storeID := storeResp.Stores[0].Id

	modelResp, err := wc.ReadAuthorizationModels(context.Background(), &openfga.ReadAuthorizationModelsRequest{
		StoreId: storeID,
	})
	if err != nil {
		panic(err)
	}

	if len(modelResp.AuthorizationModels) == 0 {
		panic(">>No openfga auth model found<<")
	}
	// Note: becasue the code always return the first model,
	// when we have updated the model, we need to update the code to get the
	// latest model. i.e. restart the service.
	modelID := modelResp.AuthorizationModels[0].Id

	return ACLClient{
		writeClient:          wc,
		readClient:           rc,
		redisClient:          redisClient,
		authorizationModelID: modelID,
		storeID:              storeID,
	}
}

func InitOpenFGAClient(ctx context.Context, host string, port int) (openfga.OpenFGAServiceClient, *grpc.ClientConn) {
	clientDialOpts := grpc.WithTransportCredentials(insecure.NewCredentials())

	clientConn, err := grpc.NewClient(
		fmt.Sprintf("%v:%v", host, port),
		clientDialOpts,
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(config.Config.Server.MaxDataSize*constant.MB),
			grpc.MaxCallSendMsgSize(config.Config.Server.MaxDataSize*constant.MB),
		),
	)
	if err != nil {
		panic(err)
	}

	return openfga.NewOpenFGAServiceClient(clientConn), clientConn
}

func (c *ACLClient) getClient(ctx context.Context, mode Mode) openfga.OpenFGAServiceClient {
	userUID := resource.GetRequestSingleHeader(ctx, constant.HeaderUserUIDKey)
	if c.redisClient == nil {
		return c.writeClient
	}
	if mode == WriteMode {
		// To solve the read-after-write inconsistency problem,
		// we will direct the user to read from the primary database for a certain time frame
		// to ensure that the data is synchronized from the primary DB to the replica DB.
		_ = c.redisClient.Set(ctx, fmt.Sprintf("db_pin_user:%s:openfga", userUID), time.Now(), time.Duration(config.Config.OpenFGA.Replica.ReplicationTimeFrame)*time.Second)
	}

	// If the user is pinned, we will use the primary database for querying.
	if !errors.Is(c.redisClient.Get(ctx, fmt.Sprintf("db_pin_user:%s:openfga", userUID)).Err(), redis.Nil) {
		return c.writeClient
	}
	if mode == ReadMode && c.readClient != nil {
		return c.readClient
	}
	return c.writeClient
}

func (c *ACLClient) SetOwner(ctx context.Context, objectType string, objectUID uuid.UUID, ownerType string, ownerUID uuid.UUID) error {
	var err error
	if ownerType == "users" {
		ownerType = "user"
	} else if ownerType == "organizations" {
		ownerType = "organization"
	} else {
		return fmt.Errorf("invalid owner type")
	}
	data, err := c.getClient(ctx, ReadMode).Read(ctx, &openfga.ReadRequest{
		StoreId: c.storeID,
		TupleKey: &openfga.ReadRequestTupleKey{
			User:     fmt.Sprintf("%s:%s", ownerType, ownerUID.String()),
			Relation: "owner",
			Object:   fmt.Sprintf("%s:%s", objectType, objectUID.String()),
		},
	})
	if err != nil {
		return err
	}
	if len(data.Tuples) > 0 {
		return nil
	}

	_, err = c.getClient(ctx, WriteMode).Write(ctx, &openfga.WriteRequest{
		StoreId:              c.storeID,
		AuthorizationModelId: c.authorizationModelID,
		Writes: &openfga.WriteRequestWrites{
			TupleKeys: []*openfga.TupleKey{
				{
					User:     fmt.Sprintf("%s:%s", ownerType, ownerUID.String()),
					Relation: "owner",
					Object:   fmt.Sprintf("%s:%s", objectType, objectUID.String()),
				},
			},
		},
	})
	if err != nil {
		return err
	}
	return nil
}

func (c *ACLClient) SetKnowledgeBasePermission(ctx context.Context, kbUID uuid.UUID, user, role string, enable bool) error {
	var err error
	_ = c.DeleteKnowledgeBasePermission(ctx, kbUID, user)

	if enable {
		_, err = c.getClient(ctx, WriteMode).Write(ctx, &openfga.WriteRequest{
			StoreId:              c.storeID,
			AuthorizationModelId: c.authorizationModelID,
			Writes: &openfga.WriteRequestWrites{
				TupleKeys: []*openfga.TupleKey{
					{
						User:     user,
						Relation: role,
						Object:   fmt.Sprintf("knowledgebase:%s", kbUID.String()),
					},
				},
			},
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *ACLClient) DeleteKnowledgeBasePermission(ctx context.Context, kbUID uuid.UUID, user string) error {

	for _, role := range []string{"admin", "writer", "executor", "reader"} {
		_, _ = c.getClient(ctx, WriteMode).Write(ctx, &openfga.WriteRequest{
			StoreId:              c.storeID,
			AuthorizationModelId: c.authorizationModelID,
			Deletes: &openfga.WriteRequestDeletes{
				TupleKeys: []*openfga.TupleKeyWithoutCondition{
					{
						User:     user,
						Relation: role,
						Object:   fmt.Sprintf("knowledgebase:%s", kbUID.String()),
					},
				},
			},
		})
	}

	return nil
}

func (c *ACLClient) SetPublicKnowledgeBasePermission(ctx context.Context, kbUID uuid.UUID) error {
	for _, t := range []string{"user", "visitor"} {
		err := c.SetKnowledgeBasePermission(ctx, kbUID, fmt.Sprintf("%s:*", t), "reader", true)
		if err != nil {
			return err
		}
	}
	err := c.SetKnowledgeBasePermission(ctx, kbUID, "user:*", "executor", true)
	if err != nil {
		return err
	}

	return nil
}

func (c *ACLClient) DeletePublicKnowledgeBasePermission(ctx context.Context, kbUID uuid.UUID) error {
	for _, t := range []string{"user", "visitor"} {
		err := c.DeleteKnowledgeBasePermission(ctx, kbUID, fmt.Sprintf("%s:*", t))
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *ACLClient) Purge(ctx context.Context, objectType string, objectUID uuid.UUID) error {

	data, err := c.getClient(ctx, ReadMode).Read(ctx, &openfga.ReadRequest{
		StoreId: c.storeID,
		TupleKey: &openfga.ReadRequestTupleKey{
			Object: fmt.Sprintf("%s:%s", objectType, objectUID),
		},
	})
	if err != nil {
		return err
	}
	for _, data := range data.Tuples {
		_, err = c.getClient(ctx, WriteMode).Write(ctx, &openfga.WriteRequest{
			StoreId:              c.storeID,
			AuthorizationModelId: c.authorizationModelID,
			Deletes: &openfga.WriteRequestDeletes{
				TupleKeys: []*openfga.TupleKeyWithoutCondition{
					{
						User:     data.Key.User,
						Relation: data.Key.Relation,
						Object:   data.Key.Object,
					},
				},
			},
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *ACLClient) CheckPermission(ctx context.Context, objectType string, objectUID uuid.UUID, role string) (bool, error) {

	userType := resource.GetRequestSingleHeader(ctx, constant.HeaderAuthTypeKey)
	userUID := ""
	if userType == "user" {
		userUID = resource.GetRequestSingleHeader(ctx, constant.HeaderUserUIDKey)
	} else {
		userUID = resource.GetRequestSingleHeader(ctx, constant.HeaderVisitorUIDKey)
	}

	if userUID == "" {
		return false, fmt.Errorf("userUID is empty in check permission")
	}

	data, err := c.getClient(ctx, ReadMode).Check(ctx, &openfga.CheckRequest{
		StoreId:              c.storeID,
		AuthorizationModelId: c.authorizationModelID,
		TupleKey: &openfga.CheckRequestTupleKey{
			User:     fmt.Sprintf("%s:%s", userType, userUID),
			Relation: role,
			Object:   fmt.Sprintf("%s:%s", objectType, objectUID.String()),
		},
	})
	if err != nil {
		return false, err
	}

	return data.Allowed, nil
}

func (c *ACLClient) CheckPublicExecutable(ctx context.Context, objectType string, objectUID uuid.UUID) (bool, error) {

	data, err := c.getClient(ctx, ReadMode).Check(ctx, &openfga.CheckRequest{
		StoreId:              c.storeID,
		AuthorizationModelId: c.authorizationModelID,
		TupleKey: &openfga.CheckRequestTupleKey{
			User:     "user:*",
			Relation: "executor",
			Object:   fmt.Sprintf("%s:%s", objectType, objectUID.String()),
		},
	})
	if err != nil {
		return false, err
	}
	return data.Allowed, nil
}

func (c *ACLClient) ListPermissions(ctx context.Context, objectType string, role string, isPublic bool) ([]uuid.UUID, error) {

	userType := resource.GetRequestSingleHeader(ctx, constant.HeaderAuthTypeKey)
	userUIDStr := ""
	if userType == "user" {
		userUIDStr = resource.GetRequestSingleHeader(ctx, constant.HeaderUserUIDKey)

	} else {
		userUIDStr = resource.GetRequestSingleHeader(ctx, constant.HeaderVisitorUIDKey)
	}

	if isPublic {
		userUIDStr = "*"
	}

	listObjectsResult, err := c.getClient(ctx, ReadMode).ListObjects(ctx, &openfga.ListObjectsRequest{
		StoreId:              c.storeID,
		AuthorizationModelId: c.authorizationModelID,
		User:                 fmt.Sprintf("%s:%s", userType, userUIDStr),
		Relation:             role,
		Type:                 objectType,
	})
	//  TODO: handle error when no model is created
	if err != nil {
		if statusErr, ok := status.FromError(err); ok {
			if statusErr.Code() == codes.Code(openfga.ErrorCode_type_not_found) {
				return []uuid.UUID{}, nil
			}
		}
		return nil, err
	}

	objectUIDs := []uuid.UUID{}
	for _, object := range listObjectsResult.GetObjects() {
		objectUIDs = append(objectUIDs, uuid.FromStringOrNil(strings.Split(object, ":")[1]))
	}

	return objectUIDs, nil
}

// checkRequesterPermission validates that the authenticated user can make
// requests on behalf of the resource identified by the requester UID.
func (c *ACLClient) CheckRequesterPermission(ctx context.Context) error {
	authType := resource.GetRequestSingleHeader(ctx, constant.HeaderAuthTypeKey)
	if authType != "user" {
		// Only authenticated users can switch namespaces.
		return fmt.Errorf("unauthenticated user")
	}
	requester := resource.GetRequestSingleHeader(ctx, constant.HeaderRequesterUIDKey)
	authenticatedUser := resource.GetRequestSingleHeader(ctx, constant.HeaderUserUIDKey)
	if requester == "" || authenticatedUser == requester {
		// Request doesn't contain impersonation.
		return nil
	}

	// The only impersonation that's currently implemented is switching to an
	// organization namespace.
	isMember, err := c.CheckPermission(ctx, "organization", uuid.FromStringOrNil(requester), "member")
	if err != nil {
		return fmt.Errorf("checking organization membership: %w", err)
	}

	if !isMember {
		return fmt.Errorf("authenticated user doesn't belong to requester organization")
	}

	return nil
}
