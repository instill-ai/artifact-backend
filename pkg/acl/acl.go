package acl

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gofrs/uuid"

	"github.com/instill-ai/artifact-backend/pkg/types"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	openfga "github.com/openfga/api/proto/openfga/v1"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/constant"
	"github.com/instill-ai/x/resource"

	constantx "github.com/instill-ai/x/constant"
	errorsx "github.com/instill-ai/x/errors"
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
	UID      types.ObjectUIDType
	Relation string
}

const CatalogObject = "knowledgebase"

func NewACLClient(wc openfga.OpenFGAServiceClient, rc openfga.OpenFGAServiceClient, redisClient *redis.Client) *ACLClient {
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

	return &ACLClient{
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
	userUID := resource.GetRequestSingleHeader(ctx, constantx.HeaderUserUIDKey)
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

// SetOwner sets the owner of a given object. It first normalizes the ownerType to its singular form,
// then checks if the owner already exists in the database. If the owner does not exist, it writes the new owner to the database.
// Parameters:
// - ctx: context for managing request-scoped values, cancellation, and deadlines.
// - objectType: the type of the object (e.g., "pipeline", "_model", "knowledgebase").
// - objectUID: the unique identifier of the object (uses uuid.UUID for polymorphic object types).
// - ownerType: the type of the owner (e.g., "users", "organizations").
// - ownerUID: the unique identifier of the owner.
// Returns an error if the ownerType is invalid, if there is an error reading from or writing to the database, or nil if successful.
//
// Note: ACL functions use uuid.UUID instead of typed aliases for objectUID parameters
// because they are polymorphic and work across multiple object types (knowledgebase,
// organization, pipeline, model, etc.). The objectType parameter specifies the actual type.
func (c *ACLClient) SetOwner(ctx context.Context, objectType string, objectUID uuid.UUID, ownerType string, ownerUID types.OwnerUIDType) error {
	var err error
	// Normalize ownerType to singular form. because in our openfga, the
	// owner/organization type is singular.
	switch ownerType {
	case "users":
		ownerType = "user"
	case "organizations":
		ownerType = "organization"
	default:
		return fmt.Errorf("invalid owner type")
	}

	// Check if the owner already exists
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

	// Write the new owner
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

// Note: may move this to service layer due to it is specific to knowledge base
func (c *ACLClient) SetKnowledgeBasePermission(ctx context.Context, kbUID types.KBUIDType, user, role string, enable bool) error {
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

func (c *ACLClient) DeleteKnowledgeBasePermission(ctx context.Context, kbUID types.KBUIDType, user string) error {

	// delete all roles
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

// Note: may move this to service layer due to it is specific to knowledge base
func (c *ACLClient) SetPublicKnowledgeBasePermission(ctx context.Context, kbUID types.KBUIDType) error {
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

// Note: may move this to service layer due to it is specific to knowledge base
func (c *ACLClient) DeletePublicKnowledgeBasePermission(ctx context.Context, kbUID types.KBUIDType) error {
	for _, t := range []string{"user", "visitor"} {
		// delete user:* and visitor:* from all roles
		err := c.DeleteKnowledgeBasePermission(ctx, kbUID, fmt.Sprintf("%s:*", t))
		if err != nil {
			return err
		}
	}

	return nil
}

// Purge deletes all permissions associated with the specified object type and object UID.
// It reads all the tuples related to the object and then deletes each one.
func (c *ACLClient) Purge(ctx context.Context, objectType string, objectUID uuid.UUID) error {

	// Read all tuples related to the specified object
	data, err := c.getClient(ctx, ReadMode).Read(ctx, &openfga.ReadRequest{
		StoreId: c.storeID,
		TupleKey: &openfga.ReadRequestTupleKey{
			Object: fmt.Sprintf("%s:%s", objectType, objectUID),
		},
	})
	if err != nil {
		return err
	}

	// Iterate over each tuple and delete it
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

// CheckPermission verifies if a user or visitor has a specific role for a given object.
// It retrieves the user type and user UID from the request context headers, constructs a CheckRequest,
// and sends it to the OpenFGA client to check the permission.
func (c *ACLClient) CheckPermission(ctx context.Context, objectType string, objectUID uuid.UUID, role string) (bool, error) {
	// Retrieve the user type from the request context headers
	userType := resource.GetRequestSingleHeader(ctx, constantx.HeaderAuthTypeKey)
	userUID := ""

	// Determine the user UID based on the user type
	if userType == "user" {
		// If the user type is "user", get the user UID from the corresponding header
		userUID = resource.GetRequestSingleHeader(ctx, constantx.HeaderUserUIDKey)
	} else {
		// If the user type is not "user", assume it is "visitor" and get the visitor UID from the corresponding header
		userUID = resource.GetRequestSingleHeader(ctx, constantx.HeaderVisitorUIDKey)
	}

	// Check if the user UID is empty and return an error if it is
	if userUID == "" {
		return false, fmt.Errorf("%w: userUID is empty in check permission", errorsx.ErrUnauthenticated)
	}

	// Create a CheckRequest to verify the user's permission for the specified object and role
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
		// Return false and the error if the CheckRequest fails
		return false, err
	}

	// Return the result of the permission check
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

	userType := resource.GetRequestSingleHeader(ctx, constantx.HeaderAuthTypeKey)
	userUIDStr := ""
	if userType == "user" {
		userUIDStr = resource.GetRequestSingleHeader(ctx, constantx.HeaderUserUIDKey)

	} else {
		userUIDStr = resource.GetRequestSingleHeader(ctx, constantx.HeaderVisitorUIDKey)
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
	//  TODO: handle error when no auth model is created
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
	authType := resource.GetRequestSingleHeader(ctx, constantx.HeaderAuthTypeKey)
	if authType != "user" {
		// Only authenticated users can switch namespaces.
		return fmt.Errorf("unauthenticated user")
	}
	requester := resource.GetRequestSingleHeader(ctx, constantx.HeaderRequesterUIDKey)
	authenticatedUser := resource.GetRequestSingleHeader(ctx, constantx.HeaderUserUIDKey)
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

// CheckShareLinkPermission checks if a share link token has the specified permission
// for a given resource. This is used to authorize anonymous access via share links.
// This is infrastructure for EE features.
func (c *ACLClient) CheckShareLinkPermission(ctx context.Context, shareToken string, objectType string, objectUID uuid.UUID, relation string) (bool, error) {
	// Create a CheckRequest to verify the share link's permission for the specified object and role
	// The user is identified as share_link:{token}
	data, err := c.getClient(ctx, ReadMode).Check(ctx, &openfga.CheckRequest{
		StoreId:              c.storeID,
		AuthorizationModelId: c.authorizationModelID,
		TupleKey: &openfga.CheckRequestTupleKey{
			User:     fmt.Sprintf("share_link:%s", shareToken),
			Relation: relation,
			Object:   fmt.Sprintf("%s:%s", objectType, objectUID.String()),
		},
	})
	if err != nil {
		// Handle specific error codes
		if statusErr, ok := status.FromError(err); ok {
			if statusErr.Code() == codes.Code(openfga.ErrorCode_type_not_found) {
				return false, nil
			}
		}
		return false, err
	}

	return data.Allowed, nil
}
