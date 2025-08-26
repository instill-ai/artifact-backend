package acl

import (
	"context"
	"fmt"

	"github.com/gofrs/uuid"
	openfgaclient "github.com/openfga/go-sdk/client"

	constantx "github.com/instill-ai/x/constant"
	openfgax "github.com/instill-ai/x/openfga"
	"github.com/instill-ai/x/resource"
)

// Artifact-specific object types
const (
	ObjectTypeKnowledgeBase openfgax.ObjectType = "knowledgebase"
	ObjectTypeOrganization  openfgax.ObjectType = "organization"
)

// aclClient wraps the x/openfga Client with artifact-backend specific operations
type aclClient struct {
	openfgax.Client
}

// ACLClientInterface defines the interface for artifact-backend ACL operations
type ACLClientInterface interface {
	openfgax.Client

	CheckPublicExecutable(ctx context.Context, objectType openfgax.ObjectType, objectUID uuid.UUID) (bool, error)
	SetKnowledgeBasePermission(ctx context.Context, kbUID uuid.UUID, user string, role string, enable bool) error
	SetPublicKnowledgeBasePermission(ctx context.Context, kbUID uuid.UUID) error
	DeleteKnowledgeBasePermission(ctx context.Context, kbUID uuid.UUID, user string) error
	DeletePublicKnowledgeBasePermission(ctx context.Context, kbUID uuid.UUID) error
	CheckRequesterPermission(ctx context.Context) error
}

// NewFGAClient creates a new artifact-backend specific FGA client
func NewFGAClient(client openfgax.Client) ACLClientInterface {
	return &aclClient{Client: client}
}

// CheckPublicExecutable checks if public users can execute an object
func (c *aclClient) CheckPublicExecutable(ctx context.Context, objectType openfgax.ObjectType, objectUID uuid.UUID) (bool, error) {
	body := openfgaclient.ClientCheckRequest{
		User:     fmt.Sprintf("%s:*", openfgax.OwnerTypeUser),
		Relation: "executor",
		Object:   fmt.Sprintf("%s:%s", objectType, objectUID.String()),
	}
	data, err := c.SDKClient().Check(ctx).Body(body).Execute()
	if err != nil {
		return false, err
	}
	return *data.Allowed, nil
}

// SetKnowledgeBasePermission sets a specific permission for a user on a knowledge base
func (c *aclClient) SetKnowledgeBasePermission(ctx context.Context, kbUID uuid.UUID, user string, role string, enable bool) error {
	// First delete existing permission for this user
	_ = c.DeleteKnowledgeBasePermission(ctx, kbUID, user)

	if enable {
		writeBody := openfgaclient.ClientWriteRequest{
			Writes: []openfgaclient.ClientTupleKey{
				{
					User:     user,
					Relation: role,
					Object:   fmt.Sprintf("%s:%s", ObjectTypeKnowledgeBase, kbUID.String()),
				},
			},
		}
		_, err := c.SDKClient().Write(ctx).Body(writeBody).Execute()
		return err
	}

	return nil
}

// SetPublicKnowledgeBasePermission sets public permissions for a knowledge base
func (c *aclClient) SetPublicKnowledgeBasePermission(ctx context.Context, kbUID uuid.UUID) error {
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

// DeleteKnowledgeBasePermission deletes all permissions for a user on a knowledge base
func (c *aclClient) DeleteKnowledgeBasePermission(ctx context.Context, kbUID uuid.UUID, user string) error {
	for _, role := range []string{"admin", "writer", "executor", "reader"} {
		deleteBody := openfgaclient.ClientWriteRequest{
			Deletes: []openfgaclient.ClientTupleKeyWithoutCondition{
				{
					User:     user,
					Relation: role,
					Object:   fmt.Sprintf("%s:%s", ObjectTypeKnowledgeBase, kbUID.String()),
				},
			},
		}
		_, _ = c.SDKClient().Write(ctx).Body(deleteBody).Execute()
	}

	return nil
}

// DeletePublicKnowledgeBasePermission deletes public permissions for a knowledge base
func (c *aclClient) DeletePublicKnowledgeBasePermission(ctx context.Context, kbUID uuid.UUID) error {
	for _, t := range []string{"user", "visitor"} {
		err := c.DeleteKnowledgeBasePermission(ctx, kbUID, fmt.Sprintf("%s:*", t))
		if err != nil {
			return err
		}
	}

	return nil
}

// CheckRequesterPermission checks if the current user can act as the requester
func (c *aclClient) CheckRequesterPermission(ctx context.Context) error {
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
	isMember, err := c.CheckPermission(ctx, ObjectTypeOrganization, uuid.FromStringOrNil(requester), "member")
	if err != nil {
		return fmt.Errorf("checking organization membership: %w", err)
	}

	if !isMember {
		return fmt.Errorf("authenticated user doesn't belong to requester organization")
	}

	return nil
}
