package acl

import (
	"context"
	"fmt"

	"github.com/gofrs/uuid"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"

	openfga "github.com/openfga/api/proto/openfga/v1"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/types"

	aclx "github.com/instill-ai/x/acl"
)

// ACLClient wraps the shared ACL client and adds artifact-specific methods.
type ACLClient struct {
	*aclx.ACLClient
}

// Relation represents a permission relation.
type Relation struct {
	UID      types.ObjectUIDType
	Relation string
}

const CatalogObject = "knowledgebase"

// NewACLClient creates a new ACL client using the shared library.
func NewACLClient(wc openfga.OpenFGAServiceClient, rc openfga.OpenFGAServiceClient, redisClient *redis.Client) *ACLClient {
	cfg := aclx.Config{
		Host: config.Config.OpenFGA.Host,
		Port: config.Config.OpenFGA.Port,
		Replica: aclx.ReplicaConfig{
			Host:                 config.Config.OpenFGA.Replica.Host,
			Port:                 config.Config.OpenFGA.Replica.Port,
			ReplicationTimeFrame: config.Config.OpenFGA.Replica.ReplicationTimeFrame,
		},
		Cache: aclx.CacheConfig{
			Enabled: config.Config.OpenFGA.Cache.Enabled,
			TTL:     config.Config.OpenFGA.Cache.TTL,
		},
	}

	sharedClient := aclx.NewClient(wc, rc, redisClient, cfg)

	return &ACLClient{
		ACLClient: sharedClient,
	}
}

// InitOpenFGAClient initializes gRPC connections to OpenFGA server.
func InitOpenFGAClient(ctx context.Context, host string, port int) (openfga.OpenFGAServiceClient, *grpc.ClientConn) {
	return aclx.InitOpenFGAClient(ctx, host, port, config.Config.Server.MaxDataSize)
}

// SetKnowledgeBasePermission sets a permission for a user on a knowledge base.
func (c *ACLClient) SetKnowledgeBasePermission(ctx context.Context, kbUID types.KBUIDType, user, role string, enable bool) error {
	return c.SetResourcePermission(ctx, CatalogObject, uuid.UUID(kbUID), user, role, enable)
}

// DeleteKnowledgeBasePermission deletes all permissions for a user on a knowledge base.
func (c *ACLClient) DeleteKnowledgeBasePermission(ctx context.Context, kbUID types.KBUIDType, user string) error {
	return c.DeleteResourcePermission(ctx, CatalogObject, uuid.UUID(kbUID), user)
}

// SetPublicKnowledgeBasePermission sets public permissions on a knowledge base.
func (c *ACLClient) SetPublicKnowledgeBasePermission(ctx context.Context, kbUID types.KBUIDType) error {
	return c.SetPublicPermission(ctx, CatalogObject, uuid.UUID(kbUID))
}

// DeletePublicKnowledgeBasePermission deletes public permissions from a knowledge base.
func (c *ACLClient) DeletePublicKnowledgeBasePermission(ctx context.Context, kbUID types.KBUIDType) error {
	return c.DeletePublicPermission(ctx, CatalogObject, uuid.UUID(kbUID))
}

// GetKnowledgeBaseOwner retrieves the owner of a given knowledge base.
func (c *ACLClient) GetKnowledgeBaseOwner(ctx context.Context, kbUID types.KBUIDType) (string, string, error) {
	return c.GetOwner(ctx, CatalogObject, uuid.UUID(kbUID))
}

// CheckKnowledgeBasePermission checks if the current user has a specific permission on a knowledge base.
func (c *ACLClient) CheckKnowledgeBasePermission(ctx context.Context, kbUID types.KBUIDType, role string) (bool, error) {
	return c.CheckPermission(ctx, CatalogObject, uuid.UUID(kbUID), role)
}

// PurgeKnowledgeBase deletes all permissions associated with a knowledge base.
func (c *ACLClient) PurgeKnowledgeBase(ctx context.Context, kbUID types.KBUIDType) error {
	return c.Purge(ctx, CatalogObject, uuid.UUID(kbUID))
}

// SetKnowledgeBaseOwner sets the owner of a knowledge base.
func (c *ACLClient) SetKnowledgeBaseOwner(ctx context.Context, kbUID types.KBUIDType, ownerType string, ownerUID uuid.UUID) error {
	return c.SetOwner(ctx, CatalogObject, uuid.UUID(kbUID), ownerType, ownerUID)
}

// CheckKnowledgeBaseShareLink checks if a share link has permission for a knowledge base.
func (c *ACLClient) CheckKnowledgeBaseShareLink(ctx context.Context, shareToken string, kbUID types.KBUIDType, relation string) (bool, error) {
	return c.CheckShareLinkPermission(ctx, shareToken, CatalogObject, uuid.UUID(kbUID), relation)
}

// GetObjectOwner retrieves the owner of any object type - kept for backward compatibility.
func (c *ACLClient) GetObjectOwner(ctx context.Context, objectType string, objectUID uuid.UUID) (string, string, error) {
	return c.GetOwner(ctx, objectType, objectUID)
}

// CheckObjectShareLink checks if a share link has permission for any object type - kept for backward compatibility.
func (c *ACLClient) CheckObjectShareLink(ctx context.Context, shareToken string, objectType string, objectUID uuid.UUID, relation string) (bool, error) {
	return c.CheckShareLinkPermission(ctx, shareToken, objectType, objectUID, relation)
}

// Legacy alias for backward compatibility.
func (c *ACLClient) CheckShareLinkPermission(ctx context.Context, shareToken string, objectType string, objectUID uuid.UUID, relation string) (bool, error) {
	return c.ACLClient.CheckShareLinkPermission(ctx, shareToken, objectType, objectUID, relation)
}

// Legacy alias for backward compatibility.
func (c *ACLClient) CheckRequesterPermission(ctx context.Context) error {
	return c.ACLClient.CheckRequesterPermission(ctx)
}

// Legacy alias for backward compatibility - GetOwner delegates to shared client.
func (c *ACLClient) GetOwner(ctx context.Context, objectType string, objectUID uuid.UUID) (string, string, error) {
	return c.ACLClient.GetOwner(ctx, objectType, objectUID)
}

// Legacy alias for backward compatibility.
func (c *ACLClient) ListPermissions(ctx context.Context, objectType string, role string, isPublic bool) ([]uuid.UUID, error) {
	return c.ACLClient.ListPermissions(ctx, objectType, role, isPublic)
}

// FormatOwnerPermissionsPrefix returns a helper for owner permission formatting.
func FormatOwnerPermissionsPrefix(ownerType, ownerUID string) string {
	return fmt.Sprintf("%s:%s", ownerType, ownerUID)
}
