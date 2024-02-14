package acl

import (
	"github.com/gofrs/uuid"

	openfgaClient "github.com/openfga/go-sdk/client"
)

type ACLClient struct {
	client               *openfgaClient.OpenFgaClient
	authorizationModelID *string
}

type Relation struct {
	UID      uuid.UUID
	Relation string
}

func NewACLClient(c *openfgaClient.OpenFgaClient, a *string) ACLClient {
	return ACLClient{
		client:               c,
		authorizationModelID: a,
	}
}
