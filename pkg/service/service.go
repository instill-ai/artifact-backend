package service

import (
	"github.com/go-redis/redis/v9"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"go.temporal.io/sdk/client"

	"github.com/instill-ai/artifact-backend/pkg/acl"
	"github.com/instill-ai/artifact-backend/pkg/repository"
)

// Service interface
type Service interface {
}

type service struct {
	repository          repository.Repository
	redisClient         *redis.Client
	temporalClient      client.Client
	influxDBWriteClient api.WriteAPI
	aclClient           *acl.ACLClient
}

// NewService initiates a service instance
func NewService(
	r repository.Repository,
	rc *redis.Client,
	t client.Client,
	i api.WriteAPI,
	acl *acl.ACLClient) Service {
	return &service{
		repository:          r,
		redisClient:         rc,
		temporalClient:      t,
		influxDBWriteClient: i,
		aclClient:           acl,
	}
}
