package worker

import (
	"github.com/go-redis/redis/v9"
	"github.com/influxdata/influxdb-client-go/v2/api"

	"github.com/instill-ai/artifact-backend/pkg/repository"
)

const TaskQueue = "artifact-backend"

type Worker interface {
}

type worker struct {
	repository          repository.Repository
	redisClient         *redis.Client
	influxDBWriteClient api.WriteAPI
}

func NewWorker(r repository.Repository, rd *redis.Client, i api.WriteAPI) Worker {
	return &worker{
		repository:          r,
		redisClient:         rd,
		influxDBWriteClient: i,
	}
}
