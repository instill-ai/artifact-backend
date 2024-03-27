package worker

import (
	"github.com/go-redis/redis/v9"
	"github.com/influxdata/influxdb-client-go/v2/api"
)

const TaskQueue = "artifact-backend"

type Worker interface {
}

type worker struct {
	redisClient         *redis.Client
	influxDBWriteClient api.WriteAPI
}

func NewWorker(rd *redis.Client, i api.WriteAPI) Worker {
	return &worker{
		redisClient:         rd,
		influxDBWriteClient: i,
	}
}
