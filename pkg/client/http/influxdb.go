package httpclient

import (
	"context"
	"time"

	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/influxdata/influxdb-client-go/v2/log"
	"google.golang.org/grpc/credentials"

	influxdb "github.com/influxdata/influxdb-client-go/v2"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/logger"
)

// NewInfluxDBClient returns an initialized InfluxDB HTTP client.
func NewInfluxDBClient(ctx context.Context) (influxdb.Client, api.WriteAPI) {
	logger, _ := logger.GetZapLogger(ctx)

	var creds credentials.TransportCredentials
	var err error

	influxOptions := influxdb.DefaultOptions()
	if config.Config.Server.Debug {
		influxOptions = influxOptions.SetLogLevel(log.DebugLevel)
	}
	influxOptions = influxOptions.SetFlushInterval(uint(time.Duration(config.Config.InfluxDB.FlushInterval * int(time.Second)).Milliseconds()))

	if config.Config.InfluxDB.HTTPS.Cert != "" && config.Config.InfluxDB.HTTPS.Key != "" {
		// TODO: support TLS
		creds, err = credentials.NewServerTLSFromFile(config.Config.InfluxDB.HTTPS.Cert, config.Config.InfluxDB.HTTPS.Key)
		if err != nil {
			logger.Fatal(err.Error())
		}
		logger.Info(creds.Info().ServerName)
	}

	client := influxdb.NewClientWithOptions(
		config.Config.InfluxDB.URL,
		config.Config.InfluxDB.Token,
		influxOptions,
	)

	if _, err := client.Ping(ctx); err != nil {
		logger.Warn(err.Error())
	}

	writeAPI := client.WriteAPI(config.Config.InfluxDB.Org, config.Config.InfluxDB.Bucket)

	return client, writeAPI
}
