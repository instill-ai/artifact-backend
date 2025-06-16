package config

import (
	"flag"
	"log"
	"os"
	"strings"
	"time"

	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/redis/go-redis/v9"

	miniox "github.com/instill-ai/x/minio"
)

// Config - Global variable to export
var Config AppConfig

// AppConfig defines
type AppConfig struct {
	Server                ServerConfig          `koanf:"server"`
	Database              DatabaseConfig        `koanf:"database"`
	InfluxDB              InfluxDBConfig        `koanf:"influxdb"`
	Cache                 CacheConfig           `koanf:"cache"`
	Log                   LogConfig             `koanf:"log"`
	MgmtBackend           MgmtBackendConfig     `koanf:"mgmtbackend"`
	PipelineBackend       PipelineBackendConfig `koanf:"pipelinebackend"`
	ModelBackend          ModelBackendConfig    `koanf:"modelbackend"`
	Registry              RegistryConfig        `koanf:"registry"`
	OpenFGA               OpenFGAConfig         `koanf:"openfga"`
	Minio                 miniox.Config         `koanf:"minio"`
	Milvus                MilvusConfig          `koanf:"milvus"`
	FileToEmbeddingWorker FileToEmbeddingWorker `koanf:"filetoembeddingworker"`
	Blob                  BlobConfig            `koanf:"blob"`
	APIGateway            APIGatewayConfig      `koanf:"apigateway"`
}

// OpenFGA config
type OpenFGAConfig struct {
	Host    string `koanf:"host"`
	Port    int    `koanf:"port"`
	Replica struct {
		Host                 string `koanf:"host"`
		Port                 int    `koanf:"port"`
		ReplicationTimeFrame int    `koanf:"replicationtimeframe"` // in seconds
	} `koanf:"replica"`
}

// ServerConfig defines HTTP server configurations
type ServerConfig struct {
	PublicPort  int `koanf:"publicport"`
	PrivatePort int `koanf:"privateport"`
	HTTPS       struct {
		Cert string `koanf:"cert"`
		Key  string `koanf:"key"`
	}
	Edition string `koanf:"edition"`
	Usage   struct {
		Enabled    bool   `koanf:"enabled"`
		TLSEnabled bool   `koanf:"tlsenabled"`
		Host       string `koanf:"host"`
		Port       int    `koanf:"port"`
	}
	Debug       bool `koanf:"debug"`
	MaxDataSize int  `koanf:"maxdatasize"`
	Workflow    struct {
		MaxWorkflowTimeout int32 `koanf:"maxworkflowtimeout"`
		MaxWorkflowRetry   int32 `koanf:"maxworkflowretry"`
		MaxActivityRetry   int32 `koanf:"maxactivityretry"`
	}
}

// DatabaseConfig related to database
type DatabaseConfig struct {
	Username string `koanf:"username"`
	Password string `koanf:"password"`
	Host     string `koanf:"host"`
	Port     int    `koanf:"port"`
	Name     string `koanf:"name"`
	Version  uint   `koanf:"version"`
	TimeZone string `koanf:"timezone"`
	Pool     struct {
		IdleConnections int           `koanf:"idleconnections"`
		MaxConnections  int           `koanf:"maxconnections"`
		ConnLifeTime    time.Duration `koanf:"connlifetime"`
	}
}

// InfluxDBConfig related to influxDB database
type InfluxDBConfig struct {
	URL           string `koanf:"url"`
	Token         string `koanf:"token"`
	Org           string `koanf:"org"`
	Bucket        string `koanf:"bucket"`
	FlushInterval int    `koanf:"flushinterval"`
	HTTPS         struct {
		Cert string `koanf:"cert"`
		Key  string `koanf:"key"`
	}
}

// LogConfig related to logging
type LogConfig struct {
	External      bool `koanf:"external"`
	OtelCollector struct {
		Host string `koanf:"host"`
		Port string `koanf:"port"`
	}
}

// CacheConfig related to Redis
type CacheConfig struct {
	Redis struct {
		RedisOptions redis.Options `koanf:"redisoptions"`
	}
}

// MgmtBackendConfig related to mgmt-backend
type MgmtBackendConfig struct {
	Host        string `koanf:"host"`
	PublicPort  int    `koanf:"publicport"`
	PrivatePort int    `koanf:"privateport"`
	HTTPS       struct {
		Cert string `koanf:"cert"`
		Key  string `koanf:"key"`
	}
}

// PipelineBackendConfig related to pipeline-backend
type PipelineBackendConfig struct {
	Host        string `koanf:"host"`
	PublicPort  int    `koanf:"publicport"`
	PrivatePort int    `koanf:"privateport"`
	HTTPS       struct {
		Cert string `koanf:"cert"`
		Key  string `koanf:"key"`
	}
}

// ModelBackendConfig related to model-backend
type ModelBackendConfig struct {
	Host        string `koanf:"host"`
	PublicPort  int    `koanf:"publicport"`
	PrivatePort int    `koanf:"privateport"`
	Namespace   string `koanf:"namespace"`
	HTTPS       struct {
		Cert string `koanf:"cert"`
		Key  string `koanf:"key"`
	}
}

// RegistryConfig is the registry configuration.
type RegistryConfig struct {
	Host string `koanf:"host"`
	Port int    `koanf:"port"`
}

// MilvusConfig is the milvus configuration.
type MilvusConfig struct {
	Host string `koanf:"host"`
	Port string `koanf:"port"`
}

type FileToEmbeddingWorker struct {
	NumberOfWorkers int `koanf:"numberofworkers"`
}

type BlobConfig struct {
	HostPort string `koanf:"hostport"`
}

// APIGateway defines a way to call the public Instill API Gateway.
type APIGatewayConfig struct {
	URL   string `koanf:"url"`
	Token string `koanf:"token"`
}

// Init - Assign global config to decoded config struct
func Init() error {
	k := koanf.New(".")
	parser := yaml.Parser()
	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	fileRelativePath := fs.String("file", "config/config.yaml", "configuration file")
	err := fs.Parse(os.Args[1:])
	if err != nil {
		log.Fatal(err.Error())
	}
	if err := k.Load(
		file.Provider(*fileRelativePath),
		parser,
	); err != nil {
		log.Fatal(err.Error())
	}

	if err := k.Load(
		env.ProviderWithValue(
			"CFG_",
			".",
			func(k string, v string) (string, interface{}) {
				key := strings.ReplaceAll(strings.ToLower(strings.TrimPrefix(k, "CFG_")), "_", ".")
				if strings.Contains(v, ",") {
					return key, strings.Split(strings.TrimSpace(v), ",")
				}
				return key, v
			},
		),
		nil,
	); err != nil {
		return err
	}

	if err := k.Unmarshal("", &Config); err != nil {
		return err
	}

	return ValidateConfig(&Config)
}

// ValidateConfig is for custom validation rules for the configuration
func ValidateConfig(cfg *AppConfig) error {
	return nil
}
