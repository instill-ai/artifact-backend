package config

import (
	"flag"
	"log"
	"os"
	"strings"
	"time"

	"github.com/go-playground/validator"
	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/confmap"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/redis/go-redis/v9"

	"github.com/instill-ai/x/client"
	"github.com/instill-ai/x/temporal"

	miniox "github.com/instill-ai/x/minio"
)

// Config - Global variable to export
var Config AppConfig

// AppConfig defines
type AppConfig struct {
	Server          ServerConfig          `koanf:"server"`
	Database        DatabaseConfig        `koanf:"database"`
	InfluxDB        InfluxDBConfig        `koanf:"influxdb"`
	Temporal        temporal.ClientConfig `koanf:"temporal"`
	Cache           CacheConfig           `koanf:"cache"`
	OTELCollector   OTELCollectorConfig   `koanf:"otelcollector"`
	MgmtBackend     client.ServiceConfig  `koanf:"mgmtbackend"`
	PipelineBackend client.ServiceConfig  `koanf:"pipelinebackend"`
	ModelBackend    client.ServiceConfig  `koanf:"modelbackend"`
	Registry        RegistryConfig        `koanf:"registry"`
	OpenFGA         OpenFGAConfig         `koanf:"openfga"`
	Minio           miniox.Config         `koanf:"minio"`
	Milvus          MilvusConfig          `koanf:"milvus"`
	Blob            BlobConfig            `koanf:"blob"`
	GCS             GCSConfig             `koanf:"gcs"`
	RAG             RAGConfig             `koanf:"rag"`
}

// OpenFGAConfig is the openfga configuration.
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
	InstillCoreHost string `koanf:"instillcorehost" validate:"url"`
}

// DatabaseConfig related to database
type DatabaseConfig struct {
	Username string `koanf:"username"`
	Password string `koanf:"password"`
	Host     string `koanf:"host"`
	Port     int    `koanf:"port"`
	Name     string `koanf:"name"`
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

// OTELCollectorConfig related to OTEL collector
type OTELCollectorConfig struct {
	Enable bool   `koanf:"enable"`
	Host   string `koanf:"host"`
	Port   int    `koanf:"port"`
}

// CacheConfig related to Redis
type CacheConfig struct {
	Redis struct {
		RedisOptions redis.Options `koanf:"redisoptions"`
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

// BlobConfig is the blob configuration.
type BlobConfig struct {
	HostPort string `koanf:"hostport"`
}

// RAGConfig defines the configuration for RAG (Retrieval Augmented Generation)
type RAGConfig struct {
	Update RAGUpdateConfig `koanf:"update"`
	Model  ModelConfig     `koanf:"model"`
}

// RAGUpdateConfig defines the configuration for RAG system updates
type RAGUpdateConfig struct {
	RollbackRetentionDays int `koanf:"rollback_retention_days"` // Days to keep old KB after swap
	BatchSize             int `koanf:"batch_size"`              // KBs to process concurrently
}

// ModelConfig defines the configuration for AI model providers
type ModelConfig struct {
	Gemini   GeminiConfig   `koanf:"gemini"`
	OpenAI   OpenAIConfig   `koanf:"openai"`
	VertexAI VertexAIConfig `koanf:"vertexai"`
	// Future AI providers:
	// Anthropic   AnthropicConfig   `koanf:"anthropic"`
}

// GeminiConfig defines the configuration for Gemini AI
type GeminiConfig struct {
	APIKey string `koanf:"apikey"`
}

// OpenAIConfig defines the configuration for OpenAI
type OpenAIConfig struct {
	APIKey string `koanf:"apikey"`
}

// VertexAIConfig defines the configuration for VertexAI
type VertexAIConfig struct {
	ProjectID string `koanf:"projectid"`
	Region    string `koanf:"region"`
	SAKey     string `koanf:"sakey"` // JSON string of service account key
}

// GCSConfig defines the configuration for Google Cloud Storage as an object storage backend
// GCS serves as an alternative to MinIO and becomes the default storage when VertexAI is configured
type GCSConfig struct {
	ProjectID string `koanf:"projectid"`
	Region    string `koanf:"region"`
	Bucket    string `koanf:"bucket"`
	SAKey     string `koanf:"sakey"` // JSON string of service account key
}

// Init - Assign global config to decoded config struct
func Init(filePath string) error {
	k := koanf.New(".")
	parser := yaml.Parser()

	if err := k.Load(confmap.Provider(map[string]any{
		"database.replica.replicationtimeframe": 60,
		"openfga.replica.replicationtimeframe":  60,
	}, "."), nil); err != nil {
		log.Fatal(err.Error())
	}

	if err := k.Load(file.Provider(filePath), parser); err != nil {
		log.Fatal(err.Error())
	}

	if err := k.Load(env.ProviderWithValue("CFG_", ".", func(s string, v string) (string, any) {
		key := strings.ReplaceAll(strings.ToLower(strings.TrimPrefix(s, "CFG_")), "_", ".")
		if strings.Contains(v, ",") {
			return key, strings.Split(strings.TrimSpace(v), ",")
		}
		return key, v
	}), nil); err != nil {
		return err
	}

	if err := k.Unmarshal("", &Config); err != nil {
		return err
	}

	return ValidateConfig(&Config)
}

// ValidateConfig is for custom validation rules for the configuration
func ValidateConfig(cfg *AppConfig) error {
	validate := validator.New()
	if err := validate.Struct(cfg); err != nil {
		return err
	}
	return nil
}

var defaultConfigPath = "config/config.yaml"

// ParseConfigFlag allows clients to specify the relative path to the file from
// which the configuration will be loaded.
func ParseConfigFlag() string {
	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	configPath := fs.String("file", defaultConfigPath, "configuration file")
	flag.Parse()

	return *configPath
}
