module github.com/instill-ai/artifact-backend

go 1.21.1

toolchain go1.21.3

require (
	github.com/go-redis/redis/v9 v9.0.0-beta.2
	github.com/gofrs/uuid v4.4.0+incompatible
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.16.0
	github.com/influxdata/influxdb-client-go/v2 v2.12.3
	github.com/instill-ai/protogen-go v0.3.3-alpha.0.20240213113306-651642106db5
	github.com/instill-ai/usage-client v0.2.4-alpha.0.20240123081026-6c78d9a5197a
	github.com/instill-ai/x v0.3.0-alpha.0.20231219052200-6230a89e386c
	github.com/knadh/koanf v1.5.0
	github.com/openfga/go-sdk v0.2.3
	go.opentelemetry.io/contrib/propagators/b3 v1.17.0
	go.opentelemetry.io/otel v1.16.0
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc v0.39.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.16.0
	go.opentelemetry.io/otel/exporters/stdout/stdoutmetric v0.39.0
	go.opentelemetry.io/otel/exporters/stdout/stdouttrace v1.16.0
	go.opentelemetry.io/otel/sdk v1.16.0
	go.opentelemetry.io/otel/sdk/metric v0.39.0
	go.opentelemetry.io/otel/trace v1.16.0
	go.temporal.io/sdk v1.21.0
	go.uber.org/zap v1.26.0
	golang.org/x/net v0.18.0
	google.golang.org/genproto/googleapis/rpc v0.0.0-20231030173426-d783a09b4405
	google.golang.org/grpc v1.59.0
	google.golang.org/protobuf v1.31.0
	gorm.io/driver/postgres v1.4.5
	gorm.io/gorm v1.24.7-0.20230306060331-85eaf9eeda11
)

require (
	github.com/gogo/status v1.1.1 // indirect
	github.com/golang/mock v1.6.0 // indirect
	github.com/google/go-cmp v0.6.0 // indirect
	github.com/pelletier/go-toml v1.9.3 // indirect
	go.temporal.io/api v1.16.0 // indirect
	golang.org/x/sync v0.5.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20231016165738-49dd2c1f3d0b // indirect
)

require (
	github.com/catalinc/hashcash v0.0.0-20220723060415-5e3ec3e24f67 // indirect
	github.com/cenkalti/backoff/v4 v4.2.1 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/deepmap/oapi-codegen v1.8.2 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/facebookgo/clock v0.0.0-20150410010913-600d898af40a // indirect
	github.com/fsnotify/fsnotify v1.6.0 // indirect
	github.com/go-logr/logr v1.2.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/gogo/googleapis v1.4.1 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/google/uuid v1.4.0 // indirect
	github.com/influxdata/line-protocol v0.0.0-20200327222509-2487e7298839 // indirect
	github.com/jackc/chunkreader/v2 v2.0.1 // indirect
	github.com/jackc/pgconn v1.14.0
	github.com/jackc/pgio v1.0.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgproto3/v2 v2.3.2 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/jackc/pgtype v1.13.0 // indirect
	github.com/jackc/pgx/v4 v4.17.2 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/lib/pq v1.10.7 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/pborman/uuid v1.2.1 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/robfig/cron v1.2.0 // indirect
	github.com/rogpeppe/go-internal v1.10.0 // indirect
	github.com/stretchr/objx v0.5.0 // indirect
	github.com/stretchr/testify v1.8.4 // indirect
	go.opentelemetry.io/otel/exporters/otlp/internal/retry v1.16.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric v0.39.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.16.0 // indirect
	go.opentelemetry.io/otel/metric v1.16.0 // indirect
	go.opentelemetry.io/proto/otlp v0.19.0 // indirect
	go.uber.org/atomic v1.10.0 // indirect
	go.uber.org/multierr v1.10.0 // indirect
	golang.org/x/crypto v0.17.0
	golang.org/x/sys v0.15.0 // indirect
	golang.org/x/text v0.14.0 // indirect
	golang.org/x/time v0.3.0 // indirect
	google.golang.org/genproto v0.0.0-20231016165738-49dd2c1f3d0b // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
