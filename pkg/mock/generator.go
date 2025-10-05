package mock

//go:generate minimock -g -i github.com/instill-ai/artifact-backend/pkg/service.RegistryClient -o ./ -s "_mock.gen.go"
//go:generate minimock -g -i github.com/instill-ai/artifact-backend/pkg/repository.RepositoryI -o ./ -s "_mock.gen.go"
//go:generate minimock -g -i github.com/instill-ai/artifact-backend/pkg/minio.MinioI -o ./ -s "_mock.gen.go"
//go:generate minimock -g -i github.com/instill-ai/artifact-backend/pkg/service.Service -o ./ -s "_mock.gen.go"
//go:generate minimock -g -i github.com/instill-ai/artifact-backend/pkg/service.VectorDatabase -o ./ -s "_mock.gen.go"
