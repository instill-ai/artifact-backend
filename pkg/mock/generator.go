package mock

// Mocks for service and handler tests
// Worker package has its own mocks in pkg/worker/mock to avoid import cycles
//go:generate minimock -g -i github.com/instill-ai/artifact-backend/pkg/service.RegistryClient -o ./ -s "_mock.gen.go"
//go:generate minimock -g -i github.com/instill-ai/artifact-backend/pkg/repository.Repository -o ./ -s "_mock.gen.go"
//go:generate minimock -g -i github.com/instill-ai/artifact-backend/pkg/service.Service -o ./ -s "_mock.gen.go"
