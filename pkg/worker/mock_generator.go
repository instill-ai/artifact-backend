package worker

// Generate mocks for interfaces that would create import cycles if generated in pkg/mock
//go:generate minimock -g -i github.com/instill-ai/artifact-backend/pkg/service.Service -o ./ -s "_mock.gen.go"
//go:generate minimock -g -i github.com/instill-ai/artifact-backend/pkg/service.VectorDatabase -o ./ -s "_mock.gen.go"
