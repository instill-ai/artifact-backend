package mock

//go:generate minimock -g -i github.com/instill-ai/artifact-backend/pkg/repository.Repository -o ./ -s "_mock.gen.go"
//go:generate minimock -g -i github.com/instill-ai/artifact-backend/internal/ai.Client -o ./ -s "_mock.gen.go"
