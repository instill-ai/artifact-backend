package mock

//go:generate minimock -g -i github.com/instill-ai/artifact-backend/pkg/repository.Repository -o ./ -s "_mock.gen.go"
//go:generate minimock -g -i github.com/instill-ai/artifact-backend/internal/ai.Provider -o ./ -s "_mock.gen.go"
