package repository

// import (
// 	"context"
// 	"fmt"
// 	"os"
// 	"testing"

// 	"github.com/google/uuid"
// 	"github.com/instill-ai/artifact-backend/config"
// 	"github.com/instill-ai/artifact-backend/pkg/db"
// )

// func TestGetEmbeddingByUIDs(t *testing.T) {
// 	// set file flag
// 	// 	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
// 	os.Args = []string{"", "-file", "../../config/config_local.yaml"}
// 	config.Init()
// 	// get db connection
// 	db := db.GetConnection()
// 	// get repository
// 	repo := NewRepository(db)
// 	// get embeddings
// 	uid := "006db525-ad0f-4951-8dd0-d226156b789b"
// 	// turn uid into uuid
// 	uidUUID, err := uuid.Parse(uid)
// 	if err != nil {
// 		t.Fatalf("Failed to parse uid: %v", err)
// 	}

// 	embeddings, err := repo.GetEmbeddingByUIDs(context.TODO(), []uuid.UUID{uidUUID})
// 	if err != nil {
// 		t.Fatalf("Failed to get embeddings: %v", err)
// 	}
// 	fmt.Println(embeddings)

// }
