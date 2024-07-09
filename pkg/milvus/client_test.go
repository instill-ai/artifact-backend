package milvus

import (
	"context"
	"fmt"
	"testing"

	"github.com/milvus-io/milvus-sdk-go/v2/client"
)

func TestMilvus(t *testing.T) {
	// Create a Milvus client instance
	mc, err := client.NewGrpcClient(context.TODO(), "localhost:19530")
	if err != nil {
		t.Fatalf("Failed to create Milvus client: %v", err)
	}
	defer mc.Close()
	h, err := mc.CheckHealth(context.TODO())
	if err != nil {
		t.Fatalf("Failed to check Milvus health: %v", err)
	}
	v, err := mc.GetVersion(context.TODO())
	if err != nil {
		t.Fatalf("Failed to get Milvus version: %v", err)
	}
	fmt.Printf("Successfully connected to Milvus!. health: %v, version: %v\n", h.IsHealthy, v)

}

func TestMilvusClientCreateCollection(t *testing.T) {
	mc, err := NewMilvusClient(context.TODO(), "localhost", "19530")
	if err != nil {
		t.Fatalf("Failed to create Milvus client: %v", err)
	}
	defer mc.Close()
	h, err := mc.GetHealth(context.TODO())
	if err != nil {
		t.Fatalf("Failed to check Milvus health: %v", err)
	}
	v, err := mc.GetVersion(context.TODO())
	if err != nil {
		t.Fatalf("Failed to get Milvus version: %v", err)
	}
	fmt.Printf("Successfully connected to Milvus!. health: %v, version: %v\n", h, v)

	// Create a collection
	kbUID := "gary_test_kb_1"
	err = mc.CreateKnowledgeBaseCollection(context.TODO(), kbUID)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}
}

func TestMilvusClientInsertVectorsToKnowledgeBaseCollection(t *testing.T) {
	mc, err := NewMilvusClient(context.TODO(), "localhost", "19530")
	if err != nil {
		t.Fatalf("Failed to create Milvus client: %v", err)
	}
	defer mc.Close()

	h, err := mc.GetHealth(context.TODO())
	if err != nil {
		t.Fatalf("Failed to check Milvus health: %v", err)
	}
	v, err := mc.GetVersion(context.TODO())
	if err != nil {
		t.Fatalf("Failed to get Milvus version: %v", err)
	}
	fmt.Printf("Successfully connected to Milvus! Health: %v, Version: %v\n", h, v)

	kbUID := "gary_test_kb_1"
	err = mc.CreateKnowledgeBaseCollection(context.TODO(), kbUID)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// Adjust the vector size to match the collection schema (3072 dimensions)
	dummyVector := make([]float32, 3072)
	for i := range dummyVector {
		dummyVector[i] = 0.1 // Example value, adjust as needed
	}
	dummyVector2 := make([]float32, 3072)
	for i := range dummyVector {
		dummyVector2[i] = 0.1 // Example value, adjust as needed
	}
	dummyVector3 := make([]float32, 3072)
	for i := range dummyVector {
		dummyVector2[i] = 0.1 // Example value, adjust as needed
	}
	dummyVector[0] = 1
	dummyVector2[0] = 10
	dummyVector3[0] = 100
	embeddings := []Embedding{
		{
			SourceTable:  "table1",
			SourceUID:    "id1",
			EmbeddingUID: "uid1",
			Vector:       dummyVector,
		},
		{
			SourceTable:  "table1",
			SourceUID:    "id2",
			EmbeddingUID: "uid2",
			Vector:       dummyVector2,
		},
		{
			SourceTable:  "table1",
			SourceUID:    "id3",
			EmbeddingUID: "uid3",
			Vector:       dummyVector3,
		},
	}

	err = mc.InsertVectorsToKnowledgeBaseCollection(context.TODO(), kbUID, embeddings)
	if err != nil {
		t.Fatalf("Failed to insert vectors: %v", err)
	}
	fmt.Println("Successfully inserted vectors to collection", kbUID)
	t.Logf("Successfully inserted vectors to collection %s", kbUID)
}

// test get all collection names
func TestMilvusClientGetAllCollectionNames(t *testing.T) {
	mc, err := NewMilvusClient(context.TODO(), "localhost", "19530")
	if err != nil {
		t.Fatalf("Failed to create Milvus client: %v", err)
	}
	defer mc.Close()

	h, err := mc.GetHealth(context.TODO())
	if err != nil {
		t.Fatalf("Failed to check Milvus health: %v", err)
	}
	v, err := mc.GetVersion(context.TODO())
	if err != nil {
		t.Fatalf("Failed to get Milvus version: %v", err)
	}
	fmt.Printf("Successfully connected to Milvus! Health: %v, Version: %v\n", h, v)

	collections, err := mc.GetAllCollectionNames(context.TODO())
	if err != nil {
		t.Fatalf("Failed to get all collection names: %v", err)
	}
	fmt.Println("All collection names:", collections)
	t.Logf("All collection names: %v", collections)
	for _, c := range collections {
		fmt.Println(c.Name)
	}
}

// test list embeddings
func TestMilvusClientListEmbeddings(t *testing.T) {
	mc, err := NewMilvusClient(context.TODO(), "localhost", "19530")
	if err != nil {
		t.Fatalf("Failed to create Milvus client: %v", err)
	}
	defer mc.Close()

	h, err := mc.GetHealth(context.TODO())
	if err != nil {
		t.Fatalf("Failed to check Milvus health: %v", err)
	}
	v, err := mc.GetVersion(context.TODO())
	if err != nil {
		t.Fatalf("Failed to get Milvus version: %v", err)
	}
	fmt.Printf("Successfully connected to Milvus! Health: %v, Version: %v\n", h, v)

	collections, err := mc.GetAllCollectionNames(context.TODO())
	if err != nil {
		t.Fatalf("Failed to get all collection names: %v", err)
	}
	for _, c := range collections {
		fmt.Println("collection name:", c.Name)
		embeddings, err := mc.ListEmbeddings(context.TODO(), c.Name)
		if err != nil {
			t.Fatalf("Failed to list embeddings: %v", err)
		}

		for i, e := range embeddings {
			fmt.Println("embedding", i)
			fmt.Println("embedding's sourceTable:", e.SourceTable)
			fmt.Println("embedding's sourceUID:", e.SourceUID)
			fmt.Println("embedding's embeddingUID:", e.EmbeddingUID)
			fmt.Println("embedding's vector's dims:", len(e.Vector))
			fmt.Println("embedding's vector's first element:", e.Vector[0])

		}
		t.Logf("Embeddings in collection %s: %v", c.Name, embeddings)
	}
}

// test delete embeddings
func TestMilvusClientDeleteEmbeddings(t *testing.T) {
	mc, err := NewMilvusClient(context.TODO(), "localhost", "19530")
	if err != nil {
		t.Fatalf("Failed to create Milvus client: %v", err)
	}
	defer mc.Close()

	h, err := mc.GetHealth(context.TODO())
	if err != nil {
		t.Fatalf("Failed to check Milvus health: %v", err)
	}
	v, err := mc.GetVersion(context.TODO())
	if err != nil {
		t.Fatalf("Failed to get Milvus version: %v", err)
	}
	fmt.Printf("Successfully connected to Milvus! Health: %v, Version: %v\n", h, v)
	err = mc.DeleteEmbedding(context.TODO(), "kb_gary_test_kb_1", []string{"5a51f2d5-9587-4472-9be2-67fd200145f3", "uid2"})
	if err != nil {
		t.Fatalf("Failed to delete embeddings: %v", err)
	}

	fmt.Println("Successfully deleted embeddings")

}
func TestMilvusClientSearchEmbeddings(t *testing.T) {
	mc, err := NewMilvusClient(context.TODO(), "localhost", "19530")
	if err != nil {
		t.Fatalf("Failed to create Milvus client: %v", err)
	}
	defer mc.Close()

	h, err := mc.GetHealth(context.TODO())
	if err != nil {
		t.Fatalf("Failed to check Milvus health: %v", err)
	}
	v, err := mc.GetVersion(context.TODO())
	if err != nil {
		t.Fatalf("Failed to get Milvus version: %v", err)
	}
	fmt.Printf("Successfully connected to Milvus! Health: %v, Version: %v\n", h, v)

	collectionName := "kb_gary_test_kb_1"
	dummyVector := make([]float32, 3072)
	for i := range dummyVector {
		dummyVector[i] = 0.1 // Example value, adjust as needed
	}
	dummyVector[0] = 1
	topK := 2
	batchVector := [][]float32{dummyVector}
	embeddings, err := mc.SearchEmbeddings(context.TODO(), collectionName, batchVector, topK)
	if err != nil {
		t.Fatalf("Failed to search embeddings: %v", err)
	}

	fmt.Println("Search results:")
	for i, es := range embeddings {
		fmt.Println("searchVector[:10]: ", batchVector[i][:10])
		for j, e := range es {
			fmt.Println("embedding", j)
			fmt.Println("embedding's sourceTable:", e.SourceTable)
			fmt.Println("embedding's sourceUID:", e.SourceUID)
			fmt.Println("embedding's embeddingUID:", e.EmbeddingUID)
			fmt.Println("embedding's vector's dims:", len(e.Vector))
			fmt.Println("embedding's vector[:10]:", e.Vector[:10])
		}
	}

	t.Logf("Search results: %v", embeddings)
}
