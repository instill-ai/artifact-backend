package repository

import (
	"testing"
)

func TestKnowledgeBaseFile_ExtraMetaDataMarshal(t *testing.T) {
	kf := KnowledgeBaseFile{
		ExtraMetaDataUnmarshal: &ExtraMetaData{
			FaileReason: "Some reason",
		},
	}

	err := kf.ExtraMetaDataMarshal()
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	expected := `{"fail_reason":"Some reason"}`
	if kf.ExtraMetaData != expected {
		t.Errorf("Expected ExtraMetaData to be %q, but got %q", expected, kf.ExtraMetaData)
	}
}

// test ExtraMetaDataUnmarshal, when extra metadata is empty
func TestKnowledgeBaseFile_ExtraMetaDataUnmarshal_Empty(t *testing.T) {
	kf := KnowledgeBaseFile{ExtraMetaData: `{"fail_reason":"some reason"}`}
	err := kf.ExtraMetaDataUnmarshalFunc()
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if kf.ExtraMetaDataUnmarshal.FaileReason != "some reason" {
		t.Errorf("Expected FaileReason to be %q, but got %q", "some reason", kf.ExtraMetaDataUnmarshal.FaileReason)
	}
}

// test ExtraMetaDataUnmarshal, when extra metadata is ""
func TestKnowledgeBaseFile_ExtraMetaDataUnmarshal_EmptyString(t *testing.T) {
	kf := KnowledgeBaseFile{ExtraMetaData: ""}
	err := kf.ExtraMetaDataUnmarshalFunc()
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if kf.ExtraMetaDataUnmarshal != nil {
		t.Errorf("Expected ExtraMetaDataUnmarshal to be nil, but got %v", kf.ExtraMetaDataUnmarshal)
	}
}

// test ExtraMetaDataMarshal when ExtraMetaDataUnmarshal is nil
func TestKnowledgeBaseFile_ExtraMetaDataMarshal_Nil(t *testing.T) {
	kf := KnowledgeBaseFile{}
	err := kf.ExtraMetaDataMarshal()
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if kf.ExtraMetaData != "" {
		t.Errorf("Expected ExtraMetaData to be empty, but got %q", kf.ExtraMetaData)
	}
}

// // Note: need launch the db to run
// func TestKnowledgeBaseFile_Gorm_Hook(t *testing.T) {
// 	// set file flag
// 	// 	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
// 	os.Args = []string{"", "-file", "../../config/config_local.yaml"}
// 	config.Init()
// 	// get db connection
// 	db := db.GetConnection()
// 	// get repository
// 	repo := NewRepository(db)
// 	// create knowledge base
// 	kb, err := repo.CreateKnowledgeBase(context.TODO(), KnowledgeBase{
// 		Name: "kb-test-1",
// 		KbID: "kb-test-1",
// 		// owner is uuid
// 		Owner: "006db525-ad0f-4951-8dd0-d226156b789b",
// 	}, nil)
// 	if err != nil {
// 		t.Fatalf("Failed to create knowledge base: %v", err)
// 	}
// 	defer func() {
// 		_, err = repo.DeleteKnowledgeBase(context.TODO(), kb.Owner, kb.KbID)
// 		if err != nil {
// 			t.Fatalf("Failed to delete knowledge base: %v", err)
// 		}

// 	}()
// 	kbf, err := repo.CreateKnowledgeBaseFile(context.TODO(), KnowledgeBaseFile{
// 		KnowledgeBaseUID: kb.UID,
// 		ExtraMetaDataUnmarshal: &ExtraMetaData{
// 			FaileReason: "Some reason",
// 		},
// 	}, nil)
// 	defer func() {
// 		repo.DeleteKnowledgeBaseFile(context.TODO(), kbf.UID.String())
// 	}()
// 	kbfs, err := repo.GetKnowledgeBaseFilesByFileUIDs(context.TODO(), []uuid.UUID{kbf.UID})
// 	if err != nil {
// 		t.Fatalf("Failed to create knowledge base file: %v", err)
// 	}
// 	fmt.Println("kbf")
// 	data, err := json.MarshalIndent(kbfs[0], "", "  ")
// 	if err != nil {
// 		t.Fatalf("Failed to marshal JSON: %v", err)
// 	}
// 	fmt.Println(string(data))

// }

// // Note: need launch the db to run
// func TestKnowledgeBaseFile_Gorm_Hook_Nil(t *testing.T) {
// 	// set file flag
// 	// 	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
// 	os.Args = []string{"", "-file", "../../config/config_local.yaml"}
// 	config.Init()
// 	// get db connection
// 	db := db.GetConnection()
// 	// get repository
// 	repo := NewRepository(db)
// 	// create knowledge base
// 	kb, err := repo.CreateKnowledgeBase(context.TODO(), KnowledgeBase{
// 		Name: "kb-test-1",
// 		KbID: "kb-test-1",
// 		// owner is uuid
// 		Owner: "006db525-ad0f-4951-8dd0-d226156b789b",
// 	}, nil)
// 	if err != nil {
// 		t.Fatalf("Failed to create knowledge base: %v", err)
// 	}
// 	defer func() {
// 		_, err = repo.DeleteKnowledgeBase(context.TODO(), kb.Owner, kb.KbID)
// 		if err != nil {
// 			t.Fatalf("Failed to delete knowledge base: %v", err)
// 		}

// 	}()
// 	kbf, err := repo.CreateKnowledgeBaseFile(context.TODO(), KnowledgeBaseFile{
// 		KnowledgeBaseUID:       kb.UID,
// 		ExtraMetaDataUnmarshal: nil,
// 	}, nil)
// 	if err != nil {
// 		t.Fatalf("Failed to create knowledge base file: %v", err)
// 	}
// 	defer func() {
// 		repo.DeleteKnowledgeBaseFile(context.TODO(), kbf.UID.String())
// 	}()
// 	kbfs, err := repo.GetKnowledgeBaseFilesByFileUIDs(context.TODO(), []uuid.UUID{kbf.UID})
// 	if err != nil {
// 		t.Fatalf("Failed to create knowledge base file: %v", err)
// 	}
// 	fmt.Println("kbf")
// 	data, err := json.MarshalIndent(kbfs[0], "", "  ")
// 	if err != nil {
// 		t.Fatalf("Failed to marshal JSON: %v", err)
// 	}
// 	fmt.Println(string(data))

// }
