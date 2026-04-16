package handler

import (
	"testing"
	"time"

	"github.com/gofrs/uuid"
	qt "github.com/frankban/quicktest"
	"gorm.io/gorm"

	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/resource"
	"github.com/instill-ai/artifact-backend/pkg/types"

	artifactpb "github.com/instill-ai/protogen-go/artifact/v1alpha"
	mgmtpb "github.com/instill-ai/protogen-go/mgmt/v1beta"
)

func TestCreatorResourceName(t *testing.T) {
	c := qt.New(t)

	c.Run("uses user ID when user proto is available", func(c *qt.C) {
		user := &mgmtpb.User{Id: "usr-abc123", Name: "users/usr-abc123"}
		got := creatorResourceName(user)
		c.Assert(got, qt.Equals, "users/usr-abc123")
	})

	c.Run("returns empty when user proto is nil", func(c *qt.C) {
		got := creatorResourceName(nil)
		c.Assert(got, qt.Equals, "")
	})

	c.Run("returns empty when user ID is empty", func(c *qt.C) {
		user := &mgmtpb.User{Id: ""}
		got := creatorResourceName(user)
		c.Assert(got, qt.Equals, "")
	})
}

func TestCreatorDisplayInfo(t *testing.T) {
	c := qt.New(t)

	c.Run("nil user returns empty", func(c *qt.C) {
		dn, av := creatorDisplayInfo(nil)
		c.Assert(dn, qt.Equals, "")
		c.Assert(av, qt.IsNil)
	})

	c.Run("user with profile avatar", func(c *qt.C) {
		avatarURL := "https://example.com/avatar.png"
		user := &mgmtpb.User{
			DisplayName: "Xiaofei",
			Profile:     &mgmtpb.UserProfile{Avatar: &avatarURL},
		}
		dn, av := creatorDisplayInfo(user)
		c.Assert(dn, qt.Equals, "Xiaofei")
		c.Assert(av, qt.Not(qt.IsNil))
		c.Assert(*av, qt.Equals, "https://example.com/avatar.png")
	})

	c.Run("user without avatar", func(c *qt.C) {
		user := &mgmtpb.User{
			DisplayName: "Alice",
			Profile:     &mgmtpb.UserProfile{},
		}
		dn, av := creatorDisplayInfo(user)
		c.Assert(dn, qt.Equals, "Alice")
		c.Assert(av, qt.IsNil)
	})
}

func TestOwnerDisplayInfo(t *testing.T) {
	c := qt.New(t)

	c.Run("nil owner returns empty", func(c *qt.C) {
		dn, av := ownerDisplayInfo(nil)
		c.Assert(dn, qt.Equals, "")
		c.Assert(av, qt.IsNil)
	})

	c.Run("user owner with avatar", func(c *qt.C) {
		avatarURL := "https://example.com/user-av.png"
		owner := &mgmtpb.Owner{
			Owner: &mgmtpb.Owner_User{
				User: &mgmtpb.User{
					DisplayName: "Bob",
					Profile:     &mgmtpb.UserProfile{Avatar: &avatarURL},
				},
			},
		}
		dn, av := ownerDisplayInfo(owner)
		c.Assert(dn, qt.Equals, "Bob")
		c.Assert(*av, qt.Equals, "https://example.com/user-av.png")
	})

	c.Run("org owner with avatar", func(c *qt.C) {
		avatarURL := "https://example.com/org-av.png"
		owner := &mgmtpb.Owner{
			Owner: &mgmtpb.Owner_Organization{
				Organization: &mgmtpb.Organization{
					DisplayName: "Instill AI",
					Profile:     &mgmtpb.OrganizationProfile{Avatar: &avatarURL},
				},
			},
		}
		dn, av := ownerDisplayInfo(owner)
		c.Assert(dn, qt.Equals, "Instill AI")
		c.Assert(*av, qt.Equals, "https://example.com/org-av.png")
	})
}

func TestConvertKBFileToPB_CreatorName(t *testing.T) {
	c := qt.New(t)

	now := time.Now()
	creatorUID := uuid.FromStringOrNil("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")

	makeFileModel := func() *repository.FileModel {
		return &repository.FileModel{
			UID:           uuid.FromStringOrNil("11111111-2222-3333-4444-555555555555"),
			ID:            "fil-abc123",
			Slug:          "test-file",
			DisplayName:   "test.pdf",
			FileType:      "TYPE_PDF",
			ProcessStatus: "FILE_PROCESS_STATUS_COMPLETED",
			CreatorUID:    types.CreatorUIDType(creatorUID),
			CreateTime:    &now,
			UpdateTime:    &now,
			DeleteTime:    gorm.DeletedAt{},
			Size:          1024,
		}
	}

	ns := &resource.Namespace{NsUID: uuid.FromStringOrNil("99999999-0000-0000-0000-000000000001"), NsID: "test-ns"}
	kb := &repository.KnowledgeBaseModel{ID: "kb-xyz", DisplayName: "Test KB"}

	c.Run("CreatorName uses user ID when creator is resolved", func(c *qt.C) {
		creator := &mgmtpb.User{
			Id:          "usr-u0xxiIMznp",
			Name:        "users/usr-u0xxiIMznp",
			DisplayName: "Xiaofei",
		}
		file := convertKBFileToPB(makeFileModel(), ns, kb, nil, creator, "")
		c.Assert(file.CreatorName, qt.Equals, "users/usr-u0xxiIMznp")
		c.Assert(file.CreatorDisplayName, qt.Equals, "Xiaofei")
	})

	c.Run("CreatorName is empty when creator is nil (no UUID leak)", func(c *qt.C) {
		file := convertKBFileToPB(makeFileModel(), ns, kb, nil, nil, "")
		c.Assert(file.CreatorName, qt.Equals, "")
		c.Assert(file.CreatorDisplayName, qt.Equals, "")
	})

	c.Run("OwnerDisplayName and OwnerAvatar populated from owner", func(c *qt.C) {
		avatarURL := "https://example.com/org.png"
		owner := &mgmtpb.Owner{
			Owner: &mgmtpb.Owner_Organization{
				Organization: &mgmtpb.Organization{
					DisplayName: "My Org",
					Profile:     &mgmtpb.OrganizationProfile{Avatar: &avatarURL},
				},
			},
		}
		file := convertKBFileToPB(makeFileModel(), ns, kb, owner, nil, "")
		c.Assert(file.OwnerDisplayName, qt.Equals, "My Org")
		c.Assert(file.GetOwnerAvatar(), qt.Equals, "https://example.com/org.png")
	})

	c.Run("File fields are correctly set", func(c *qt.C) {
		file := convertKBFileToPB(makeFileModel(), ns, kb, nil, nil, "obj-hash123")
		c.Assert(file.Id, qt.Equals, "fil-abc123")
		c.Assert(file.Slug, qt.Equals, "test-file")
		c.Assert(file.DisplayName, qt.Equals, "test.pdf")
		c.Assert(file.Name, qt.Equals, "namespaces/test-ns/knowledge-bases/kb-xyz/files/fil-abc123")
		c.Assert(file.Object, qt.Equals, "namespaces/test-ns/objects/obj-hash123")
		c.Assert(file.Size, qt.Equals, int64(1024))
	})

	c.Run("Visibility defaults to WORKSPACE when model field is empty", func(c *qt.C) {
		// Legacy rows created before the visibility column existed will have
		// an empty string after migration rollback/forward, so the converter
		// must never surface UNSPECIFIED to the wire.
		file := convertKBFileToPB(makeFileModel(), ns, kb, nil, nil, "")
		c.Assert(file.Visibility, qt.Equals, artifactpb.File_VISIBILITY_WORKSPACE)
	})

	c.Run("Visibility from model is propagated to proto", func(c *qt.C) {
		m := makeFileModel()
		m.Visibility = artifactpb.File_VISIBILITY_LINK_SHARED.String()
		file := convertKBFileToPB(m, ns, kb, nil, nil, "")
		c.Assert(file.Visibility, qt.Equals, artifactpb.File_VISIBILITY_LINK_SHARED)
	})
}

func TestConvertFileVisibility(t *testing.T) {
	c := qt.New(t)

	testCases := []struct {
		name  string
		input string
		want  artifactpb.File_Visibility
	}{
		{"empty falls back to WORKSPACE", "", artifactpb.File_VISIBILITY_WORKSPACE},
		{"unknown value falls back to WORKSPACE", "VISIBILITY_GARBAGE", artifactpb.File_VISIBILITY_WORKSPACE},
		{"UNSPECIFIED is normalized to WORKSPACE", "VISIBILITY_UNSPECIFIED", artifactpb.File_VISIBILITY_WORKSPACE},
		{"PRIVATE", "VISIBILITY_PRIVATE", artifactpb.File_VISIBILITY_PRIVATE},
		{"PUBLIC", "VISIBILITY_PUBLIC", artifactpb.File_VISIBILITY_PUBLIC},
		{"WORKSPACE", "VISIBILITY_WORKSPACE", artifactpb.File_VISIBILITY_WORKSPACE},
		{"LINK_SHARED", "VISIBILITY_LINK_SHARED", artifactpb.File_VISIBILITY_LINK_SHARED},
	}

	for _, tc := range testCases {
		tc := tc
		c.Run(tc.name, func(c *qt.C) {
			c.Assert(convertFileVisibility(tc.input), qt.Equals, tc.want)
		})
	}
}

func TestNormalizeFileVisibility(t *testing.T) {
	c := qt.New(t)

	testCases := []struct {
		name  string
		input artifactpb.File_Visibility
		want  string
	}{
		{"UNSPECIFIED maps to WORKSPACE string", artifactpb.File_VISIBILITY_UNSPECIFIED, "VISIBILITY_WORKSPACE"},
		{"PRIVATE round-trips", artifactpb.File_VISIBILITY_PRIVATE, "VISIBILITY_PRIVATE"},
		{"PUBLIC round-trips", artifactpb.File_VISIBILITY_PUBLIC, "VISIBILITY_PUBLIC"},
		{"WORKSPACE round-trips", artifactpb.File_VISIBILITY_WORKSPACE, "VISIBILITY_WORKSPACE"},
		{"LINK_SHARED round-trips", artifactpb.File_VISIBILITY_LINK_SHARED, "VISIBILITY_LINK_SHARED"},
	}

	for _, tc := range testCases {
		tc := tc
		c.Run(tc.name, func(c *qt.C) {
			c.Assert(normalizeFileVisibility(tc.input), qt.Equals, tc.want)
		})
	}
}

func TestExtractCollectionIDs(t *testing.T) {
	c := qt.New(t)

	c.Run("extracts collection IDs from tags", func(c *qt.C) {
		tags := []string{"agent:collection:col-abc", "other-tag", "agent:collection:col-xyz"}
		ids := extractCollectionIDs(tags)
		c.Assert(ids, qt.DeepEquals, []string{"col-abc", "col-xyz"})
	})

	c.Run("returns nil for no matching tags", func(c *qt.C) {
		tags := []string{"tag1", "tag2"}
		ids := extractCollectionIDs(tags)
		c.Assert(ids, qt.IsNil)
	})

	c.Run("skips empty collection IDs", func(c *qt.C) {
		tags := []string{"agent:collection:", "agent:collection:col-ok"}
		ids := extractCollectionIDs(tags)
		c.Assert(ids, qt.DeepEquals, []string{"col-ok"})
	})
}
