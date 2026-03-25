package handler

import (
	"context"
	"testing"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"

	qt "github.com/frankban/quicktest"

	"github.com/instill-ai/artifact-backend/pkg/mock"
	"github.com/instill-ai/artifact-backend/pkg/resource"
	"github.com/instill-ai/artifact-backend/pkg/types"

	artifactpb "github.com/instill-ai/protogen-go/artifact/v1alpha"
	constantx "github.com/instill-ai/x/constant"
)

func ctxWithUserUID(uid string) context.Context {
	md := metadata.New(map[string]string{
		constantx.HeaderUserUIDKey: uid,
	})
	return metadata.NewIncomingContext(context.Background(), md)
}

func TestGetObjectDownloadURL_PassesFormatToService(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name   string
		format string
	}{
		{name: "empty format", format: ""},
		{name: "pdf format", format: "pdf"},
	}

	nsUID := uuid.FromStringOrNil("00000000-0000-0000-0000-000000000001")

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			svc := mock.NewServiceMock(t)
			svc.GetNamespaceByNsIDMock.Return(&resource.Namespace{
				NsUID: nsUID,
				NsID:  "test-ns",
			}, nil)
			svc.CheckNamespacePermissionMock.Return(nil)

			var capturedFormat string
			svc.GetDownloadURLMock.Set(func(
				_ context.Context,
				_ string,
				_ types.NamespaceUIDType,
				_ string,
				_ int32,
				_ string,
				format string,
				_ string,
			) (*artifactpb.GetObjectDownloadURLResponse, error) {
				capturedFormat = format
				return &artifactpb.GetObjectDownloadURLResponse{
					DownloadUrl: "https://example.com/download",
					UrlExpireAt: timestamppb.Now(),
				}, nil
			})

			ph := NewPublicHandler(svc, zap.NewNop())

			resp, err := ph.GetObjectDownloadURL(ctxWithUserUID("user-uid"), &artifactpb.GetObjectDownloadURLRequest{
				Name:   "namespaces/test-ns/objects/obj-abc123",
				Format: tt.format,
			})

			c.Assert(err, qt.IsNil)
			c.Assert(resp, qt.IsNotNil)
			c.Check(capturedFormat, qt.Equals, tt.format)
		})
	}
}
