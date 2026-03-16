package handler

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"go.uber.org/zap"

	"github.com/instill-ai/artifact-backend/pkg/mock"
	artifact "github.com/instill-ai/artifact-backend/pkg/service"
)

func TestNewPrivateHandler(t *testing.T) {
	c := qt.New(t)

	svc := mock.NewServiceMock(t)
	h := NewPrivateHandler(svc, zap.NewNop())

	c.Assert(h, qt.IsNotNil)
	c.Check(h.service, qt.Equals, artifact.Service(svc))
	c.Check(h.logger, qt.IsNotNil)
}

func TestPrivateHandler_GetService(t *testing.T) {
	c := qt.New(t)

	c.Run("returns the injected service", func(c *qt.C) {
		svc := mock.NewServiceMock(t)
		h := NewPrivateHandler(svc, zap.NewNop())

		got := h.GetService()
		c.Assert(got, qt.IsNotNil)
		c.Check(got, qt.Equals, artifact.Service(svc))
	})

	c.Run("returns nil when handler created with nil service", func(c *qt.C) {
		h := NewPrivateHandler(nil, zap.NewNop())

		got := h.GetService()
		c.Check(got, qt.IsNil)
	})
}
