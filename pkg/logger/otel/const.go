package otel

import (
	"encoding/json"

	"go.opentelemetry.io/otel/trace"

	"github.com/instill-ai/artifact-backend/pkg/utils"

	mgmtpb "github.com/instill-ai/protogen-go/core/mgmt/v1beta"
)

type Option func(l logMessage) logMessage

type logMessage struct {
	ID          string `json:"id"`
	ServiceName string `json:"serviceName"`
	TraceInfo   struct {
		TraceID string `json:"traceID"`
		SpanID  string `json:"spanID"`
	}
	UserInfo struct {
		UserID   string `json:"userID"`
		UserUUID string `json:"userUUID"`
	}
	Event struct {
		IsAuditEvent bool `json:"isAuditEvent"`
		EventInfo    struct {
			EventName string `json:"eventName"`
			Billable  bool   `json:"billable"`
		}
		EventResource interface{} `json:"eventResource"`
		EventResult   interface{} `json:"eventResult"`
		EventMessage  string      `json:"eventMessage"`
	}
	ErrorMessage string `json:"errorMessage"`
	Metadata     interface{}
}

func SetEventResource(resource interface{}) Option {
	return func(l logMessage) logMessage {
		l.Event.EventResource = resource
		return l
	}
}

func SetEventResult(result interface{}) Option {
	return func(l logMessage) logMessage {
		l.Event.EventResult = result
		return l
	}
}

func SetEventMessage(message string) Option {
	return func(l logMessage) logMessage {
		l.Event.EventMessage = message
		return l
	}
}

func SetErrorMessage(e string) Option {
	return func(l logMessage) logMessage {
		l.ErrorMessage = e
		return l
	}
}

func SetMetadata(m string) Option {
	return func(l logMessage) logMessage {
		l.Metadata = m
		return l
	}
}

func NewLogMessage(
	span trace.Span,
	logID string,
	user *mgmtpb.User,
	eventName string,
	options ...Option,
) []byte {
	logMessage := logMessage{}
	logMessage.ID = logID
	logMessage.ServiceName = "mgmt-backend"
	logMessage.TraceInfo = struct {
		TraceID string "json:\"traceID\""
		SpanID  string "json:\"spanID\""
	}{
		TraceID: span.SpanContext().TraceID().String(),
		SpanID:  span.SpanContext().SpanID().String(),
	}
	logMessage.UserInfo = struct {
		UserID   string "json:\"userID\""
		UserUUID string "json:\"userUUID\""
	}{
		UserID:   user.Id,
		UserUUID: *user.Uid,
	}
	logMessage.Event = struct {
		IsAuditEvent bool "json:\"isAuditEvent\""
		EventInfo    struct {
			EventName string "json:\"eventName\""
			Billable  bool   "json:\"billable\""
		}
		EventResource interface{} "json:\"eventResource\""
		EventResult   interface{} "json:\"eventResult\""
		EventMessage  string      "json:\"eventMessage\""
	}{
		IsAuditEvent: utils.IsAuditEvent(eventName),
		EventInfo: struct {
			EventName string "json:\"eventName\""
			Billable  bool   "json:\"billable\""
		}{
			EventName: eventName,
			Billable:  false,
		},
	}

	for _, o := range options {
		logMessage = o(logMessage)
	}

	bLogMessage, _ := json.Marshal(logMessage)

	return bLogMessage
}
