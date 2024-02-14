package utils

import "strings"

const (
	CreateEvent     string = "Create"
	UpdateEvent     string = "Update"
	DeleteEvent     string = "Delete"
)

func IsAuditEvent(eventName string) bool {
	return strings.HasPrefix(eventName, CreateEvent) ||
		strings.HasPrefix(eventName, UpdateEvent) ||
		strings.HasPrefix(eventName, DeleteEvent)
}
