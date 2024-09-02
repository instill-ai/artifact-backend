package utils

import (
	"fmt"
	"strings"
)

const (
	CreateEvent string = "Create"
	UpdateEvent string = "Update"
	DeleteEvent string = "Delete"
)

func IsAuditEvent(eventName string) bool {
	return strings.HasPrefix(eventName, CreateEvent) ||
		strings.HasPrefix(eventName, UpdateEvent) ||
		strings.HasPrefix(eventName, DeleteEvent)
}

// GoRecover take a function as input, if the function panics, the panic will be recovered and the error will be returned
func GoRecover(f func(), name string) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("panic occurred at: ", name, "panic: \n", r)
		}
	}()
	f()
}
