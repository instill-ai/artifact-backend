package utils

import (
	"fmt"
)

const (
	CreateEvent string = "Create"
	UpdateEvent string = "Update"
	DeleteEvent string = "Delete"
)

// GoRecover take a function as input, if the function panics, the panic will be recovered and the error will be returned
func GoRecover(f func(), name string) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("\x1b[31m", "panic occurred at: \n", name, "\npanic: \n", r, "\x1b[0m")
		}
	}()
	f()
}
