package service

import "context"

// RegistryClient interacts with a distribution registry to manage
// repositories.
type RegistryClient interface {
	ListTags(_ context.Context, repository string) (tags []string, _ error)
}
