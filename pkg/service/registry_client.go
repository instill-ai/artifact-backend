package service

import "context"

// RegistryClient interacts with a distribution registry to manage
// repositories.
type RegistryClient interface {
	ListTags(_ context.Context, repository string) (tags []string, _ error)
	DeleteTag(_ context.Context, repository string, digest string) (_ error)
	GetTagDigest(_ context.Context, repository string, tag string) (digest string, _ error)
}
