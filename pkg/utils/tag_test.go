package utils

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestRepositoryTagName_ExtractRepositoryAndID(t *testing.T) {
	c := qt.New(t)

	validPairs := []struct {
		repo string
		id   string
	}{
		{repo: "connan-wombat/mockasin", id: "latest"},
		{repo: "shake/home", id: "1.0.0"},
	}
	for _, pair := range validPairs {
		c.Run(fmt.Sprintf("ok - %s:%s", pair.repo, pair.id), func(c *qt.C) {
			name := NewRepositoryTagName(pair.repo, pair.id)
			repo, id, err := name.ExtractRepositoryAndID()
			c.Check(err, qt.IsNil)
			c.Check(repo, qt.Equals, pair.repo)
			c.Check(id, qt.Equals, pair.id)
		})
	}

	invalid := []RepositoryTagName{
		"repositories/connan-wombat/tags/latest",
		"repository/connan-wombat/mockasin/tags/latest",
		"connan-wombat/mockasin:latest",
	}

	for _, name := range invalid {
		c.Run(fmt.Sprintf("nok - %s", name), func(c *qt.C) {
			repo, id, err := name.ExtractRepositoryAndID()
			c.Check(repo, qt.Equals, "")
			c.Check(id, qt.Equals, "")
			c.Check(err, qt.IsNotNil)
		})
	}
}

func TestNewRepositoryTagName(t *testing.T) {
	c := qt.New(t)
	got := NewRepositoryTagName("connan-wombat/mockasin", "latest")
	want := RepositoryTagName("repositories/connan-wombat/mockasin/tags/latest")
	c.Check(got, qt.Equals, want)
}
