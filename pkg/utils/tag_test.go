package utils

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestRepositoryTagName_ExtractRepositoryAndID(t *testing.T) {
	c := qt.New(t)

	validPairs := []struct {
		name RepositoryTagName
		repo string
		id   string
	}{
		{name: RepositoryTagName("repositories/connan-wombat/mockasin/tags/latest"), repo: "connan-wombat/mockasin", id: "latest"},
		{name: RepositoryTagName("repositories/shake/home/tags/1.0.0"), repo: "shake/home", id: "1.0.0"},
	}
	for _, pair := range validPairs {
		c.Run(fmt.Sprintf("ok - %s:%s", pair.repo, pair.id), func(c *qt.C) {
			repo, id, err := pair.name.ExtractRepositoryAndID()
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
