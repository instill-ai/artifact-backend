package repository

import (
	"fmt"
	"time"
)

type repositoryTag struct {
	Name       string `gorm:"primaryKey"`
	Digest     string
	UpdateTime time.Time `gorm:"autoUpdateTime:nano"`
}

func (repositoryTag) TableName() string {
	return "repository_tag"
}

func repositoryTagName(repo, id string) string {
	// In the database, the tag name is the primary key. It is compacted to
	// <repository>:tag to improve the efficiency of the queries.
	return fmt.Sprintf("%s:%s", repo, id)
}
