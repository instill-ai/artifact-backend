package repository

import "time"

type repositoryTag struct {
	Name       string `gorm:"primaryKey"`
	Digest     string
	UpdateTime time.Time `gorm:"autoUpdateTime:nano"`
}

func (repositoryTag) TableName() string {
	return "repository_tag"
}
