package repository

import (
	"database/sql/driver"
	"fmt"
	"time"

	"github.com/gofrs/uuid"
	"gopkg.in/guregu/null.v4"
	"gorm.io/datatypes"

	pb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	runpb "github.com/instill-ai/protogen-go/common/run/v1alpha"
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

// for saving the protobuf types as string values
type (
	RunStatus runpb.RunStatus
	RunSource runpb.RunSource
	RunAction pb.CatalogRunAction
)

func (v *RunStatus) Scan(value any) error {
	*v = RunStatus(runpb.RunStatus_value[value.(string)])
	return nil
}

func (v RunStatus) Value() (driver.Value, error) {
	return runpb.RunStatus(v).String(), nil
}

func (v *RunSource) Scan(value any) error {
	*v = RunSource(runpb.RunSource_value[value.(string)])
	return nil
}

func (v RunSource) Value() (driver.Value, error) {
	return runpb.RunSource(v).String(), nil
}

func (v *RunAction) Scan(value any) error {
	*v = RunAction(pb.CatalogRunAction_value[value.(string)])
	return nil
}

func (v RunAction) Value() (driver.Value, error) {
	return pb.CatalogRunAction(v).String(), nil
}

type CatalogRun struct {
	UID           uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();primaryKey"`
	CatalogUID    uuid.UUID
	Status        RunStatus
	Source        RunSource
	Action        RunAction
	RunnerUID     uuid.UUID
	RequesterUID  uuid.UUID
	FileUIDs      TagsArray      `gorm:"type:VARCHAR(255)[]"`
	ReferenceUIDs TagsArray      `gorm:"type:VARCHAR(255)[]"`
	Payload       datatypes.JSON `gorm:"type:jsonb"`
	StartedTime   time.Time
	CompletedTime null.Time
	TotalDuration null.Int
	Error         null.String
	CreateTime    time.Time `gorm:"autoCreateTime:nano"`
	UpdateTime    time.Time `gorm:"autoUpdateTime:nano"`
}
