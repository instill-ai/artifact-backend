package service

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestPipelineReleaseFromName(t *testing.T) {
	c := qt.New(t)

	testcases := []struct {
		name    string
		input   string
		want    PipelineRelease
		wantErr string
	}{
		{
			name:  "ok - valid pipeline release name",
			input: "namespace/pipeline@v1.0.0",
			want: PipelineRelease{
				Namespace: "namespace",
				ID:        "pipeline",
				Version:   "v1.0.0",
			},
		},
		{
			name:  "ok - valid pipeline release name with complex version",
			input: "my-namespace/my-pipeline@v1.2.3-beta",
			want: PipelineRelease{
				Namespace: "my-namespace",
				ID:        "my-pipeline",
				Version:   "v1.2.3-beta",
			},
		},
		{
			name:    "nok - missing namespace",
			input:   "pipeline@v1.0.0",
			wantErr: "name must have the format {namespace}/{id}@{version}",
		},
		{
			name:    "nok - missing version",
			input:   "namespace/pipeline",
			wantErr: "name must have the format {namespace}/{id}@{version}",
		},
		{
			name:    "nok - empty string",
			input:   "",
			wantErr: "name must have the format {namespace}/{id}@{version}",
		},
		{
			name:    "nok - invalid format with multiple @",
			input:   "namespace/pipeline@v1.0.0@extra",
			wantErr: "name must have the format {namespace}/{id}@{version}",
		},
		{
			name:    "nok - invalid format with multiple /",
			input:   "namespace/sub/pipeline@v1.0.0",
			wantErr: "name must have the format {namespace}/{id}@{version}",
		},
	}

	for _, tc := range testcases {
		c.Run(tc.name, func(c *qt.C) {
			got, err := PipelineReleaseFromName(tc.input)

			if tc.wantErr != "" {
				c.Check(err, qt.IsNotNil)
				c.Check(err.Error(), qt.Equals, tc.wantErr)
				return
			}

			c.Check(err, qt.IsNil)
			c.Check(got, qt.DeepEquals, tc.want)
		})
	}
}
