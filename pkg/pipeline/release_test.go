package pipeline

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestPipelineReleaseFromName(t *testing.T) {
	c := qt.New(t)

	testcases := []struct {
		name    string
		input   string
		want    Release
		wantErr string
	}{
		{
			name:  "ok - valid pipeline release name",
			input: "namespace/pipeline@v1.0.0",
			want: Release{
				Namespace: "namespace",
				ID:        "pipeline",
				Version:   "v1.0.0",
			},
		},
		{
			name:  "ok - valid pipeline release name with complex version",
			input: "my-namespace/my-pipeline@v1.2.3-beta",
			want: Release{
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
			got, err := ReleaseFromName(tc.input)

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

func TestPipelineRelease_Name(t *testing.T) {
	c := qt.New(t)

	got := Release{
		Namespace: "namespace",
		ID:        "pipeline",
		Version:   "v1.0.0",
	}.Name()

	c.Check(got, qt.Equals, "namespace/pipeline@v1.0.0")
}

func TestPipelineReleases_Names(t *testing.T) {
	c := qt.New(t)

	testcases := []struct {
		name      string
		pipelines Releases
		want      []string
	}{
		{
			name: "ok - single pipeline",
			pipelines: Releases{
				{
					Namespace: "namespace",
					ID:        "pipeline",
					Version:   "v1.0.0",
				},
			},
			want: []string{"namespace/pipeline@v1.0.0"},
		},
		{
			name: "ok - multiple pipelines",
			pipelines: Releases{
				{
					Namespace: "namespace1",
					ID:        "pipeline1",
					Version:   "v1.0.0",
				},
				{
					Namespace: "namespace2",
					ID:        "pipeline2",
					Version:   "v2.1.0",
				},
				{
					Namespace: "preset",
					ID:        "indexing-embed",
					Version:   "v1.1.0",
				},
			},
			want: []string{
				"namespace1/pipeline1@v1.0.0",
				"namespace2/pipeline2@v2.1.0",
				"preset/indexing-embed@v1.1.0",
			},
		},
	}

	for _, tc := range testcases {
		c.Run(tc.name, func(c *qt.C) {
			got := tc.pipelines.Names()
			c.Check(got, qt.DeepEquals, tc.want)
		})
	}
}
