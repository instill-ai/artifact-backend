package asset

import (
	"embed"
)

//go:embed preset/pipelines/*
var PresetPipelines embed.FS
