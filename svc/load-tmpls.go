package svc

import (
	"embed"

	"github.com/pkg/errors"

	"ztbus/template"
)

// load query templates from embed
// leaving "loadErr" if all is not well

// Todo: prolly not as it's a bear to unit??

//go:embed es-tmpl/*
var tmplFs embed.FS

var (
	templates = &template.Template{
		Path:   "es-tmpl",
		Suffix: "yaml",
		Left:   "<<",
		Right:  ">>",
	}
	loadErr error
)

func init() {

	loadErr = templates.Load(tmplFs)
	loadErr = errors.Wrap(loadErr, "failed to load on init")
}
