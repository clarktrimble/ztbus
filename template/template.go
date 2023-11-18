package template

import (
	"bytes"
	"fmt"
	"io"
	"io/fs"
	"strings"
	"text/template"

	"github.com/ghodss/yaml"
	"github.com/pkg/errors"
)

// Todo: restore lost features as warranted
//  - delims
//  - render to reader
//  - quote comma
//  and modularize!

// Template is template configurables and a handle to the top-level template.
type Template struct {
	Suffix string
	Tmpl   *template.Template
}

// Load reads and parses templates from top folder of the given filesystem.
// Templates are named after file in which found.
func (tmpl *Template) Load(fs fs.FS) (err error) {

	tt := template.New("top")

	glob := fmt.Sprintf("*.%s", tmpl.Suffix)
	tt, err = tt.ParseFS(fs, glob)
	if err != nil {
		err = errors.Wrapf(err, "failed to parse templates: %s", glob)
		return
	}

	tmpl.Tmpl = tt
	return
}

// RenderString renders a template.
func (tmpl *Template) RenderString(name string, data interface{}) (out string, err error) {

	builder := &strings.Builder{}

	err = tmpl.render(name, data, builder)
	if err != nil {
		return
	}

	out = builder.String()
	return
}

// RenderJson renders a template and then, hoping the result is yaml, converts that to json.
func (tmpl *Template) RenderJson(name string, data interface{}) (out []byte, err error) {

	buf := &bytes.Buffer{}
	err = tmpl.render(name, data, buf)
	if err != nil {
		return
	}

	out, err = yaml.YAMLToJSON(buf.Bytes())
	err = errors.Wrapf(err, "failed to convert yaml into json")
	return
}

// unexported

func (tmpl *Template) render(name string, data interface{}, writer io.Writer) (err error) {

	if tmpl.Tmpl == nil {
		err = errors.Errorf("no templates loaded for: %#v", tmpl)
		return
	}

	err = tmpl.Tmpl.ExecuteTemplate(writer, fmt.Sprintf("%s.%s", name, tmpl.Suffix), data)
	err = errors.Wrapf(err, "failed to render template %s with data %#v", name, data)
	return
}
