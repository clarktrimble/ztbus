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

// Todo: round corners and modularize!?

// Template is template configurables and a handle to the top-level template.
type Template struct {
	Path   string
	Suffix string
	Left   string
	Right  string
	tmpl   *template.Template
}

// Load reads and parses templates from the given filesystem.
// Templates are named after file in which found.
func (tmpl *Template) Load(fs fs.FS) (err error) {

	tt := template.New("top").Delims(tmpl.Left, tmpl.Right)
	tt = tt.Funcs(template.FuncMap{"quoteComma": quoteComma}) // Todo: whafor? always??

	glob := fmt.Sprintf("%s/*.%s", tmpl.Path, tmpl.Suffix)
	tt, err = tt.ParseFS(fs, glob)
	if err != nil {
		err = errors.Wrapf(err, "failed to parse templates with pattern: %s and delims %s %s", glob, tmpl.Left, tmpl.Right)
		return
	}

	tmpl.tmpl = tt
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

// Render renders a template.
func (tmpl *Template) Render(name string, data interface{}) (out io.Reader, err error) {

	// Todo: want this'n ??

	buf := &bytes.Buffer{}

	err = tmpl.render(name, data, buf)
	if err != nil {
		return
	}

	out = buf
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

	if tmpl.tmpl == nil {
		err = errors.Errorf("no templates loaded for: %#v", tmpl)
		return
	}

	err = tmpl.tmpl.ExecuteTemplate(writer, fmt.Sprintf("%s.%s", name, tmpl.Suffix), data)
	err = errors.Wrapf(err, "failed to render template %s with data %#v", name, data)
	return
}

func quoteComma(items []string) (new string) {

	// Todo: doc!!!!

	quoted := []string{}
	for _, item := range items {
		quoted = append(quoted, fmt.Sprintf("'%s'", strings.Trim(item, "', ")))
	}

	new = strings.Join(quoted, ", ")
	return
}
