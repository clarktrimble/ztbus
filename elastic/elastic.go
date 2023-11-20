// Package elastic inserts documents and queries ES via it's json api.
// Queries can be rendered from yaml templates.
package elastic

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"

	"github.com/pkg/errors"

	"ztbus/template"
)

const (
	docPath    = "/%s/_doc"
	searchPath = "/%s/_search"
)

// Client specifies an http client.
type Client interface {
	SendObject(ctx context.Context, method, path string, snd, rcv any) (err error)
	SendJson(ctx context.Context, method, path string, body io.Reader) (data []byte, err error)
}

// Config represents config options for Elastic.
type Config struct {
	Idx string `json:"es_index" desc:"es index name" required:"true"`
}

// Elastic is an ES json api client.
type Elastic struct {
	Client Client
	Idx    string
	tmpl   *template.Template
}

// New creates a new Elastic from Config, loading query templates.
func (cfg *Config) New(client Client, tmplFs fs.FS) (es *Elastic, err error) {

	tmpl := &template.Template{
		Suffix: "yaml",
	}

	err = tmpl.Load(tmplFs)
	if err != nil {
		return
	}

	es = &Elastic{
		Client: client,
		Idx:    cfg.Idx,
		tmpl:   tmpl,
	}

	return
}

// Insert inserts a document.
func (es *Elastic) Insert(ctx context.Context, doc any) (err error) {

	result := esResult{}

	path := fmt.Sprintf(docPath, es.Idx)
	err = es.Client.SendObject(ctx, "POST", path, doc, &result)
	if err != nil {
		return
	}

	if result.Result != "created" {
		err = errors.Errorf("unexpected result from es: %#v", result.Result)
	}
	return
}

// InsertRaw inserts a raw json document.
func (es *Elastic) InsertRaw(ctx context.Context, raw []byte) (err error) {

	result := esResult{}

	path := fmt.Sprintf(docPath, es.Idx)
	response, err := es.Client.SendJson(ctx, "POST", path, bytes.NewBuffer(raw))
	if err != nil {
		return
	}

	err = json.Unmarshal(response, &result)
	if err != nil {
		err = errors.Wrapf(err, "failed to unmarshal repsonse from es")
		return
	}

	if result.Result != "created" {
		err = errors.Errorf("unexpected result from es: %s", response)
	}
	return
}

// Query renders a query.
func (es *Elastic) Query(name string, data map[string]string) (query []byte, err error) {

	query, err = es.tmpl.RenderJson(name, data)
	return
}

// Search sends a query to ES.
func (es *Elastic) Search(ctx context.Context, query []byte) (response []byte, err error) {

	path := fmt.Sprintf(searchPath, es.Idx)
	response, err = es.Client.SendJson(ctx, "GET", path, bytes.NewBuffer(query))
	return
}

// unexported

type esResult struct {
	Result string `json:"result"`
}
