// Package elastic inserts documents and queries ES via it's json api.
// Queries can be rendered from yaml templates.
package elastic

//go:generate moq -pkg mock -out mock/mock.go . Client

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

// Todo: think about generating id's from select doc fields for upsert
//       perhaps via hash/fnv ? yeah and use create for error on dup?
//       b-but what about replay for stragglers?

const (
	docPath    = "/%s/_doc"
	searchPath = "/%s/_search"
	bulkPath   = "/%s/_bulk"
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
	tmpl   *template.Template // Todo: why private, cannot test??
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

	// Todo: doc would be more better as map[string]any yeah?? or too confining

	result := DocResult{}

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

// PostBulk posts to the ES Bulk api.
func (es *Elastic) PostBulk(ctx context.Context, data io.Reader) (err error) {

	path := fmt.Sprintf(bulkPath, es.Idx)

	response, err := es.Client.SendJson(ctx, "POST", path, data)
	if err != nil {
		return
	}

	result := bulkResult{}
	err = json.Unmarshal(response, &result)
	if err != nil {
		err = errors.Wrapf(err, "failed to unmarshal repsonse from es")
		return
	}
	if result.Errors {
		err = errors.Errorf("failed to post bulk to ES, got: %s", response)
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

	// Todo: could query be reader already from tmpl?

	path := fmt.Sprintf(searchPath, es.Idx)
	response, err = es.Client.SendJson(ctx, "GET", path, bytes.NewBuffer(query))
	return
}

// unexported

type DocResult struct {
	Result string `json:"result"`
}

// Todo: pub for unit?
type bulkResult struct {
	Errors bool `json:"errors"`
	// ignoring "items"
}

/*
	Items  []struct {
		Index struct {
			ID     string `json:"_id"`
			Result string `json:"result"`
			Status int    `json:"status"`
			Error  struct {
				Type   string `json:"type"`
				Reason string `json:"reason"`
				Cause  struct {
					Type   string `json:"type"`
					Reason string `json:"reason"`
				} `json:"caused_by"`
			} `json:"error"`
		} `json:"index"`
	} `json:"items"`
}
*/
