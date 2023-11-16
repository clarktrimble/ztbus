// Package elastic chats with ES via it's json api.
package elastic

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
	"ztbus/entity"

	"github.com/pkg/errors"
	"github.com/tidwall/gjson"
)

// Todo: dump query mode

const (
	docPath    = "/%s/_doc"
	searchPath = "/%s/_search"
)

// Client specifies an http client.
type Client interface {
	SendObject(ctx context.Context, method, path string, snd, rcv any) (err error)
}

// Config is configurables.
type Config struct {
	Idx string `json:"es_index" desc:"es index name" required:"true"`
}

// Elastic is the service layer.
type Elastic struct {
	Idx    string
	Client Client
}

// New creates a new Svc from Config
func (cfg *Config) New(client Client) *Elastic {

	return &Elastic{
		Idx:    cfg.Idx,
		Client: client,
	}
}

// CreateDoc inserts a document.
func (es *Elastic) CreateDoc(ctx context.Context, doc any) (err error) {

	result := esResult{}

	err = es.Client.SendObject(ctx, "POST", fmt.Sprintf(docPath, es.Idx), doc, &result)
	if err != nil {
		return
	}

	if result.Result != "created" {
		err = errors.Errorf("unexpected result from es: %s", result.Result)
	}
	return
}

// AggAvgBody provides a copy of the agg request body for sanity-checks, debug, etc.
func (es *Elastic) AggAvgBody(ctx context.Context, data map[string]string) (body string, err error) {

	request, err := newAggRequest("agg-avg", "qry-rng", data)
	if err != nil {
		return
	}

	//out, err := json.MarshalIndent(request, "", "  ")
	//if err != nil {
	//out = []byte(`{"error": "somehow failed to marshal agg request body"}`)
	//}

	body = request.String()
	return
}

// AggAvg gets average over an interval.
func (es *Elastic) AggAvg(ctx context.Context, data map[string]string, dr bool) (body string, vals entity.TsValues, err error) {
	//func (es *Elastic) AggAvg(ctx context.Context, data map[string]string) (vals entity.TsValues, err error) {

	// form up an agg request and send it off

	request, err := newAggRequest("agg-avg", "qry-rng", data)
	if err != nil {
		return
	}
	body = request.String()

	if dr {
		return
	}

	response := map[string]json.RawMessage{}
	err = es.Client.SendObject(ctx, "GET", fmt.Sprintf(searchPath, es.Idx), request, &response)
	if err != nil {
		return
	}

	// pick over response, strongly coupled to agg template!!

	vals = entity.TsValues{}
	for _, bkt1 := range gjson.GetBytes(response["aggregations"], "outer.buckets").Array() {

		nxTs := bkt1.Get("key").Int()
		val := bkt1.Get("inner.value").Float()

		vals = append(vals, entity.TsValue{
			Ts:    time.UnixMilli(nxTs).UTC(),
			Value: val,
		})
	}

	return
}

// unexported

type esResult struct {
	Result string `json:"result"`
}

type aggRequest struct {
	Aggs  json.RawMessage `json:"aggs"`
	Query json.RawMessage `json:"query"`
	Size  int             `json:"size"`
}

func (ar aggRequest) String() string {

	out, err := json.MarshalIndent(ar, "", "  ")
	if err != nil {
		return `{"error": "somehow failed to marshal agg request body"}`
	}

	return string(out)
}

func newAggRequest(aggName, qryName string, data map[string]string) (request aggRequest, err error) {

	// check that tmpls loaded via init w/o issue

	if loadErr != nil {
		err = loadErr
		return
	}

	// render

	agg, err := templates.RenderJson(aggName, data)
	if err != nil {
		return
	}

	qry, err := templates.RenderJson(qryName, data)
	if err != nil {
		return
	}

	// and assemble

	request = aggRequest{
		Aggs:  agg,
		Query: qry,
		Size:  0,
	}

	return
}
