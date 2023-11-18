// Package elastic chats with ES via it's json api.
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
	Idx    string
	Client Client
	tmpl   *template.Template
}

// New creates a new Elastic from Config.
func (cfg *Config) New(client Client, tmplFs fs.FS) (es *Elastic, err error) {

	tmpl := &template.Template{
		Path:   "es-tmpl",
		Suffix: "yaml",
		Left:   "<<",
		Right:  ">>",
		// Todo: need angle brackets here??
	}

	err = tmpl.Load(tmplFs)
	if err != nil {
		return
	}

	es = &Elastic{
		Idx:    cfg.Idx,
		Client: client,
		tmpl:   tmpl,
	}

	return
}

/*
// func (cfg *Config) New(client Client, lgr Logger) (svc *Svc, err error) {
func (es *Elastic) Load(tmplFs fs.FS) (err error) {

	err = es.tmpl.Load(tmplFs)
	if err != nil {
		return
	}

	return
}
*/

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
func (es *Elastic) Query(name string, data map[string]string) (query []byte, err error) {

	query, err = es.tmpl.RenderJson(name, data)
	return
}

func (es *Elastic) Search(ctx context.Context, query []byte) (result []byte, err error) {

	//if svc.DryRun {
	//svc.Logger.Info(ctx, "stopping short", "dry_run", svc.DryRun)
	//return
	//}

	path := fmt.Sprintf(searchPath, es.Idx)
	result, err = es.Client.SendJson(ctx, "GET", path, bytes.NewBuffer(query))
	return
}

/*
// AggAvgBody generates the agg request body.
func (es *Elastic) AggAvgBody(ctx context.Context, data map[string]string) (body []byte, err error) {

	//ar, err := newAggRequest("agg-avg", "qry-rng", data)
	ar, err := newAggRequest("agg-term-dthist-avg", "qry-rng", data)
	if err != nil {
		return
	}

	body, err = json.Marshal(ar)
	err = errors.Wrapf(err, `{"error": "somehow failed to marshal agg request body"}`)
	return
}

// Todo: name AggIntervalTermFloat or TermTsVal, TsTermVal?
func (es *Elastic) AggAvg(ctx context.Context, body []byte) (vals entity.TsVals, err error) {

	fmt.Printf(">>> body: \n%s\n\n", body)

	response, err := es.Client.SendJson(ctx, "GET", fmt.Sprintf(searchPath, es.Idx), bytes.NewBuffer(body))
	if err != nil {
		return
	}

	fmt.Printf(">>> response: \n%s\n\n", response)

	// pick over response, strongly coupled to agg template!!

	vals = entity.TsVals{}
	//for _, bkt1 := range gjson.GetBytes(response, "aggregations.outer.buckets").Array() {
	for _, bkt1 := range gjson.GetBytes(response, "aggregations.interval_agg.buckets").Array() {
		for _, bkt2 := range bkt1.Get("term_agg.buckets").Array() {

			ts := bkt1.Get("key").Int()
			term := bkt2.Get("key").String()
			value := bkt2.Get("float_agg.value").Float()
			fmt.Printf(">>> %d %s %f\n", ts, term, value)

				nxTs := bkt1.Get("key").Int()
				val := bkt1.Get("inner.value").Float()

				vals = append(vals, entity.TsValue{
					Ts:    time.UnixMilli(nxTs).UTC(),
					Value: val,
				})
		}
	}

	return
}

// AggAvgBody generates the agg request body.
func (es *Elastic) AggAvgBodyOld(ctx context.Context, data map[string]string) (body []byte, err error) {

	ar, err := newAggRequest("agg-avg", "qry-rng", data)
	if err != nil {
		return
	}

	body, err = json.Marshal(ar)
	err = errors.Wrapf(err, `{"error": "somehow failed to marshal agg request body"}`)
	return
}

// AggAvg gets average over an interval.
func (es *Elastic) AggAvgOld(ctx context.Context, body []byte) (vals entity.TsVals, err error) {

	fmt.Printf(">>> body: \n%s\n\n", body)

	response, err := es.Client.SendJson(ctx, "GET", fmt.Sprintf(searchPath, es.Idx), bytes.NewBuffer(body))
	if err != nil {
		return
	}

	fmt.Printf(">>> response: \n%s\n\n", response)

	// pick over response, strongly coupled to agg template!!

	vals = entity.TsVals{}
	for _, bkt1 := range gjson.GetBytes(response, "aggregations.outer.buckets").Array() {

		nxTs := bkt1.Get("key").Int()
		val := bkt1.Get("inner.value").Float()

		vals = append(vals, entity.TsVal{
			Ts:  time.UnixMilli(nxTs).UTC(),
			Val: val,
		})
	}

	return
}
*/

// unexported

type esResult struct {
	Result string `json:"result"`
}

type aggRequest struct {
	Aggs  json.RawMessage `json:"aggs"`
	Query json.RawMessage `json:"query"`
	Size  int             `json:"size"`
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
