// Package svc implements a service-layer between whomever wants to interact with repo and an http client.
package ztbsvc

import (
	"context"
	"embed"
	"io"
	"time"

	"ztbus"
	"ztbus/template"

	"github.com/tidwall/gjson"
)

// Todo: rename with es added

// injecting repo would be cool but ...

//go:embed es-tmpl/*
var TmplFs embed.FS

const (
	docPath    = "/%s/_doc"
	searchPath = "/%s/_search"
)

// Logger specifies the logger.
type Logger interface {
	Info(ctx context.Context, msg string, kv ...any)
	Error(ctx context.Context, msg string, err error, kv ...any)
}

// Client specifies an http client.
type Client interface {
	SendObject(ctx context.Context, method, path string, snd, rcv any) (err error)
	SendJson(ctx context.Context, method, path string, body io.Reader) (data []byte, err error)
}

// Repo specifies the data store.
type Repo interface {
	CreateDoc(ctx context.Context, doc any) (err error)
	//AggAvgBody(ctx context.Context, data map[string]string) (body []byte, err error)
	//AggAvg(ctx context.Context, body []byte) (vals entity.TsVals, err error)
	Query(name string, data map[string]string) (query []byte, err error)
	Search(ctx context.Context, query []byte) (result []byte, err error)
}

/*
	"interval": "5m",
	"bgn":      "2022-09-21T08:00:00Z",
	"end":      "2022-09-21T16:59:59.999Z",
*/

// Config represents config options for Svc.
type Config struct {
	//Idx    string `json:"es_index" desc:"es index name" required:"true"`
	DryRun bool `json:"dry_run" desc:"stop short of hitting repo"`
}

// Svc is the service layer.
type Svc struct {
	Repo   Repo
	Logger Logger
	//Client Client
	tmpl *template.Template
	//Idx    string
	DryRun bool
}

// New creates a new Svc from Config
func (cfg *Config) New(repo Repo, lgr Logger) *Svc {
	//func (cfg *Config) New(client Client, lgr Logger) (svc *Svc, err error) {

	/*
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
	*/

	return &Svc{
		DryRun: cfg.DryRun,
		//Idx:    cfg.Idx,
		Repo:   repo,
		Logger: lgr,
		//Client: client,
		//tmpl:   tmpl,
	}

}

/*
// Aggregate renders an agg by name and returns it and the corresponding result from ES.
func (svc *Svc) Aggregate(ctx context.Context, name string, data map[string]string) (agg, result []byte, err error) {

	// Todo: hmmm could pass  tmplFs to "elastic" and push all this there?
	agg, err = svc.tmpl.RenderJson(name, data)
	if err != nil {
		return
	}

	if svc.DryRun {
		svc.Logger.Info(ctx, "stopping short", "dry_run", svc.DryRun)
		return
	}

	//result, err = svc.Client.SendJson(ctx, "GET", fmt.Sprintf(searchPath, svc.Idx), bytes.NewBuffer(agg))
	result = []byte{}
	return
}

*/

// AvgSpeed gets average speeds.
func (svc *Svc) AvgSpeed(ctx context.Context, data map[string]string) (avgs ztbus.AvgSpeeds, err error) {

	name := "avgspeed"
	query, err := svc.Repo.Query(name, data)

	svc.Logger.Info(ctx, "sending query", "query", string(query))
	if svc.DryRun {
		svc.Logger.Info(ctx, "stopping short", "dry_run", svc.DryRun)
		return
	}

	result, err := svc.Repo.Search(ctx, query)
	if err != nil {
		return
	}

	// pick over response, strongly coupled to agg template!!

	avgs = ztbus.AvgSpeeds{}

	for _, bkt1 := range gjson.GetBytes(result, "aggregations.outer.buckets").Array() {
		for _, bkt2 := range bkt1.Get("middle.buckets").Array() {

			ts := bkt1.Get("key").Int()

			avgs = append(avgs, ztbus.AvgSpeed{
				Ts:           time.UnixMilli(ts).UTC(),
				BusId:        bkt2.Get("key").String(),
				VehicleSpeed: bkt2.Get("inner.value").Float(),
			})
		}
	}

	return
}

// CreateDocs inserts ztbus records into repo.
func (svc *Svc) CreateDocs(ctx context.Context, ztc *ztbus.ZtBusCols) (err error) {

	svc.Logger.Info(ctx, "inserting records", "count", ztc.Len)

	if svc.DryRun {
		svc.Logger.Info(ctx, "stopping short", "dry_run", svc.DryRun)
		return
	}

	start := time.Now()
	for i := 0; i < ztc.Len; i++ {
		err = svc.Repo.CreateDoc(ctx, ztc.Row(i))
		if err != nil {
			return
		}
	}
	svc.Logger.Info(ctx, "insertion finished", "elapsed", time.Since(start).Seconds())

	return
}

/*
// AggAvg gets average over an interval.
func (svc *Svc) AggAvg(ctx context.Context, data map[string]string) (vals entity.TsVals, err error) {

	// Todo: config datums, at least ntrvl, bgn, end

	datums := map[string]string{
		"ts_field":   "ts",
		"term_field": "bus_id",
		"data_field": "vehicle_speed",
		"interval":   "60m",
		"bgn":        "2022-09-21T08:00:00Z",
		"end":        "2022-09-21T16:59:59.999Z",
	}

	for key, val := range data {
		datums[key] = val
	}

		agg, err := svc.Repo.AggAvgBody(ctx, datums)
		if err != nil {
			return
		}

		svc.Logger.Info(ctx, "sending aggregation query to repo", "agg", string(agg))

		if svc.DryRun {
			svc.Logger.Info(ctx, "stopping short", "dry_run", svc.DryRun)
			return
		}

		vals, err = svc.Repo.AggAvg(ctx, agg)
	return
}
*/
