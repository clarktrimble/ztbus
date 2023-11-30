// Package svc provides a home for ZTBus agg templates and the means to decode thier results.
// And a means to inset records in to the repo.
package svc

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"io"
	"time"

	"github.com/pkg/errors"
	"github.com/tidwall/gjson"

	"ztbus"
)

//go:embed *yaml
var TmplFs embed.FS

// Logger specifies the logger.
type Logger interface {
	Info(ctx context.Context, msg string, kv ...any)
	Error(ctx context.Context, msg string, err error, kv ...any)
}

// Repo specifies the data store.
type Repo interface {
	BulkInsert(ctx context.Context, chunk int, rdr io.Reader) (count int, skip [][]byte, err error)
	Query(name string, data map[string]string) (query []byte, err error)
	Search(ctx context.Context, query []byte) (result []byte, err error)
}

// Config represents config options for Svc.
type Config struct {
	DryRun bool `json:"dry_run" desc:"stop short of hitting repo"`
}

// Svc is the service layer.
type Svc struct {
	Repo   Repo
	Logger Logger
	DryRun bool
}

// New creates a new Svc from Config
func (cfg *Config) New(repo Repo, lgr Logger) *Svc {

	return &Svc{
		Repo:   repo,
		Logger: lgr,
		DryRun: cfg.DryRun,
	}
}

// AvgSpeed gets average speeds.
func (svc *Svc) AvgSpeed(ctx context.Context, data map[string]string) (avgs ztbus.AvgSpeeds, err error) {

	name := "avgspeed"
	query, err := svc.Repo.Query(name, data)
	// Todo: linter missed missing error check here?
	if err != nil {
		return
	}

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

	buf := &bytes.Buffer{}
	newline := []byte("\n")
	var data []byte

	for i := 0; i < ztc.Len; i++ {
		data, err = json.Marshal(ztc.Row(i))
		if err != nil {
			err = errors.Wrapf(err, "somehow failed to marshal row %d of ztbus cols", i)
			return
		}
		buf.Write(data)
		buf.Write(newline)
	}

	if svc.DryRun {
		svc.Logger.Info(ctx, "stopping short", "dry_run", svc.DryRun)
		// Todo: stdout
		return
	}

	start := time.Now()
	count, skip, err := svc.Repo.BulkInsert(ctx, 999, buf)
	if err != nil {
		return
	}
	if len(skip) != 0 {
		//err = errors.Errorf("oops")
		// Todo: nil err ??
		svc.Logger.Error(ctx, "unexpectedly skipped", nil, "skip", skip)
	}

	svc.Logger.Info(ctx, "insertion finished", "count", count, "elapsed", time.Since(start).Seconds())
	return
}
