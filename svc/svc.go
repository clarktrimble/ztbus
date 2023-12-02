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
	"ztbus/elastic"

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
	Query(name string, data map[string]string) (query []byte, err error)
	Search(ctx context.Context, query []byte) (result []byte, err error)
	PostBulk(ctx context.Context, data io.Reader) (err error)
}

// Config represents config options for Svc.
type Config struct {
	Chunk  int  `json:"chunk_size" desc:"number of records per chunk to insert" default:"999"`
	DryRun bool `json:"dry_run" desc:"stop short of hitting repo"`
}

// Svc is the service layer.
type Svc struct {
	Repo   Repo
	Logger Logger
	Chunk  int
	DryRun bool
}

// New creates a new Svc from Config
func (cfg *Config) New(repo Repo, lgr Logger) *Svc {

	return &Svc{
		Repo:   repo,
		Logger: lgr,
		Chunk:  cfg.Chunk,
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
	start := time.Now()

	// serialize ztbus data to buffer

	buf := &bytes.Buffer{}
	var data []byte

	for i := 0; i < ztc.Len; i++ {
		data, err = json.Marshal(ztc.Row(i))
		if err != nil {
			err = errors.Wrapf(err, "somehow failed to marshal row %d of ztbus cols", i)
			return
		}
		buf.Write(data)
		buf.Write([]byte("\n"))
	}

	if svc.DryRun {
		svc.Logger.Info(ctx, "stopping short", "dry_run", svc.DryRun)
		return
	}

	// chunk them in

	bi := elastic.NewBulki(svc.Chunk, buf)
	for bi.Next() {

		err = svc.Repo.PostBulk(ctx, bi.Value())
		if err != nil {
			return
		}
	}
	err = bi.Err()
	if err != nil {
		return
	}

	for _, line := range bi.Skipped() {
		svc.Logger.Error(ctx, "unexpectedly skipped", nil, "line", string(line))
	}

	svc.Logger.Info(ctx, "insertion finished", "count", bi.Count(), "elapsed", time.Since(start).Seconds())
	return
}
