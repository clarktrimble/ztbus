// Package svc implements a service-layer between whomever wants to interact with repo and an http client.
package svc

// Todo: re-name ??

import (
	"context"
	"time"
	"ztbus"
	"ztbus/entity"
)

// Logger specifies the logger.
type Logger interface {
	Info(ctx context.Context, msg string, kv ...any)
	Error(ctx context.Context, msg string, err error, kv ...any)
}

// Repo specifies the data store.
type Repo interface {
	CreateDoc(ctx context.Context, doc any) (err error)
	AggAvgBody(ctx context.Context, data map[string]string) (body []byte, err error)
	AggAvg(ctx context.Context, body []byte) (vals entity.TsValues, err error)
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
		DryRun: cfg.DryRun,
		Repo:   repo,
		Logger: lgr,
	}
}

// CreateDocs inserts ztbus records into repo.
func (svc *Svc) CreateDocs(ctx context.Context, ztc *ztbus.ZtBusCols) (err error) {

	svc.Logger.Info(ctx, "inserting records", "count", ztc.Len)

	if svc.DryRun {
		svc.Logger.Info(ctx, "just kidding", "dry_run", svc.DryRun)
		return
	}

	start := time.Now()
	for i := 0; i < 9; i++ {
		//for i := 0; i < ztc.Len; i++ {
		// Todo: un gimp
		err = svc.Repo.CreateDoc(ctx, ztc.Row(i))
		if err != nil {
			return
		}
	}

	svc.Logger.Info(ctx, "insertion finished", "elapsed", time.Since(start).Seconds())
	return
}

// AggAvg gets average over an interval.
func (svc *Svc) AggAvg(ctx context.Context, data map[string]string) (vals entity.TsValues, err error) {

	// Todo: config datums ??

	datums := map[string]string{
		"ts_field":   "ts",
		"data_field": "vehicle_speed",
		"interval":   "60m",
		"bgn":        "2019-06-24T08:00:00Z",
		"end":        "2019-06-24T17:59:59.999Z",
	}

	for key, val := range data {
		datums[key] = val
	}

	agg, err := svc.Repo.AggAvgBody(ctx, data)
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
