// Package main aggregates ZTBus data from ES.
package main

import (
	"context"
	"fmt"
	"os"
	"ztbus/elastic"
	"ztbus/svc"

	"github.com/clarktrimble/giant"
	"github.com/clarktrimble/hondo"
	"github.com/clarktrimble/launch"
	"github.com/clarktrimble/sabot"
)

const (
	cfgPrefix string = "ztb"
)

var (
	version string
)

type Config struct {
	Version  string          `json:"version" ignored:"true"`
	Client   *giant.Config   `json:"http_client"`
	Elastic  *elastic.Config `json:"es"`
	Svc      *svc.Config     `json:"ztb_svc"`
	Truncate int             `json:"truncate" desc:"truncate log fields beyond length"`
	Interval string          `json:"agg_interval" desc:"aggregation interval" default:"5m"`
	Bgn      string          `json:"agg_start" desc:"aggregation start time" default:"2022-09-21T08:00:00Z"`
	End      string          `json:"agg_end" desc:"aggregation end time" default:"2022-09-21T16:59:59.999Z"`
}

func main() {

	// load config, setup logger

	cfg := &Config{Version: version}
	launch.Load(cfg, cfgPrefix)

	lgr := &sabot.Sabot{Writer: os.Stderr, MaxLen: cfg.Truncate}
	ctx := lgr.WithFields(context.Background(), "run_id", hondo.Rand(7))
	lgr.Info(ctx, "starting up", "config", cfg)

	// setup service layer

	client := cfg.Client.NewWithTrippers(lgr)
	repo, err := cfg.Elastic.New(client, svc.TmplFs)
	launch.Check(ctx, lgr, err)

	ztbSvc := cfg.Svc.New(repo, lgr)

	// run the agg, yay!

	avgs, err := ztbSvc.AvgSpeed(ctx, map[string]string{
		"interval": cfg.Interval,
		"bgn":      cfg.Bgn,
		"end":      cfg.End,
	})
	launch.Check(ctx, lgr, err)

	fmt.Printf("%s\n", avgs)
}
