// Package main aggregates ZTBus data from ES.
package main

import (
	"context"
	"fmt"
	"os"
	"ztbus/elastic"
	"ztbus/ztbsvc"

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
	Svc      *ztbsvc.Config  `json:"ztb_svc"`
	Truncate int             `json:"truncate" desc:"truncate log fields beyond length"`
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
	repo := cfg.Elastic.New(client)
	ztbSvc := cfg.Svc.New(repo, lgr)

	// run the agg, yay!

	vals, err := ztbSvc.AggAvg(ctx, map[string]string{})
	launch.Check(ctx, lgr, err)

	fmt.Println(vals)
}
