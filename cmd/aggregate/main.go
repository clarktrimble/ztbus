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

// Todo: push logs to ES and show off??
// Todo: update from branchy giant mod

const (
	cfgPrefix string = "ztb"
)

var (
	version string
)

type Config struct {
	Version  string          `json:"version" ignored:"true"`
	User     string          `json:"es_username" required:"true"`
	Pass     launch.Redact   `json:"es_password" required:"true"`
	Client   *giant.Config   `json:"http_client"`
	Svc      *svc.Config     `json:"svc"`
	Elastic  *elastic.Config `json:"es"`
	Truncate int             `json:"truncate" desc:"truncate log fields beyond length"`
}

func main() {

	// load config

	cfg := &Config{Version: version}
	launch.Load(cfg, cfgPrefix)

	lgr := &sabot.Sabot{Writer: os.Stderr, MaxLen: cfg.Truncate}
	ctx := lgr.WithFields(context.Background(), "run_id", hondo.Rand(7))
	lgr.Info(ctx, "starting up", "config", cfg)

	// setup service layer

	client := cfg.Client.NewWithTrippers(cfg.User, string(cfg.Pass), lgr)
	repo := cfg.Elastic.New(client)
	docSvc := cfg.Svc.New(repo, lgr)

	// run the agg, yay!

	vals, err := docSvc.AggAvg(ctx, map[string]string{})
	launch.Check(ctx, lgr, err) // Todo: demo error!

	fmt.Println(vals)
}
