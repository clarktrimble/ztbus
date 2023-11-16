// Package main parses a ZTBus csv file and inserts those records into ES.
package main

import (
	"context"
	"os"

	"github.com/clarktrimble/giant"
	"github.com/clarktrimble/hondo"
	"github.com/clarktrimble/launch"
	"github.com/clarktrimble/sabot"

	"ztbus"
	"ztbus/svc"
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
	Version  string        `json:"version" ignored:"true"`
	User     string        `json:"es_username" required:"true"`
	Pass     launch.Redact `json:"es_password" required:"true"`
	Client   *giant.Config `json:"http_client"`
	Svc      *svc.Config   `json:"svc"`
	Truncate int           `json:"truncate" desc:"truncate log fields beyond length"`
	DataPath string        `json:"data_path" desc:"path ztbus data file for input, skip agg if present"`
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
	docSvc := cfg.Svc.New(client, lgr)

	// parse csv and insert records

	ztc, err := ztbus.New(cfg.DataPath)
	launch.Check(ctx, lgr, err)

	err = docSvc.CreateDocs(ctx, ztc)
	launch.Check(ctx, lgr, err)
}
