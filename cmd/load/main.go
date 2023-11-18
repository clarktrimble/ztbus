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
	"ztbus/elastic"
	"ztbus/ztbsvc"
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
	DataPath string          `json:"data_path" desc:"path ztbus data file for input, skip agg if present"`
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
	//repo := cfg.Elastic.New(client)
	//ztbSvc := cfg.Svc.New(repo, lgr)
	ztbSvc, err := cfg.Svc.New(client, lgr)
	launch.Check(ctx, lgr, err)

	// parse csv and insert records

	ztc, err := ztbus.New(cfg.DataPath)
	launch.Check(ctx, lgr, err)

	err = ztbSvc.CreateDocs(ctx, ztc)
	launch.Check(ctx, lgr, err)
}
