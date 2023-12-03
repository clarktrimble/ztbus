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
	"ztbus/svc"
)

const (
	appId     string = "load-ztb"
	cfgPrefix string = "ztb"
	blerb     string = "'load-ztbus' parses a given ztbus csv and inserts the records to ES"
)

var (
	version string
)

type Config struct {
	Version  string          `json:"version" ignored:"true"`
	Logger   *sabot.Config   `json:"logger"`
	Client   *giant.Config   `json:"http_client"`
	Elastic  *elastic.Config `json:"es"`
	Svc      *svc.Config     `json:"ztb_svc"`
	DataPath string          `json:"data_path" desc:"path of ztbus data file for input" required:"true"`
}

func main() {

	// load config, setup logger

	cfg := &Config{Version: version}
	launch.Load(cfg, cfgPrefix, blerb)

	lgr := cfg.Logger.New(os.Stderr)
	ctx := lgr.WithFields(context.Background(), "app_id", appId, "run_id", hondo.Rand(7))
	lgr.Info(ctx, "starting up", "config", cfg)

	// setup service layer

	client := cfg.Client.NewWithTrippers(lgr)
	repo, err := cfg.Elastic.New(client, svc.TmplFs)
	launch.Check(ctx, lgr, err)

	ztbSvc := cfg.Svc.New(repo, lgr)

	// parse csv and insert records

	ztc, err := ztbus.New(cfg.DataPath)
	launch.Check(ctx, lgr, err)

	err = ztbSvc.CreateDocs(ctx, ztc)
	launch.Check(ctx, lgr, err)

	lgr.Info(ctx, "shutting down")
}
