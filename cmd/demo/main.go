// Package main dumps rendered agg, response from ES, and any error as json.
package main

import (
	"context"
	"fmt"
	"os"
	"ztbus/elastic"
	"ztbus/ztbsvc"

	"github.com/clarktrimble/giant"
	"github.com/clarktrimble/giant/basicrt"
	"github.com/clarktrimble/giant/logrt"
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
	Version string          `json:"version" ignored:"true"`
	Client  *giant.Config   `json:"http_client"`
	Elastic *elastic.Config `json:"es"`
	//Svc      *ztbsvc.Config `json:"ztb_svc"`
	Truncate int    `json:"truncate" desc:"truncate log fields beyond length"`
	Interval string `json:"agg_interval" desc:"aggregation interval" default:"5m"`
	Bgn      string `json:"agg_start" desc:"aggregation start time" default:"2022-09-21T08:00:00Z"`
	End      string `json:"agg_end" desc:"aggregation end time" default:"2022-09-21T16:59:59.999Z"`
}

func main() {

	// load config, setup logger

	cfg := &Config{Version: version}
	launch.Load(cfg, cfgPrefix)

	lgr := &sabot.Sabot{Writer: os.Stderr, MaxLen: cfg.Truncate}
	ctx := lgr.WithFields(context.Background(), "run_id", hondo.Rand(7))
	lgr.Info(ctx, "starting up", "config", cfg)

	// setup service layer

	// don't want StatusRt so we can see errors from elastic
	client := cfg.Client.New()
	client.Use(&logrt.LogRt{Logger: lgr})

	basicRt := basicrt.New(cfg.Client.User, string(cfg.Client.Pass))
	client.Use(basicRt)

	repo, err := cfg.Elastic.New(client, ztbsvc.TmplFs)
	//ztbSvc, err := cfg.Svc.New(client, lgr)
	launch.Check(ctx, lgr, err)

	// run the agg, yay!

	//agg, result, err := ztbSvc.Aggregate(ctx, "avgspeed", map[string]string{
	agg, err := repo.Query("avgspeed", map[string]string{
		"interval": cfg.Interval,
		"bgn":      cfg.Bgn,
		"end":      cfg.End,
	})

	result := []byte{}
	if err == nil {
		result, err = repo.Search(ctx, agg)
	}

	// dump everything in format hopefully digestable to the inestimable "jq"

	if len(agg) == 0 {
		agg = []byte(`"none"`)
	}
	if len(result) == 0 {
		result = []byte(`"none"`)
	}
	if err == nil {
		err = fmt.Errorf("none")
	}

	fmt.Printf(`{"request": %s, "response": %s, "error": "%s"}`, agg, result, err)

}
