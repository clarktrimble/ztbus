package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/clarktrimble/giant"
	"github.com/clarktrimble/giant/basicrt"
	"github.com/clarktrimble/giant/logrt"
	"github.com/clarktrimble/giant/statusrt"
	"github.com/clarktrimble/hondo"
	"github.com/clarktrimble/launch"
	"github.com/clarktrimble/sabot"

	"ztbus"
	"ztbus/svc"
)

// Todo: pkg docstr
// Todo: multibus!!

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
	Truncate int           `json:"truncate" desc:"truncate log fields beyond length"`
	DataPath string        `json:"data_path" desc:"path ztbus data file for input, skip agg if present"`
	DryRun   bool          `json:"dry_run" desc:"parse input data file, but don't post"`
}

func main() {

	// load config

	cfg := &Config{Version: version}
	launch.Load(cfg, cfgPrefix)
	// Todo: log loaded config
	// Todo: push logs to ES and show off??

	lgr := &sabot.Sabot{Writer: os.Stdout, MaxLen: cfg.Truncate}
	ctx := lgr.WithFields(context.Background(), "run_id", hondo.Rand(7))

	// setup service layer

	basicRt := basicrt.New(cfg.User, string(cfg.Pass))

	client := cfg.Client.New()
	client.Use(&statusrt.StatusRt{})
	client.Use(&logrt.LogRt{Logger: lgr})
	client.Use(basicRt)
	// 2023/11/12 10:39:33 RoundTripper returned a response & error; ignoring response
	// Todo: yeah fix ^^^ see: https://github.com/golang/go/issues/7620

	docSvc := &svc.Svc{
		Idx:    "ztbus001",
		Client: client,
	}

	// Todo: drop index feature ?? or curl
	// Todo: maybe two cmd's ??

	// insert records or aggregate

	if cfg.DataPath != "" {
		ztc, err := ztbus.New(cfg.DataPath)
		launch.Check(ctx, lgr, err)

		// Todo: move to svc

		lgr.Info(ctx, "inserting records", "count", ztc.Len, "index", docSvc.Idx)

		if cfg.DryRun {
			lgr.Info(ctx, "just kidding", "dry_run", cfg.DryRun)
			return
		}

		start := time.Now()
		for i := 0; i < ztc.Len; i++ {
			err = docSvc.CreateDoc(ctx, ztc.Row(i))
			launch.Check(ctx, lgr, err)
		}

		lgr.Info(ctx, "insertion finished", "elapsed", time.Since(start).Seconds())
		return
	}

	spds, err := docSvc.AggAvg(ctx, map[string]string{
		"ts_field":   "ts",
		"data_field": "vehicle_speed",
		"interval":   "60m",
		"bgn":        "2019-06-24T08:00:00Z",
		"end":        "2019-06-24T17:59:59.999Z",
	})
	launch.Check(ctx, lgr, err)

	fmt.Printf("%s\n", spds)
}
