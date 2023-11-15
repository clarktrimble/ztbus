package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/clarktrimble/giant"
	"github.com/clarktrimble/giant/logrt"
	"github.com/clarktrimble/giant/statusrt"
	"github.com/clarktrimble/launch"

	"ztbus"
	"ztbus/minlog"
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
	Pass     string        `json:"es password" required:"true"` // Todo: redact from cfg, hdr
	Client   *giant.Config `json:"http_client"`
	DataPath string        `json:"data_path" desc:"path ztbus data file for input, skip agg if present"`
	DryRun   bool          `json:"dry_run" desc:"parse input data file, but don't post"`
}

func main() {

	// load config

	cfg := &Config{Version: version}
	launch.Load(cfg, cfgPrefix)

	// Todo: make better
	msg := fmt.Sprintf("%s:%s", cfg.User, cfg.Pass)
	encoded := base64.StdEncoding.EncodeToString([]byte(msg))
	auth := fmt.Sprintf("Basic %s", encoded)

	// Todo: wring hands about map exists
	// Todo: investigate Headers cfgble
	//cfg.Client.Headers["Authorization"] = auth
	cfg.Client.Headers = map[string]string{"Authorization": auth}

	// setup service layer

	ctx := context.Background()
	lgr := &minlog.MinLog{} // Todo: use sabot

	client := cfg.Client.New()
	client.Use(&statusrt.StatusRt{})
	client.Use(&logrt.LogRt{Logger: lgr})
	// 2023/11/12 10:39:33 RoundTripper returned a response & error; ignoring response
	// Todo: wah ^^^ ?

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
