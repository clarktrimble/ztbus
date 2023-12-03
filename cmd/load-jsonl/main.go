// Package main inserts json lines into ES.
package main

import (
	"context"
	"os"

	"github.com/clarktrimble/giant"
	"github.com/clarktrimble/hondo"
	"github.com/clarktrimble/launch"
	"github.com/clarktrimble/launch/spinner"
	"github.com/clarktrimble/sabot"

	"ztbus/elastic"
)

const (
	cfgPrefix string = "jles"
	blerb     string = "'load-jsonl' scans json lines from stdin and inserts each into ES"
)

var (
	version string
)

type Config struct {
	Version string          `json:"version" ignored:"true"`
	Logger  *sabot.Config   `json:"logger"`
	Client  *giant.Config   `json:"http_client"`
	Elastic *elastic.Config `json:"es"`
	Chunk   int             `json:"chunk_size" desc:"number of records per chunk to insert" default:"999"`
}

func main() {

	// load cfg and setup elastic

	cfg := &Config{Version: version}
	launch.Load(cfg, cfgPrefix, blerb)

	lgr := cfg.Logger.New(os.Stderr)
	ctx := lgr.WithFields(context.Background(), "run_id", hondo.Rand(7))
	lgr.Info(ctx, "starting up", "config", cfg)

	es := &elastic.Elastic{
		Client: cfg.Client.NewWithTrippers(lgr),
		Idx:    cfg.Elastic.Idx,
	}

	// chunk thru input and post

	sp := spinner.New()
	bi := elastic.NewBulki(cfg.Chunk, os.Stdin)

	for bi.Next() {
		err := es.PostBulk(ctx, bi.Value())
		launch.Check(ctx, lgr, err)
		sp.Spin()
	}
	launch.Check(ctx, lgr, bi.Err())

	for _, line := range bi.Skipped() {
		lgr.Info(ctx, "skipped", "line", string(line))
	}

	lgr.Info(ctx, "insertion finished",
		"doc_count", bi.Count(),
		"chunk_count", sp.Count,
		"elapsed", sp.Elapsed(),
	)
}
