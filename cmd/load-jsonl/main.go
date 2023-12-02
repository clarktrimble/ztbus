// Package main inserts json lines into ES.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/clarktrimble/giant"
	"github.com/clarktrimble/giant/basicrt"
	"github.com/clarktrimble/giant/statusrt"
	"github.com/clarktrimble/launch"
	"github.com/clarktrimble/launch/spinner"

	"ztbus/elastic"
)

const (
	cfgPrefix string = "jles"
	blerb     string = "'load-jsonl' scans json lines from stdin and inserts each into ES"
)

type Config struct {
	Client  *giant.Config   `json:"http_client"`
	Elastic *elastic.Config `json:"es"`
	Chunk   int             `json:"chunk_size" desc:"number of records per chunk to insert" default:"999"`
}

func main() {

	// load cfg and setup elastic

	cfg := &Config{}
	launch.Load(cfg, cfgPrefix, blerb)

	client := cfg.Client.New()
	client.Use(&statusrt.StatusRt{})

	basicRt := basicrt.New(cfg.Client.User, string(cfg.Client.Pass))
	client.Use(basicRt)

	// Todo: maybe do want loggingRt?
	// Todo: document more curl against ES

	ctx := context.Background()
	es := &elastic.Elastic{
		Client: client,
		Idx:    cfg.Elastic.Idx,
	}

	// chunk thru input and post

	sp := spinner.New()
	bi := elastic.NewBulki(cfg.Chunk, os.Stdin)

	for bi.Next() {
		err := es.PostBulk(ctx, bi.Value())
		launch.Check(ctx, nil, err)
		sp.Spin()
	}
	launch.Check(ctx, nil, bi.Err())

	for _, line := range bi.Skipped() {
		fmt.Printf(">>> skipped: %s\n", line)
	}

	fmt.Printf("%d records inserted in %.2f seconds over %d chunks\n", bi.Count(), sp.Elapsed(), sp.Count)
}
