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

	"ztbus/elastic"
)

const (
	cfgPrefix string = "jles"
	blerb     string = "'load-jsonl' scans json lines from stdin and inserts each into ES"
)

type Config struct {
	Client  *giant.Config   `json:"http_client"`
	Elastic *elastic.Config `json:"es"`
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

	// Todo: config chunk size
	count, skip, err := es.BulkInsert(ctx, 3, os.Stdin)
	launch.Check(ctx, nil, err)

	for _, line := range skip {
		fmt.Printf(">>> skipped: %s\n", line)
	}

	fmt.Printf(">>> count: %d\n", count)

	// scan stdin for json lines and send to ES

	/*
		sp := spinner.New()
		scanner := bufio.NewScanner(os.Stdin)

		for scanner.Scan() {

			err := es.InsertRaw(ctx, scanner.Bytes())
			if err != nil {
				fmt.Fprintln(os.Stderr, "error inserting to es:", err)
				os.Exit(1)
			}

			sp.Spin()
		}

		err := scanner.Err()
		if err != nil {
			fmt.Fprintln(os.Stderr, "error reading standard input:", err)
			os.Exit(1)
		}

		fmt.Printf("%d records inserted in %.2f seconds\n", sp.Count, sp.Elapsed())
	*/
}
