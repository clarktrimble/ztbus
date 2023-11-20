// Package main inserts json lines into ES.
package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"time"

	"github.com/clarktrimble/giant"
	"github.com/clarktrimble/giant/basicrt"
	"github.com/clarktrimble/launch"

	"ztbus/elastic"
)

const (
	cfgPrefix string = "jles"
)

type Config struct {
	Client  *giant.Config   `json:"http_client"`
	Elastic *elastic.Config `json:"es"`
}

func main() {

	// load cfg and setup elastic

	cfg := &Config{}
	launch.Load(cfg, cfgPrefix)

	client := cfg.Client.New()

	basicRt := basicrt.New(cfg.Client.User, string(cfg.Client.Pass))
	client.Use(basicRt)

	ctx := context.Background()
	es := &elastic.Elastic{
		Client: client,
		Idx:    cfg.Elastic.Idx,
	}

	// scan stdin for json lines and send to ES

	sp := &Spinner{Chars: []string{`-`, `\`, `|`, `/`}}
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
}

// Spinner cycles chars on term to indicate activity.
// Todo: truely minimally viable mod? or tuck into launch
type Spinner struct {
	Chars []string
	Start time.Time
	Count int
}

// Spin prints the next character.
func (sp *Spinner) Spin() {

	if sp.Start.IsZero() {
		sp.Start = time.Now()
	}

	fmt.Printf(" %s \r", sp.Chars[sp.Count%len(sp.Chars)])
	sp.Count++
}

// Elapsed reports seconds since the first Spin.
func (sp *Spinner) Elapsed() (secs float64) {

	return time.Since(sp.Start).Seconds()
}
