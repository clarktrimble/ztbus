package entity

import (
	"encoding/json"
	"time"
)

type TsVal struct {
	Ts  time.Time `json:"ts"`
	Val float64   `json:"val"`
}

type TsVals []TsVal

func (vals TsVals) String() string {

	out, err := json.MarshalIndent(vals, "", "  ")
	if err != nil {
		return `{"error": "somehow failed to marshal ts-vals"}`
	}

	return string(out)
}

// Todo: use "Val" in naming

type TermTsVals struct {
	Term   string `json:"term"`
	TsVals TsVals `json:"ts_vals"`
}
