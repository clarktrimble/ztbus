package entity

import (
	"encoding/json"
	"time"
)

type TsValue struct {
	Ts    time.Time `json:"ts"`
	Value float64   `json:"val"`
}

type TsValues []TsValue

func (vals TsValues) String() string {

	out, err := json.MarshalIndent(vals, "", "  ")
	if err != nil {
		return `{"error": "somehow failed to marshal ts-vals"}`
	}

	return string(out)
}
