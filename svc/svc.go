// Package svc implements a service-layer between whomever wants to interact with Elastic
// and an http client.
package svc

// Todo: re-name ??
// Todo: wring hands over old yaml libs??

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/tidwall/gjson"
)

const (
	docPath    = "/%s/_doc"
	searchPath = "/%s/_search"
)

// Client specifies the http client.
type Client interface {
	SendObject(ctx context.Context, method, path string, snd, rcv any) (err error)
}

// Svc is the service layer.
type Svc struct {
	Idx    string
	Client Client
}

// TsValue is entity??
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

// CreateDoc inserts a document.
func (svc *Svc) CreateDoc(ctx context.Context, doc any) (err error) {

	result := esResult{}

	err = svc.Client.SendObject(ctx, "POST", fmt.Sprintf(docPath, svc.Idx), doc, &result)
	if err != nil {
		return
	}

	if result.Result != "created" {
		err = errors.Errorf("unexpected result from es: %s", result.Result)
	}
	return
}

// Todo: dump query mode

// AggAvg gets average over an interval.
func (svc *Svc) AggAvg(ctx context.Context, data map[string]string) (vals TsValues, err error) {

	// form up a request and send it off

	request, err := newAggRequest("agg-avg", "qry-rng", data)
	if err != nil {
		return
	}

	response := map[string]json.RawMessage{}
	err = svc.Client.SendObject(ctx, "GET", fmt.Sprintf(searchPath, svc.Idx), request, &response)
	if err != nil {
		return
	}

	// pick over response, strongly coupled to agg template!!

	vals = []TsValue{}
	for _, bkt1 := range gjson.GetBytes(response["aggregations"], "outer.buckets").Array() {

		nxTs := bkt1.Get("key").Int()
		val := bkt1.Get("inner.value").Float()

		vals = append(vals, TsValue{
			Ts:    time.UnixMilli(nxTs).UTC(),
			Value: val,
		})
	}

	return
}

// unexported

type esResult struct {
	Result string `json:"result"`
}

type aggRequest struct {
	Aggs  json.RawMessage `json:"aggs"`
	Query json.RawMessage `json:"query"`
	Size  int             `json:"size"`
}

func newAggRequest(aggName, qryName string, data map[string]string) (request aggRequest, err error) {

	// check that tmpls loaded via init w/o issue

	if loadErr != nil {
		err = loadErr
		return
	}

	// render

	agg, err := templates.RenderJson(aggName, data)
	if err != nil {
		return
	}

	qry, err := templates.RenderJson(qryName, data)
	if err != nil {
		return
	}

	// and assemble

	request = aggRequest{
		Aggs:  agg,
		Query: qry,
		Size:  0,
	}

	return
}

/*
var aggg = `{
  "aggs": {
    "2": {
      "date_histogram": {
        "field": "ts",
        "calendar_interval": "1m",
        "time_zone": "America/Chicago",
        "min_doc_count": 1
      },
      "aggs": {
        "1": {
          "avg": {
            "field": "vehicle_speed"
          }
        }
      }
    }
  },
  "size": 0,
  "fields": [
    {
      "field": "ts",
      "format": "date_time"
    }
  ],
  "script_fields": {},
  "stored_fields": [
    "*"
  ],
  "runtime_mappings": {},
  "_source": {
    "excludes": []
  },
  "query": {
    "bool": {
      "must": [],
      "filter": [
        {
          "range": {
            "ts": {
              "format": "strict_date_optional_time",
              "gte": "2019-06-24T08:06:36.888Z",
              "lte": "2019-06-24T18:12:00.526Z"
            }
          }
        }
      ],
      "should": [],
      "must_not": []
    }
  }
}`
*/

// logs from data dump into es:
/*





{"@timestamp":"2023-11-12T16:41:32.563Z", "log.level": "INFO", "message":"[ztbus001] creating index, cause [auto(bulk api)], templates [], shards [1]/[1]", "ecs.version": "1.2.0","service.name":"ES_ECS","event.dataset":"elasticsearch.server","process.thread.name":"elasticsearch[b1c736e00684][masterService#updateTask][T#3]","log.logger":"org.elasticsearch.cluster.metadata.MetadataCreateIndexService","elasticsearch.cluster.uuid":"7wgOc-FLRYWZRi7cgNZy3g","elasticsearch.node.id":"TY_TpbPcT660jTRd5fwppg","elasticsearch.node.name":"b1c736e00684","elasticsearch.cluster.name":"docker-cluster"}
{"@timestamp":"2023-11-12T16:41:32.618Z", "log.level": "INFO", "message":"reloading search analyzers", "ecs.version": "1.2.0","service.name":"ES_ECS","event.dataset":"elasticsearch.server","process.thread.name":"elasticsearch[b1c736e00684][generic][T#3]","log.logger":"org.elasticsearch.index.mapper.MapperService","elasticsearch.cluster.uuid":"7wgOc-FLRYWZRi7cgNZy3g","elasticsearch.node.id":"TY_TpbPcT660jTRd5fwppg","elasticsearch.node.name":"b1c736e00684","elasticsearch.cluster.name":"docker-cluster","tags":[" [ztbus001]"]}
{"@timestamp":"2023-11-12T16:41:32.717Z", "log.level": "INFO", "message":"[ztbus001/lCDV6cSiQwi_Sh6YsLZELg] create_mapping", "ecs.version": "1.2.0","service.name":"ES_ECS","event.dataset":"elasticsearch.server","process.thread.name":"elasticsearch[b1c736e00684][masterService#updateTask][T#3]","log.logger":"org.elasticsearch.cluster.metadata.MetadataMappingService","elasticsearch.cluster.uuid":"7wgOc-FLRYWZRi7cgNZy3g","elasticsearch.node.id":"TY_TpbPcT660jTRd5fwppg","elasticsearch.node.name":"b1c736e00684","elasticsearch.cluster.name":"docker-cluster"}
*/
