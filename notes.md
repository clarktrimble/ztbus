
## agg from kibana

```
{
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
}
```

## logs from initial data into es:

```
{"@timestamp":"2023-11-12T16:41:32.563Z", "log.level": "INFO", "message":"[ztbus001] creating index, cause [auto(bulk api)], templates [], shards [1]/[1]", "ecs.version": "1.2.0","service.name":"ES_ECS","event.dataset":"elasticsearch.server","process.thread.name":"elasticsearch[b1c736e00684][masterService#updateTask][T#3]","log.logger":"org.elasticsearch.cluster.metadata.MetadataCreateIndexService","elasticsearch.cluster.uuid":"7wgOc-FLRYWZRi7cgNZy3g","elasticsearch.node.id":"TY_TpbPcT660jTRd5fwppg","elasticsearch.node.name":"b1c736e00684","elasticsearch.cluster.name":"docker-cluster"}
{"@timestamp":"2023-11-12T16:41:32.618Z", "log.level": "INFO", "message":"reloading search analyzers", "ecs.version": "1.2.0","service.name":"ES_ECS","event.dataset":"elasticsearch.server","process.thread.name":"elasticsearch[b1c736e00684][generic][T#3]","log.logger":"org.elasticsearch.index.mapper.MapperService","elasticsearch.cluster.uuid":"7wgOc-FLRYWZRi7cgNZy3g","elasticsearch.node.id":"TY_TpbPcT660jTRd5fwppg","elasticsearch.node.name":"b1c736e00684","elasticsearch.cluster.name":"docker-cluster","tags":[" [ztbus001]"]}
{"@timestamp":"2023-11-12T16:41:32.717Z", "log.level": "INFO", "message":"[ztbus001/lCDV6cSiQwi_Sh6YsLZELg] create_mapping", "ecs.version": "1.2.0","service.name":"ES_ECS","event.dataset":"elasticsearch.server","process.thread.name":"elasticsearch[b1c736e00684][masterService#updateTask][T#3]","log.logger":"org.elasticsearch.cluster.metadata.MetadataMappingService","elasticsearch.cluster.uuid":"7wgOc-FLRYWZRi7cgNZy3g","elasticsearch.node.id":"TY_TpbPcT660jTRd5fwppg","elasticsearch.node.name":"b1c736e00684","elasticsearch.cluster.name":"docker-cluster"}
```

## Todo

// Todo: push logs to ES and show off??
// Todo: update from branchy giant mod
// Todo: if redact is blank, say "unset" in launch
// Todo: config trunc in sabot
// Todo: demo error! from launch

## working data

```
trimble@tartu:~/ztbus/compressed$ ls -l B*2022*09-21_*
-rw-r--r-- 1 trimble trimble 8592100 Aug  9 11:02 B183_2022-09-21_04-21-57_2022-09-21_17-19-17.csv
-rw-r--r-- 1 trimble trimble 9260703 Aug  9 11:07 B208_2022-09-21_04-05-16_2022-09-21_18-20-09.csv
```


