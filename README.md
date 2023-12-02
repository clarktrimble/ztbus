
# ZTBus

Demonstrating an approach to working with ES aggregations in Golang.

## Blogses

This project is a companion to a [post](https://clarktrimble.online/blog/ztbus/) about Golang ES aggregations.
Head on over for blather! :)

## Generalizable Highlights

I like templating queries and I'll have an eye on the `template` package for mini-modularization.

To a lesser extent `elastic` could be a candidate as well if I found myself doing more with ES.

`cmd/load-jsonl` loads json lines (aka ndjson) files into Elasticsearch which is especially nice for logs in the home laboratory.
Now in bulk!

`cmd/dump-query` is a good companion for query/results quackery!
And could be adapted for other backends without much fuss.

`svc` as a dedicated-to-this-project service-layer is nice as a place to plug stuff in..

## Tactical Code Re-Use

- [giant](https://github.com/clarktrimble/giant) http client and tripperwares
- [launch](https://github.com/clarktrimble/launch) envconfig, etc. helpers targeting main.go
- [sabot](https://github.com/clarktrimble/sabot) structured, contextual logging

are featured prominently here and quickly approaching a v1.0.0 release!

