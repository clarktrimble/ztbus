
# ZTBus

Demonstrating an approach to working with ES aggregations in Golang.

## Blogses

This project is a companion to [ZTBus](https://clarktrimble.online/blog/ztbus/).  Head on over for blather! :)

## Generalizable Highlights

I like templating queries generally and I'll have an eye on the `template` package for mini-modularization.

To a lesser extent `elastic` could be a candidate as well if I found myself doing more with ES.

`cmd/load-jsonl` loads json lines (aka ndjson) files into elastic which is especially nice for logs in the home laboratory.  Needs to be upgraded to at least the ES Bulk API though.

`cmd/dump-query` is a good companion for query/results quackery!

`svc` as a dedicated-to-this project service-layer is quite nice.

... must run for coffee, standby! 


