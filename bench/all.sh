#!/bin/bash

set -e
set -o pipefail

export random_seed=89

wrk() {
  printf '%30s ' "$(echo -n $@)"
  ~/Workspaces/wrk/wrk -t 24 -c 100 -d 5s -s bench.lua http://localhost:6200/enwiki/_search -- $@ $random_seed |
    grep 'Requests/sec\|Avg Hits\|Stopped Early\|last_request\|last_response' |
    tr '\n' ' '
  echo
}

wrk and 2
wrk or 2
wrk and 2 1000
wrk or 2 1000
wrk and 2 10000
wrk or 2 10000
wrk and 2 100000
wrk or 2 100000

wrk and~1 2
wrk and~2 2
wrk and~AUTO 2

wrk and*1 2
wrk and*1 2 10000
wrk and*4 2 10000
wrk and*100 2 10000 # this is pretty much a prefix in syntax only - never really does prefixing

wrk phrase 2
wrk phrase~1 2
wrk phrase~10  2
wrk common_2_phrase 2
wrk common_5_phrase 2
wrk common_10_phrase 2
wrk common_20_phrase 2
wrk common_30_phrase 2
wrk common_40_phrase 2
wrk common_50_phrase 2
wrk common_60_phrase 2
wrk common_70_phrase 2
wrk common_80_phrase 2
wrk common_90_phrase 2
wrk common_100_phrase 2


wrk degenerate_phrase 2
wrk degenerate_phrase 10
wrk degenerate_phrase 30
