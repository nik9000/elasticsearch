#!/bin/bash

set -e
set -o pipefail

export random_seed=85

wrk() {
  printf '%30s ' "$(echo -n $@)"
  ~/Workspaces/wrk/wrk -t 24 -c 100 -d 5s -s bench.lua http://localhost:6200/enwiki/_search -- $@ $random_seed |
    grep 'Requests/sec\|Avg Hits\|last_request\|last_response' |
    tr '\n' ' '
  echo
}
wrk and 2
wrk or 2
wrk and~AUTO 2
wrk phrase 2
wrk phrase~1 2
wrk phrase~10  2
wrk and* 2

wrk degenerate_phrase 2
wrk degenerate_phrase 10
wrk degenerate_phrase 30
