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
wrk match and 2
wrk match or 2
wrk match~AUTO and 2
wrk match_phrase and 2
wrk match_phrase~1 and 2
wrk match_phrase~10 and 2
