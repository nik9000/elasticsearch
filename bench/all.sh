#!/bin/bash

set -e
set -o pipefail

export random_seed=110

wrk() {
  ~/Workspaces/wrk/wrk -t 24 -c 100 -d30s -s bench.lua http://localhost:6200/enwiki/_search -- $@ $random_seed |
    grep 'Args\|Requests/sec\|Avg Hits\|Stopped Early\|last_request\|last_response' |
    tr '\n' ' '
  echo
}

for type in 'and' 'or'; do
  for words in $(seq 1 10); do
    wrk $type $words
  done
  for terminate_after in 1000000000 10000; do
    for common in 20 40 60 80 100; do
      wrk $type 2 $common $terminate_after
    done
  done
  for terminate_after in 1000 5000 10000 20000; do
    wrk $type 2 0 $terminate_after
  done
done

for terminate_after in 1000000000 10000; do
  for common in 0 20 40 60 80 100; do
    wrk and~1 2 $common $terminate_after
    wrk and~2 2 $common $terminate_after
    wrk and~AUTO 2 $common $terminate_after

    for prefix_length in 1 2 3 4 5 6 7; do
      wrk and*$prefix_length 2 $common $terminate_after
    done

    for slop in '' ~1 ~2 ~3 ~4 ~10 ~100 ~1000; do
      wrk phrase$slop 2 $common $terminate_after
    done

    wrk degenerate_phrase 2 $common $terminate_after
    wrk phrase_rescore~4_1000_and 2 $common $terminate_after
    wrk phrase_rescore~4_1000_or 2 $common $terminate_after
  done
done
