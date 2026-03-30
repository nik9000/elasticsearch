#!/bin/bash
# Quick-and-dirty perf test for null-skipping aggregations.
#
# Indexes 100M documents: every doc has @timestamp, but only 1 in SPARSITY
# docs has `label` (a keyword).  Then runs a few ES|QL queries so you can
# eyeball latency before/after the null-skip change.
#
# Usage:
#   ./perf-null-aggs.sh                  # uses http://localhost:9200
#   ES=http://localhost:9201 ./perf-null-aggs.sh
#   TOTAL=10000000 ./perf-null-aggs.sh   # 10M for a quick smoke-test

set -euo pipefail

ES=${ES:-http://localhost:9200}
INDEX=${INDEX:-perf_null_aggs}
TOTAL=${TOTAL:-100000000}
BATCH=${BATCH:-50000}
SPARSITY=${SPARSITY:-1000}   # 1 in SPARSITY docs has `label` → ~0.1%

echo "ES:       $ES"
echo "Index:    $INDEX"
echo "Total:    $TOTAL docs"
echo "Sparsity: 1 in $SPARSITY has 'label' ($(python3 -c "print(f'{100/$SPARSITY:.2f}')") %)"
echo ""

# ── create index ────────────────────────────────────────────────────────────

echo "==> Deleting old index (ignore 404)…"
curl -s -o /dev/null -w "%{http_code}\n" -X DELETE "$ES/$INDEX"

echo "==> Creating index…"
curl -sf -X PUT "$ES/$INDEX" -H 'Content-Type: application/json' -d '{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0,
    "refresh_interval": "-1"
  },
  "mappings": {
    "properties": {
      "@timestamp": { "type": "date" },
      "label":      { "type": "keyword" }
    }
  }
}' | python3 -m json.tool
echo ""

# ── index data ───────────────────────────────────────────────────────────────

echo "==> Indexing $TOTAL documents (batch=$BATCH, sparsity=$SPARSITY)…"

python3 - <<PYEOF
import json, sys, time, urllib.request, urllib.error

ES       = "$ES"
INDEX    = "$INDEX"
TOTAL    = $TOTAL
BATCH    = $BATCH
SPARSITY = $SPARSITY

# Base timestamp: 2024-01-01T00:00:00Z in epoch-ms
BASE_TS = 1704067200000
TS_STEP = 1000  # 1 second per doc

def post_bulk(body: bytes):
    req = urllib.request.Request(
        f"{ES}/{INDEX}/_bulk",
        data=body,
        headers={"Content-Type": "application/x-ndjson"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        result = json.loads(resp.read())
        if result.get("errors"):
            # print first error and bail
            for item in result["items"]:
                for _op, v in item.items():
                    if "error" in v:
                        print(f"Bulk error: {v['error']}", file=sys.stderr)
                        sys.exit(1)

start = time.time()
batches = (TOTAL + BATCH - 1) // BATCH

for b in range(batches):
    base = b * BATCH
    lines = []
    for i in range(base, min(base + BATCH, TOTAL)):
        lines.append(b'{"index":{}}')
        ts = BASE_TS + i * TS_STEP
        if i % SPARSITY == 0:
            doc = f'{{"@timestamp":{ts},"label":"v{i % 97}"}}'
        else:
            doc = f'{{"@timestamp":{ts}}}'
        lines.append(doc.encode())
    post_bulk(b"\n".join(lines) + b"\n")

    if b % 20 == 0 or b == batches - 1:
        done  = min((b + 1) * BATCH, TOTAL)
        pct   = 100 * done / TOTAL
        elapsed = time.time() - start
        rate  = done / elapsed if elapsed else 0
        eta   = (TOTAL - done) / rate if rate else 0
        print(f"  {done:>12,} / {TOTAL:,}  ({pct:5.1f}%)  "
              f"{rate:,.0f} docs/s  ETA {eta:.0f}s", flush=True)

elapsed = time.time() - start
print(f"\nDone. {TOTAL:,} docs in {elapsed:.1f}s  ({TOTAL/elapsed:,.0f} docs/s)")
PYEOF

# ── refresh ───────────────────────────────────────────────────────────────────

echo ""
echo "==> Refreshing…"
curl -sf -X POST "$ES/$INDEX/_refresh" | python3 -m json.tool

echo ""
echo "==> Index stats:"
curl -sf "$ES/$INDEX/_stats/docs,store" \
  | python3 -c "
import json, sys
s = json.load(sys.stdin)['indices']['$INDEX']['primaries']
print(f\"  docs:  {s['docs']['count']:,}\")
print(f\"  store: {s['store']['size_in_bytes'] / 1024**3:.2f} GB\")
"

# ── example queries ───────────────────────────────────────────────────────────

cat <<'QUERIES'

==> Example ES|QL queries to run in Kibana Dev Tools or via curl:

  # ungrouped – sparse field (should be fast with null-skip)
  POST /_query { "query": "FROM perf_null_aggs | STATS max(label)" }

  # ungrouped – dense field (always present, vector path)
  POST /_query { "query": "FROM perf_null_aggs | STATS max(@timestamp)" }

  # grouped by sparse field
  POST /_query { "query": "FROM perf_null_aggs | STATS count(*) BY label" }

  # grouped by dense field
  POST /_query { "query": "FROM perf_null_aggs | STATS max(label) BY @timestamp" }

Via curl (add -w "\nTime: %{time_total}s\n" to see wall time):
  curl -s -w "\nTime: %{time_total}s\n" -X POST http://localhost:9200/_query \
    -H 'Content-Type: application/json' \
    -d '{"query":"FROM perf_null_aggs | STATS max(label)"}'
QUERIES
