# Plan: Use `PagedBytesRefBuilder` in `TopNRow`

## Background

`TopNRow` currently uses `BreakingBytesRefBuilder` for `keys` and `values`. That class stores
everything in a single growing heap `byte[]`. `PagedBytesRefBuilder` uses recycled 16KB pages
from `PageCacheRecycler`, reducing GC pressure for large rows.

The complication is that `BreakingBytesRefBuilder` exposes three access patterns that
`PagedBytesRefBuilder` doesn't support yet:

1. **`grow` / `bytes()` / `setLength`** — encoders (e.g. `SortableAscTopNEncoder`,
   `DefaultUnsortableTopNEncoder`) pre-grow and write primitives directly via VarHandle into
   `bytes()` at `length()`.
2. **Post-append `bitwiseNot`** — `FixedLengthDescTopNEncoder` and `VersionDescTopNEncoder`
   flip bits in-place on bytes just written, via `bitwiseNot(bytes(), startOffset, length())`.
3. **`bytesRefView()`** — a live zero-copy view into the contiguous backing array, used for
   compare/equals/hash in `TopNRow`, for `minCompetitive`, and for `readKeys`/`readValues`
   decoding.

---

## Step 1 — Add `clear()` to `PagedBytesRefBuilder`

The spare `TopNRow` is reused across rows: `spare.clear()` resets it before each new row.
`PagedBytesRefBuilder` needs this:

- Release all recycler pages except keep the first (or revert to small-tail) to avoid repeated
  page allocation churn.
- Adjust breaker accordingly.
- Set `tailOffset = 0`.

---

## Step 2 — Thread `PageCacheRecycler` into `TopNRow`

`PagedBytesRefBuilder` requires a recycler. `TopNRow` gets a new constructor parameter:

```java
TopNRow(CircuitBreaker breaker, PageCacheRecycler recycler, int preAllocKeys, int preAllocValues)
```

`TopNOperator` and `GroupedTopNOperator` pass the recycler (from `DriverContext` or `BigArrays`)
through to `TopNRow` construction.

---

## Step 3 — Replace `BreakingBytesRefBuilder` with `PagedBytesRefBuilder` in `TopNRow`

Straightforward field swap. Most of `TopNRow` works unchanged because `PagedBytesRefBuilder`
already implements `compareTo`, `equals`, `hashCode`, `close`, and `ramBytesUsed`. The exception
is `bytesRefView()` — see Step 5.

---

## Step 4 — Fix encoder write patterns

### Pattern: `grow` / `bytes()` / `setLength`

Replace with `append(int v)` / `append(long v)`. Float and double always go through their raw
bit representations first — `Float.floatToRawIntBits` / `Double.doubleToRawLongBits` — giving
an `int` or `long` that is then passed to `append`. Affected: `DefaultUnsortableTopNEncoder`
(long, int, float, double), `SortableAscTopNEncoder` (long, int).

### Pattern: post-append `bitwiseNot`

Two sub-cases:

**Fixed-size types** (int, long, float, double desc encoders): pre-flip with `~`. Float and double
still go through raw bits first, then `~`:

```java
builder.append(~Float.floatToRawIntBits(value));
builder.append(~Double.doubleToRawLongBits(value));
```

**Non-fixed-size byte sequences** (`FixedLengthDescTopNEncoder`, `VersionDescTopNEncoder`):
use the new `appendNot(byte[], int, int)` which flips bits as it copies:

```java
// FixedLengthDescTopNEncoder
builder.appendNot(value.bytes, value.offset, value.length);
```

No scratch buffer needed in either case.

### Change interfaces

```
KeyExtractor.writeKey(BreakingBytesRefBuilder, int)  →  writeKey(PagedBytesRefBuilder, int)
ValueExtractor.writeValue(BreakingBytesRefBuilder, int)  →  writeValue(PagedBytesRefBuilder, int)
TopNEncoder.encodeBytesRef(BytesRef, BreakingBytesRefBuilder)  →  encodeBytesRef(BytesRef, PagedBytesRefBuilder)
```

Update all implementations. The changes per file are mechanical.

---

## Step 5 — Handle `bytesRefView()` callers

Three distinct call sites:

| Caller | Frequency | Approach |
|---|---|---|
| `TopNRow.compareTo/equals/hashCode` | Every heap operation | Use `PagedBytesRefBuilder.compareTo/equals/hashCode` directly — already implemented |
| `minCompetitive.offer(top.keys.bytesRefView())` | Every new input row (once queue is full) | `minCompetitive` holds a `PagedBytesRefBuilder`; update it with `clear()` + `append(PagedBytesRefBuilder)` from the queue top's `keys`. No copy, no flat array. |
| `readKeys(builders, row.keys.bytesRefView())` / `readValues(...)` | Once per row at result time | Modify `readKeys`/`readValues` and the decoder infrastructure (`TopNEncoder`, `ResultBuilder`, etc.) to read directly from `PagedBytesRef` using a cursor/position instead of a flat `BytesRef`. |

This requires a sequential-read abstraction over `PagedBytesRef` — a cursor that tracks the
current page index and offset within the page. Decoders advance the cursor as they consume
bytes. All `decodeBytesRef`, `decodeInt`, `decodeLong`, etc. methods are updated to use the
cursor instead of mutating a `BytesRef` offset directly. This eliminates the flat copy
entirely.

---

## Files to change

| File | Change |
|---|---|
| ~~`PagedBytesRefBuilder`~~ | ~~Add `clear()`, already has `append(PagedBytesRefBuilder)`~~ ✅ |
| ~~`PagedBytesRef` (new cursor type or inner class)~~ | ~~Sequential-read cursor over pages~~ ✅ (`PagedBytesRefCursor`) |
| `TopNRow` | Swap field types, add recycler param |
| `TopNOperator` | Pass recycler to `TopNRow` |
| `GroupedTopNOperator` | Same |
| `KeyExtractor` | Change interface signature |
| `ValueExtractor` | Change interface signature |
| ~~`TopNEncoder`~~ | ~~Change `encodeBytesRef` signature~~ ✅ (added paged overloads for all methods) |
| All `KeyExtractorFor*` classes | Update implementations |
| All `ValueExtractorFor*` classes | Update implementations |
| ~~All `*TopNEncoder` classes~~ | ~~Update `encodeBytesRef`, update `decodeBytesRef` etc. to use cursor~~ ✅ |
| `TopNRowTests` | Update for new constructor |
| ~~`PagedBytesRefBuilderTests`~~ | ~~Add `clear()` tests~~ ✅ |
