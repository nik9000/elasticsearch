# Plan: Use `PagedBytesRefBuilder` in `GroupKeyEncoder`

## Background

`GroupKeyEncoder` uses a `BreakingBytesRefBuilder scratch` to assemble a composite group key
from multiple block columns. The scratch grows to accommodate the largest key seen and retains
that allocation permanently (only `length` is reset to 0 on `clear()`).

For queries with large `BYTES_REF` group-key columns (e.g. 30 columns × 1 MB values), this
produces a ~30 MB permanently-live contiguous allocation on the JVM heap. Repeated
resize-and-discard cycles leave gaps that the GC cannot compact, leading to OOM errors before
the circuit breaker fires.

`PagedBytesRefBuilder` stores bytes in recycled 16 KB pages. After `clear()` all pages except
the first are returned to the `PageCacheRecycler`. The maximum permanent per-encoder allocation
is 16 KB regardless of peak key size.

---

## The core challenge

`GroupKeyEncoder.encode()` currently returns `scratch.bytesRefView()` — a zero-copy view into
the contiguous backing array — which callers pass directly to `BytesRefHashTable.add(BytesRef)`.

`BytesRefHashTable` (server module) requires a contiguous `BytesRef`. A naive port that builds
the key in `PagedBytesRefBuilder` and then flattens to `BytesRef` for the hash call would
re-create the same large contiguous allocation that causes fragmentation.

The correct fix: teach `BytesRefHashTable` to hash, compare, and store keys provided as
scattered byte pages, with no contiguous scratch required on the caller side.

---

## Step 1 — Add a multi-segment `add()` to `BytesRefHashTable`

Add to the `BytesRefHashTable` interface (server module):

```java
/**
 * Adds a key encoded as scattered pages. {@code pages[0..usedPages-1]} each hold
 * {@code pageSize} bytes (the last page may hold fewer); {@code totalLength} is the
 * total key length across all pages.
 *
 * Returns the same ordinal semantics as {@link #add(BytesRef)}.
 */
long add(byte[][] pages, int usedPages, int pageSize, int totalLength);
```

This uses only plain Java types — no dependency on compute-module classes.

Implement in `BytesRefHash`:
- **Hash**: iterate over pages in order, feeding each byte to the same hash function used by
  `add(BytesRef)`.
- **Compare**: compare byte-by-byte against the stored contiguous key in `BytesRefArray`, no
  extra buffer needed.
- **Store**: copy each page slice into `BytesRefArray` using the existing append logic.

No large contiguous scratch is allocated at any point.

---

## Step 2 — Add `addToHashTable(BytesRefHashTable)` to `PagedBytesRefBuilder`

`PagedBytesRefBuilder` runs in two modes:

- **Small-tail** (≤ 8 KB): data is in a single heap `byte[]`. Wrap it in a `BytesRef` and
  call `table.add(bytesRef)`.
- **Paged** (> 8 KB): call `table.add(pages, usedPages, BYTE_PAGE_SIZE, length())`.

```java
public long addToHashTable(BytesRefHashTable table) {
    if (pages == null) {
        // small-tail mode
        return table.add(new BytesRef(tail, 0, tailOffset));
    }
    return table.add(rawPages(), usedPages, BYTE_PAGE_SIZE, length());
}
```

`rawPages()` is a private helper that extracts `byte[]` from `Recycler.V<byte[]>[]`.

`BytesRefHashTable` and `PagedBytesRefBuilder` are both now in the server module (`org.elasticsearch.common.util`) — no cross-module concerns at all.

---

## Step 3 — Change `GroupKeyEncoder`

Replace `BreakingBytesRefBuilder scratch` with `PagedBytesRefBuilder scratch`. The constructor
changes from:

```java
GroupKeyEncoder(int[] groupChannels, List<ElementType> elementTypes, BreakingBytesRefBuilder scratch)
```

to:

```java
GroupKeyEncoder(int[] groupChannels, List<ElementType> elementTypes, CircuitBreaker breaker, PageCacheRecycler recycler)
```

`GroupKeyEncoder` creates and owns the builder internally. Callers no longer allocate the
scratch themselves.

Replace `encode(Page, int)` (returns `BytesRef`) with:

```java
/**
 * Encodes the group key at {@code position} from {@code page} and adds it to {@code table}.
 * Returns the same ordinal as {@link BytesRefHashTable#add(BytesRef)}.
 */
public long encodeAndAdd(Page page, int position, BytesRefHashTable table) {
    scratch.clear();
    for (int i = 0; i < groupChannels.length; i++) {
        encodeBlock(page.getBlock(groupChannels[i]), elementTypes[i], position);
    }
    return scratch.addToHashTable(table);
}
```

`encodeBlock` is unchanged except it now calls `scratch.appendVInt(...)` /
`scratch.encodeInt(...)` etc. via the `PagedBytesRefBuilder` overloads already implemented in
`TopNEncoder`.

`encode()` can be kept (delegates to a temporary `BreakingBytesRefBuilder`) or removed once
callers are updated.

---

## Step 4 — Update callers

`GroupedTopNOperator.Factory.get()`:

```java
// before
var scratch = new BreakingBytesRefBuilder(driverContext.breaker(), "group-key-encoder");
var keyEncoder = new GroupKeyEncoder(groupKeysArray, elementTypes, scratch);

// after
var keyEncoder = new GroupKeyEncoder(groupKeysArray, elementTypes,
    driverContext.breaker(), driverContext.bigArrays().recycler());
```

`GroupedTopNOperator.addInput()`:

```java
// before
BytesRef key = keyEncoder.encode(page, pos);
long hashOrd = keysHash.add(key);

// after
long hashOrd = keyEncoder.encodeAndAdd(page, pos, keysHash);
```

Same changes in `GroupedLimitOperator.Factory.get()` and its `addInput` equivalent.

---

## Files to change

| File | Change |
|---|---|
| `BytesRefHashTable` (server) | Add `long add(byte[][], int usedPages, int pageSize, int totalLength)` |
| `BytesRefHash` (server) | Implement multi-segment `add()` |
| `PagedBytesRefBuilder` (compute) | Add `long addToHashTable(BytesRefHashTable)` |
| `GroupKeyEncoder` (compute) | `BreakingBytesRefBuilder → PagedBytesRefBuilder`; `encode()` → `encodeAndAdd()` |
| `GroupedTopNOperator` (compute) | Drop scratch construction; use `encodeAndAdd` |
| `GroupedLimitOperator` (compute) | Same |
| `GroupKeyEncoderTests` | Update constructor; test `encodeAndAdd` |
